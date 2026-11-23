"""
All metadata lives in ONE SQLite file (see DECISIONS.md D-06):

    workers          who is in the cluster and whether they are alive
    objects          one row per stored object (name, size, checksum, custom metadata ...)
    chunks           one row per 1 MiB piece of an object (order, size, checksum)
    chunk_replicas   which worker holds a copy of which chunk
    events           append-only operational log shown by the demo

Keeping every SQL statement in this one module means the rest of the code never
builds SQL strings - easier to audit and to change later.

Threading note: the controller's handlers are `async` and run on a single event
loop thread, so SQLite calls never run in parallel. We still guard the
connection with a lock so this stays safe if someone later adds threads.
SQLite calls here take microseconds-to-milliseconds, which is why we accept
calling them directly from async code instead of a thread pool (D-06).
"""
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Optional


def utc_iso(ts: Optional[float] = None) -> str:
    """
    Timestamp as ISO-8601 UTC with fixed millisecond width, e.g.
    2026-11-13T10:15:30.123Z. Fixed width means plain string comparison sorts
    chronologically, which we rely on for 'older than X' queries.
    """
    dt = datetime.fromtimestamp(time.time() if ts is None else ts, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id            TEXT PRIMARY KEY,
    url                  TEXT NOT NULL,
    status               TEXT NOT NULL,          -- 'alive' | 'dead'
    registered_at        REAL NOT NULL,
    last_heartbeat       REAL NOT NULL,          -- epoch seconds
    dead_since           REAL,                   -- epoch seconds, NULL while alive
    reported_chunks      INTEGER NOT NULL DEFAULT 0,   -- from the worker's heartbeat
    reported_used_bytes  INTEGER NOT NULL DEFAULT 0,
    free_bytes           INTEGER
);

CREATE TABLE IF NOT EXISTS objects (
    object_id        TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    size             INTEGER NOT NULL DEFAULT 0,
    content_type     TEXT NOT NULL,
    checksum         TEXT,                        -- SHA-256 of the WHOLE object
    num_chunks       INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    modified_at      TEXT NOT NULL,
    custom_metadata  TEXT NOT NULL DEFAULT '{}',  -- JSON object (user key/values)
    -- 'uploading' objects are invisible to clients; only 'ready' ones are served.
    -- This is what makes uploads all-or-nothing (D-08).
    status           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id   TEXT PRIMARY KEY,
    object_id  TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    idx        INTEGER NOT NULL,                  -- position inside the object (0-based)
    size       INTEGER NOT NULL,
    checksum   TEXT NOT NULL,                     -- SHA-256 of this chunk (authoritative)
    UNIQUE (object_id, idx)
);

CREATE TABLE IF NOT EXISTS chunk_replicas (
    chunk_id    TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL,
    created_at  REAL NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)             -- a worker holds at most one copy
);
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON chunk_replicas(worker_id);

CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       REAL NOT NULL,
    kind     TEXT NOT NULL,
    message  TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        # isolation_level=None -> autocommit; we open explicit transactions with
        # BEGIN/COMMIT in transaction() when several statements must be atomic.
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row  # rows behave like dicts
        self._lock = threading.RLock()
        # WAL: readers don't block the writer and a crash can't corrupt the file.
        self._conn.execute("PRAGMA journal_mode=WAL")
        # Needed for ON DELETE CASCADE to actually work (off by default in SQLite!).
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    # ----------------------------------------------------------- low level
    def _all(self, sql: str, params: Iterable = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchall()

    def _one(self, sql: str, params: Iterable = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchone()

    def _run(self, sql: str, params: Iterable = ()) -> int:
        """Execute a write; returns the number of affected rows."""
        with self._lock:
            return self._conn.execute(sql, tuple(params)).rowcount

    @contextmanager
    def transaction(self):
        """All-or-nothing group of statements."""
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- workers
    def upsert_worker(self, worker_id: str, url: str, now: float) -> Optional[str]:
        """
        Register (or re-register) a worker as ALIVE.
        Returns its PREVIOUS status ('alive'/'dead'), or None if it is new.
        The caller uses that to decide whether the worker needs a reconcile.
        """
        with self._lock:
            row = self._one("SELECT status FROM workers WHERE worker_id=?", (worker_id,))
            if row is None:
                self._run(
                    "INSERT INTO workers (worker_id,url,status,registered_at,last_heartbeat)"
                    " VALUES (?,?,'alive',?,?)",
                    (worker_id, url, now, now),
                )
                return None
            self._run(
                "UPDATE workers SET url=?, status='alive', last_heartbeat=?, dead_since=NULL"
                " WHERE worker_id=?",
                (url, now, worker_id),
            )
            return row["status"]

    def touch_heartbeat(self, worker_id: str, now: float, stats: dict) -> Optional[str]:
        """
        Record a heartbeat. Returns the PREVIOUS status, or None if the worker
        is unknown (the API then answers 404 so the worker re-registers).
        """
        with self._lock:
            row = self._one("SELECT status FROM workers WHERE worker_id=?", (worker_id,))
            if row is None:
                return None
            self._run(
                "UPDATE workers SET status='alive', last_heartbeat=?, dead_since=NULL,"
                " reported_chunks=?, reported_used_bytes=?, free_bytes=? WHERE worker_id=?",
                (
                    now,
                    int(stats.get("chunk_count", 0)),
                    int(stats.get("used_bytes", 0)),
                    stats.get("free_bytes"),
                    worker_id,
                ),
            )
            return row["status"]

    def mark_worker_dead(self, worker_id: str, now: float) -> bool:
        """alive -> dead. Returns True only if the status actually changed."""
        return self._run(
            "UPDATE workers SET status='dead', dead_since=? WHERE worker_id=? AND status='alive'",
            (now, worker_id),
        ) > 0

    def get_worker(self, worker_id: str) -> Optional[sqlite3.Row]:
        return self._one("SELECT * FROM workers WHERE worker_id=?", (worker_id,))

    def list_workers(self, status: Optional[str] = None) -> list[sqlite3.Row]:
        if status:
            return self._all("SELECT * FROM workers WHERE status=? ORDER BY worker_id", (status,))
        return self._all("SELECT * FROM workers ORDER BY worker_id")

    # ------------------------------------------------------------- objects
    def create_object(self, object_id, name, content_type, custom_metadata: dict, now_iso) -> None:
        """Insert the object in 'uploading' state (invisible until finalize_object)."""
        self._run(
            "INSERT INTO objects (object_id,name,content_type,created_at,modified_at,"
            "custom_metadata,status) VALUES (?,?,?,?,?,?,'uploading')",
            (object_id, name, content_type, now_iso, now_iso, json.dumps(custom_metadata)),
        )

    def finalize_object(self, object_id: str, size: int, checksum: str, num_chunks: int) -> None:
        """Flip an upload to 'ready' - the moment it becomes visible to clients."""
        self._run(
            "UPDATE objects SET size=?, checksum=?, num_chunks=?, status='ready' WHERE object_id=?",
            (size, checksum, num_chunks, object_id),
        )

    def get_object(self, object_id: str) -> Optional[sqlite3.Row]:
        """Only 'ready' objects are returned: half-uploaded ones do not exist for clients."""
        return self._one("SELECT * FROM objects WHERE object_id=? AND status='ready'", (object_id,))

    def list_objects(self, limit: int, offset: int) -> list[sqlite3.Row]:
        # Newest first, with object_id as a tie-breaker so paging is stable.
        return self._all(
            "SELECT * FROM objects WHERE status='ready'"
            " ORDER BY created_at DESC, object_id LIMIT ? OFFSET ?",
            (limit, offset),
        )

    def count_objects(self) -> int:
        return self._one("SELECT COUNT(*) AS n FROM objects WHERE status='ready'")["n"]

    def update_custom_metadata(self, object_id: str, metadata: dict, now_iso: str) -> None:
        self._run(
            "UPDATE objects SET custom_metadata=?, modified_at=? WHERE object_id=?",
            (json.dumps(metadata), now_iso, object_id),
        )

    def delete_object(self, object_id: str) -> list[tuple[str, str]]:
        """
        Remove an object, its chunks and its replica records in one transaction
        (cascading foreign keys do the chunk/replica part).

        Returns [(chunk_id, worker_id), ...] describing the physical copies the
        caller must now delete from the workers.
        """
        with self.transaction():
            copies = self._all(
                "SELECT r.chunk_id, r.worker_id FROM chunk_replicas r"
                " JOIN chunks c ON c.chunk_id=r.chunk_id WHERE c.object_id=?",
                (object_id,),
            )
            self._run("DELETE FROM objects WHERE object_id=?", (object_id,))
        return [(r["chunk_id"], r["worker_id"]) for r in copies]

    def stale_upload_ids(self, older_than_iso: str) -> list[str]:
        rows = self._all(
            "SELECT object_id FROM objects WHERE status='uploading' AND created_at < ?",
            (older_than_iso,),
        )
        return [r["object_id"] for r in rows]

    def object_exists_any_state(self, object_id: str) -> bool:
        return self._one("SELECT 1 FROM objects WHERE object_id=?", (object_id,)) is not None

    # -------------------------------------------------------------- chunks
    def add_chunk(self, chunk_id, object_id, idx, size, checksum) -> None:
        self._run(
            "INSERT INTO chunks (chunk_id,object_id,idx,size,checksum) VALUES (?,?,?,?,?)",
            (chunk_id, object_id, idx, size, checksum),
        )

    def get_chunks(self, object_id: str) -> list[sqlite3.Row]:
        """Chunks of an object IN ORDER - ordering is what lets us reassemble it."""
        return self._all("SELECT * FROM chunks WHERE object_id=? ORDER BY idx", (object_id,))

    def get_chunk(self, chunk_id: str) -> Optional[sqlite3.Row]:
        return self._one("SELECT * FROM chunks WHERE chunk_id=?", (chunk_id,))

    # ------------------------------------------------------------ replicas
    def add_replica(self, chunk_id: str, worker_id: str, now: float) -> None:
        self._run(
            "INSERT OR IGNORE INTO chunk_replicas (chunk_id,worker_id,created_at) VALUES (?,?,?)",
            (chunk_id, worker_id, now),
        )

    def remove_replica(self, chunk_id: str, worker_id: str) -> bool:
        return self._run(
            "DELETE FROM chunk_replicas WHERE chunk_id=? AND worker_id=?", (chunk_id, worker_id)
        ) > 0

    def has_replica(self, chunk_id: str, worker_id: str) -> bool:
        return self._one(
            "SELECT 1 FROM chunk_replicas WHERE chunk_id=? AND worker_id=?", (chunk_id, worker_id)
        ) is not None

    def replica_workers(self, chunk_id: str) -> list[str]:
        rows = self._all(
            "SELECT worker_id FROM chunk_replicas WHERE chunk_id=? ORDER BY created_at", (chunk_id,)
        )
        return [r["worker_id"] for r in rows]

    def replicas_for_object(self, object_id: str) -> dict[str, list[str]]:
        """{chunk_id: [worker_id, ...]} for every chunk of the object (one query)."""
        rows = self._all(
            "SELECT r.chunk_id, r.worker_id FROM chunk_replicas r"
            " JOIN chunks c ON c.chunk_id=r.chunk_id WHERE c.object_id=?"
            " ORDER BY r.created_at",
            (object_id,),
        )
        out: dict[str, list[str]] = {}
        for r in rows:
            out.setdefault(r["chunk_id"], []).append(r["worker_id"])
        return out

    def chunks_on_worker(self, worker_id: str) -> list[sqlite3.Row]:
        """The controller's RECORD of what a worker should hold."""
        return self._all(
            "SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum FROM chunk_replicas r"
            " JOIN chunks c ON c.chunk_id=r.chunk_id WHERE r.worker_id=? ORDER BY c.chunk_id",
            (worker_id,),
        )

    def worker_load(self) -> dict[str, dict]:
        """{worker_id: {'chunks': n, 'bytes': b}} as recorded in the database."""
        rows = self._all(
            "SELECT r.worker_id, COUNT(*) AS n, COALESCE(SUM(c.size),0) AS b"
            " FROM chunk_replicas r JOIN chunks c ON c.chunk_id=r.chunk_id GROUP BY r.worker_id"
        )
        return {r["worker_id"]: {"chunks": r["n"], "bytes": r["b"]} for r in rows}

    def all_chunks_with_holders(self) -> dict[str, dict]:
        """
        Every chunk of every READY object with the list of workers holding it:
            {chunk_id: {'chunk_id','object_id','idx','size','checksum','holders':[...]}}
        Used by the repair loop and the replication report.
        """
        rows = self._all(
            "SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum, r.worker_id"
            " FROM chunks c JOIN objects o ON o.object_id=c.object_id AND o.status='ready'"
            " LEFT JOIN chunk_replicas r ON r.chunk_id=c.chunk_id"
            " ORDER BY c.object_id, c.idx"
        )
        out: dict[str, dict] = {}
        for r in rows:
            entry = out.setdefault(
                r["chunk_id"],
                {
                    "chunk_id": r["chunk_id"],
                    "object_id": r["object_id"],
                    "idx": r["idx"],
                    "size": r["size"],
                    "checksum": r["checksum"],
                    "holders": [],
                },
            )
            if r["worker_id"]:
                entry["holders"].append(r["worker_id"])
        return out

    # -------------------------------------------------------------- events
    def add_event(self, kind: str, message: str) -> None:
        self._run("INSERT INTO events (ts,kind,message) VALUES (?,?,?)", (time.time(), kind, message))
        # Keep the table small: retain only the newest 2000 events.
        self._run("DELETE FROM events WHERE id <= (SELECT MAX(id) FROM events) - 2000")

    def recent_events(self, limit: int) -> list[dict]:
        rows = self._all("SELECT * FROM (SELECT * FROM events ORDER BY id DESC LIMIT ?) ORDER BY id", (limit,))
        return [
            {"id": r["id"], "time": utc_iso(r["ts"]), "kind": r["kind"], "message": r["message"]}
            for r in rows
        ]
