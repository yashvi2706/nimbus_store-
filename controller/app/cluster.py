"""
The controller's brain for DATA operations.

`Cluster` ties together the database (what we know) and the worker client (how
we talk to workers) and implements:

    * placement         - which workers should hold a new chunk
    * store_chunk       - write a chunk to REPLICATION_FACTOR workers
    * fetch_chunk       - read a chunk, verifying its checksum, failing over to
                          another copy if a copy is bad or its worker is down
    * drop_bad_replica  - forget a copy that is corrupt/missing (this is what
                          triggers self-healing)
    * replication_report- how healthy is replication right now?

Background jobs (failure detection, repair, scrubbing) live in maintenance.py
and use the same Cluster object.
"""
import asyncio
import hashlib
import logging
import random
import time
from typing import AsyncIterator, Iterable

from . import config
from .db import Database
from .errors import ApiError
from .worker_client import (
    ChunkCorrupt,
    ChunkMissing,
    WorkerClient,
    WorkerError,
    WorkerUnreachable,
)

log = logging.getLogger("controller.cluster")


class ChunkUnavailable(Exception):
    """No valid copy of a chunk could be read from any live worker."""


class Cluster:
    def __init__(self, db: Database, client: WorkerClient):
        self.db = db
        self.client = client
        # Set whenever something happens that might need repair; wakes the
        # maintenance loop immediately instead of waiting for its timer.
        self.repair_event = asyncio.Event()
        # Only ONE maintenance activity (repair / scrub / reconcile) at a time,
        # so they never fight over the same chunks (see D-13).
        self.maintenance_lock = asyncio.Lock()
        # Workers that just (re)joined and must be compared against our records
        # BEFORE their chunks are trusted for trimming/repair decisions.
        self.pending_reconcile: set[str] = set()

    # ------------------------------------------------------------- helpers
    def log_event(self, kind: str, message: str) -> None:
        """Write to both the process log and the persistent events table."""
        log.info("%s: %s", kind, message)
        self.db.add_event(kind, message)

    def alive_workers(self) -> list:
        return self.db.list_workers(status="alive")

    async def mark_dead(self, worker_id: str, reason: str) -> None:
        """Declare a worker dead (idempotent) and wake the repair loop."""
        if self.db.mark_worker_dead(worker_id, time.time()):
            self.log_event("worker_down", f"{worker_id} marked DEAD ({reason})")
            self.repair_event.set()

    def worker_seen(self, worker_id: str, previous_status) -> None:
        """
        Called when a worker registers or heartbeats. If it was NOT alive before
        (brand new, or coming back from the dead) we queue a reconcile: before we
        count its disk contents we verify them against our records.
        """
        if previous_status != "alive":
            kind = "worker_registered" if previous_status is None else "worker_up"
            self.log_event(kind, f"{worker_id} is ALIVE (was: {previous_status or 'unknown'})")
            self.pending_reconcile.add(worker_id)
            self.repair_event.set()

    # ----------------------------------------------------------- placement
    def choose_workers(self, n: int, exclude: Iterable[str] = ()) -> list:
        """
        Pick up to `n` ALIVE workers, skipping `exclude`.

        Strategy: least-loaded first (fewest chunks recorded), random tie-break.
        Because every new chunk goes to the emptiest workers, data spreads evenly
        without any extra bookkeeping (D-07). Never returns the same worker twice,
        which is what guarantees copies land on DIFFERENT workers.
        """
        excluded = set(exclude)
        load = self.db.worker_load()
        candidates = [w for w in self.alive_workers() if w["worker_id"] not in excluded]
        random.shuffle(candidates)  # random tie-break, then stable sort by load
        candidates.sort(key=lambda w: load.get(w["worker_id"], {}).get("chunks", 0))
        return candidates[:n]

    # --------------------------------------------------------------- write
    async def _try_put(self, worker, chunk_id: str, data: bytes, sha: str):
        """Attempt one write. Returns None on success, or the exception on failure."""
        try:
            await self.client.put_chunk(worker["url"], chunk_id, data, sha)
            return None
        except WorkerUnreachable as exc:
            if exc.connect_failed:
                await self.mark_dead(worker["worker_id"], "write failed: connection refused")
            return exc
        except WorkerError as exc:
            return exc

    async def store_chunk(self, chunk_id: str, data: bytes, sha: str) -> list[str]:
        """
        Write one chunk to REPLICATION_FACTOR distinct workers and record each
        successful copy in the database. Returns the worker ids used.

        The copies are written IN PARALLEL (latency of one write, not two). If a
        write fails we try ANOTHER worker instead of giving up, so a worker dying
        mid-upload does not fail the upload as long as enough workers remain.
        """
        placed: list[str] = []
        tried: set[str] = set()
        while len(placed) < config.REPLICATION_FACTOR:
            need = config.REPLICATION_FACTOR - len(placed)
            targets = self.choose_workers(need, exclude=tried)
            if not targets:
                raise ApiError(
                    503,
                    "insufficient_workers",
                    f"could only store {len(placed)} of {config.REPLICATION_FACTOR} "
                    "required copies: not enough live storage workers",
                )
            tried.update(w["worker_id"] for w in targets)
            results = await asyncio.gather(
                *(self._try_put(w, chunk_id, data, sha) for w in targets)
            )
            for worker, failure in zip(targets, results):
                if failure is None:
                    self.db.add_replica(chunk_id, worker["worker_id"], time.time())
                    placed.append(worker["worker_id"])
                else:
                    log.warning("write of %s to %s failed: %s", chunk_id, worker["worker_id"], failure)
        return placed

    # ---------------------------------------------------------------- read
    async def fetch_chunk(self, chunk) -> bytes:
        """
        Return the verified bytes of a chunk, trying live replicas in random
        order (random = reads spread across workers).

        A copy is only accepted if its SHA-256 equals the checksum stored in
        OUR database. A copy that fails the check is dropped (and will be
        re-created from a good one) and we silently move on to the next copy -
        the client never sees corrupted bytes while a good copy exists.
        """
        chunk_id = chunk["chunk_id"]
        alive = {w["worker_id"]: w for w in self.alive_workers()}
        holders = [alive[h] for h in self.db.replica_workers(chunk_id) if h in alive]
        random.shuffle(holders)

        for worker in holders:
            wid = worker["worker_id"]
            try:
                data = await self.client.get_chunk(worker["url"], chunk_id)
            except WorkerUnreachable as exc:
                if exc.connect_failed:
                    await self.mark_dead(wid, "read failed: connection refused")
                continue
            except ChunkCorrupt:
                await self.drop_bad_replica(chunk_id, wid, "corrupt", "worker-side verification failed")
                continue
            except ChunkMissing:
                await self.drop_bad_replica(chunk_id, wid, "missing", "worker reports chunk not found")
                continue
            except WorkerError as exc:
                log.warning("read of %s from %s failed: %s", chunk_id, wid, exc)
                continue

            # Authoritative check against OUR checksum (the worker's sidecar could
            # itself have been damaged, so we never rely on the worker alone).
            if len(data) != chunk["size"] or hashlib.sha256(data).hexdigest() != chunk["checksum"]:
                await self.drop_bad_replica(chunk_id, wid, "corrupt", "controller-side checksum mismatch")
                continue
            return data

        raise ChunkUnavailable(chunk_id)

    def check_available(self, chunks) -> None:
        """
        Pre-flight for downloads: make sure EVERY chunk has at least one copy on
        a live worker BEFORE we send response headers. That way "data not
        available" becomes a clean 503 instead of a download that dies halfway.
        """
        if not chunks:
            return
        alive = {w["worker_id"] for w in self.alive_workers()}
        replicas = self.db.replicas_for_object(chunks[0]["object_id"])
        for c in chunks:
            if not any(h in alive for h in replicas.get(c["chunk_id"], [])):
                raise ApiError(
                    503,
                    "storage_unavailable",
                    f"no live copy of chunk {c['idx']} of this object is currently available",
                )

    async def stream_object(self, chunks) -> AsyncIterator[bytes]:
        """Yield the object's bytes chunk by chunk, in order, each one verified."""
        for c in chunks:
            try:
                yield await self.fetch_chunk(c)
            except ChunkUnavailable:
                # Headers are already sent, so we cannot change the status code.
                # Abort the stream: the client sees a truncated body (length
                # mismatch) rather than silently receiving wrong data.
                self.log_event(
                    "read_failed", f"chunk {c['chunk_id']} unreadable mid-download; stream aborted"
                )
                raise

    # ------------------------------------------------------ bad replicas
    async def drop_bad_replica(self, chunk_id: str, worker_id: str, kind: str, why: str) -> None:
        """
        Forget a copy that is corrupt ("corrupt") or gone ("missing").

        1. delete our record of the copy  -> the chunk is now under-replicated
        2. delete the bad file from the worker (if reachable) so it cannot be served
        3. wake the repair loop         -> it re-creates the copy from a good one
        """
        if not self.db.remove_replica(chunk_id, worker_id):
            return  # already handled (or the object was deleted meanwhile)
        self.log_event(
            "corruption_detected" if kind == "corrupt" else "replica_missing",
            f"chunk {chunk_id} on {worker_id}: {why}; copy discarded",
        )
        worker = self.db.get_worker(worker_id)
        if worker and worker["status"] == "alive":
            try:
                await self.client.delete_chunk(worker["url"], chunk_id)
            except (WorkerUnreachable, WorkerError):
                pass  # best effort; the orphan sweep will catch leftovers
        self.repair_event.set()

    # -------------------------------------------------------------- delete
    async def delete_copies(self, copies: list[tuple[str, str]]) -> None:
        """
        Physically delete chunk copies from workers (best effort).
        Copies on dead workers are skipped: when that worker returns, reconcile
        finds them unknown to the database and removes them (orphans).
        """
        urls = {w["worker_id"]: w["url"] for w in self.alive_workers()}
        gate = asyncio.Semaphore(16)  # don't open hundreds of connections at once

        async def one(chunk_id: str, worker_id: str) -> None:
            if worker_id not in urls:
                return
            async with gate:
                try:
                    await self.client.delete_chunk(urls[worker_id], chunk_id)
                except (WorkerUnreachable, WorkerError) as exc:
                    log.warning("could not delete %s on %s: %s", chunk_id, worker_id, exc)

        await asyncio.gather(*(one(c, w) for c, w in copies))

    async def discard_object(self, object_id: str) -> None:
        """Remove an object everywhere: database first (instantly invisible), then disks."""
        copies = self.db.delete_object(object_id)
        await self.delete_copies(copies)

    # ------------------------------------------------------------ reporting
    def replication_report(self, sample: int = 20) -> dict:
        """
        Classify every chunk by how many copies sit on ALIVE workers:

            healthy       == REPLICATION_FACTOR
            under         0 < copies < RF   (data readable, redundancy lost)
            unavailable   0 copies          (data temporarily unreadable)
            over          > RF              (surplus copy, will be trimmed)
        """
        alive = {w["worker_id"] for w in self.alive_workers()}
        rf = config.REPLICATION_FACTOR
        counts = {"healthy": 0, "under_replicated": 0, "unavailable": 0, "over_replicated": 0}
        under_sample: list[dict] = []
        for c in self.db.all_chunks_with_holders().values():
            n = sum(1 for h in c["holders"] if h in alive)
            if n == 0:
                counts["unavailable"] += 1
            elif n < rf:
                counts["under_replicated"] += 1
                if len(under_sample) < sample:
                    under_sample.append({"chunk_id": c["chunk_id"], "live_copies": n})
            elif n == rf:
                counts["healthy"] += 1
            else:
                counts["over_replicated"] += 1
        total = sum(counts.values())
        return {
            "replication_factor": rf,
            "total_chunks": total,
            **counts,
            "replication_ok": total == counts["healthy"],
            "under_replicated_sample": under_sample,
        }
