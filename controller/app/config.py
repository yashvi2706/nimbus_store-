"""
Controller configuration (all from environment variables - see DECISIONS.md D-02).

The defaults match the project specification. The timing values are chosen so a
live demo is watchable: failures are detected in seconds, not minutes.
"""
import os


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


# ---- Specification parameters ------------------------------------------------
# Chunk size: 1 MiB (1,048,576 bytes), exactly as required.
CHUNK_SIZE = _int("CHUNK_SIZE", 1024 * 1024)
# Number of copies of every chunk, each on a DIFFERENT worker.
REPLICATION_FACTOR = _int("REPLICATION_FACTOR", 2)

# ---- Storage -----------------------------------------------------------------
# SQLite file holding all metadata. In Docker it lives on the controller volume.
DB_PATH = os.environ.get("DB_PATH", "/data/controller.db")

# ---- Failure detection -------------------------------------------------------
# A worker that has sent no heartbeat for this many seconds is declared DEAD.
# Workers beat every ~2 s, so 6 s tolerates two lost heartbeats (network hiccup)
# before we react. Too small -> false alarms; too large -> slow detection.
HEARTBEAT_TIMEOUT = _float("HEARTBEAT_TIMEOUT", 6.0)
# How often the monitor loop checks for timed-out workers.
MONITOR_INTERVAL = _float("MONITOR_INTERVAL", 1.0)

# ---- Self-healing ------------------------------------------------------------
# After a worker dies we WAIT this long before re-creating its copies elsewhere.
# Reason: restarting a container usually takes seconds; copying gigabytes around
# for a 5-second blip would be wasteful (and then we'd delete the surplus again).
REPAIR_GRACE_SECONDS = _float("REPAIR_GRACE_SECONDS", 15.0)
# Maximum idle time between repair passes (a pass also runs immediately when
# something wakes it, e.g. a worker joining or a corrupt copy being found).
REPAIR_INTERVAL = _float("REPAIR_INTERVAL", 3.0)

# ---- Integrity scrubbing -----------------------------------------------------
# Every N seconds, deep-scan all workers and compare with our checksums.
# 0 disables the periodic scrub (it can still be triggered via the admin API).
SCRUB_INTERVAL = _float("SCRUB_INTERVAL", 60.0)
# Files on a worker that the controller does not know about are only deleted
# once they are older than this - a chunk being uploaded RIGHT NOW is briefly
# unknown to the database and must not be mistaken for garbage (D-14).
ORPHAN_GRACE_SECONDS = _float("ORPHAN_GRACE_SECONDS", 60.0)
# Uploads that never completed (crash, abandoned) are discarded after this long.
STALE_UPLOAD_SECONDS = _float("STALE_UPLOAD_SECONDS", 600.0)

# ---- Misc limits --------------------------------------------------------------
# Custom metadata is small by nature; cap it so it cannot bloat the database.
MAX_METADATA_BYTES = _int("MAX_METADATA_BYTES", 16 * 1024)
# httpx timeouts for controller -> worker calls (seconds).
WORKER_CONNECT_TIMEOUT = _float("WORKER_CONNECT_TIMEOUT", 2.0)
WORKER_IO_TIMEOUT = _float("WORKER_IO_TIMEOUT", 30.0)
