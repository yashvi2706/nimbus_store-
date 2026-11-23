"""
Background jobs that keep the cluster healthy. Three long-running loops:

    monitor_loop      every ~1 s   declare workers DEAD when heartbeats stop
    maintenance_loop  on demand    reconcile rejoined workers, then REPAIR
                      (+ every few seconds as a safety net)
    scrub_loop        every ~60 s  deep-scan all workers for corruption

...and the functions they (and the admin API) call:

    reconcile_worker  compare one worker's disk with the database
    repair_chunks     restore missing copies / trim surplus copies
    maintenance_pass  reconcile pending workers + repair + cleanup
    scrub_all         reconcile EVERY alive worker + repair

Concurrency rule (D-13): reconcile/repair/scrub all run while holding
`cluster.maintenance_lock`, so only one of them works at a time.
"""
import asyncio
import logging
import time

from . import config
from .cluster import ChunkUnavailable, Cluster
from .db import utc_iso
from .worker_client import WorkerError, WorkerUnreachable

log = logging.getLogger("controller.maintenance")


# ===========================================================================
# 1. Failure detection
# ===========================================================================
async def monitor_loop(cluster: Cluster) -> None:
    """
    A worker is DEAD if its last heartbeat is older than HEARTBEAT_TIMEOUT.
    (Faster detection also happens elsewhere: a refused connection during a
    read/write marks the worker dead immediately.)
    """
    while True:
        await asyncio.sleep(config.MONITOR_INTERVAL)
        try:
            now = time.time()
            for w in cluster.db.list_workers(status="alive"):
                silent_for = now - w["last_heartbeat"]
                if silent_for > config.HEARTBEAT_TIMEOUT:
                    await cluster.mark_dead(
                        w["worker_id"], f"no heartbeat for {silent_for:.1f}s"
                    )
        except Exception:  # noqa: BLE001 - a monitor crash would blind us
            log.exception("monitor loop error")


# ===========================================================================
# 2. Reconcile: compare a worker's REAL disk with our records
# ===========================================================================
async def reconcile_worker(cluster: Cluster, worker_id: str) -> dict | None:
    """
    Deep-scan one worker and fix every disagreement with the database:

      * we expect a chunk, worker lacks it       -> forget the copy   ("missing")
      * we expect a chunk, hash/size is wrong    -> discard the copy  ("corrupt")
      * worker has a chunk we do not know about  -> delete it         ("orphan")

    Returns counts, or None if the worker could not be reached.
    Caller must hold cluster.maintenance_lock.
    """
    db = cluster.db
    worker = db.get_worker(worker_id)
    if not worker or worker["status"] != "alive":
        return None

    # ORDER MATTERS (D-14): snapshot the database BEFORE asking the worker.
    # A chunk written after the snapshot is simply not in `expected`, so it can
    # never be misreported as "missing".
    expected = {r["chunk_id"]: r for r in db.chunks_on_worker(worker_id)}
    try:
        actual = {c["chunk_id"]: c for c in await cluster.client.list_chunks(worker["url"], worker_id)}
    except (WorkerUnreachable, WorkerError) as exc:
        log.warning("reconcile of %s failed: %s", worker_id, exc)
        return None

    result = {"corrupt": 0, "missing": 0, "orphans": 0, "checked": len(actual)}

    # ---- chunks we expect ----------------------------------------------------
    for chunk_id, rec in expected.items():
        found = actual.get(chunk_id)
        if found is None:
            if db.has_replica(chunk_id, worker_id):  # skip if deleted meanwhile
                result["missing"] += 1
                await cluster.drop_bad_replica(chunk_id, worker_id, "missing", "chunk absent from worker disk")
        elif found["sha256"] != rec["checksum"] or found["size"] != rec["size"]:
            result["corrupt"] += 1
            await cluster.drop_bad_replica(
                chunk_id, worker_id, "corrupt", "on-disk checksum differs from recorded checksum"
            )

    # ---- orphans: on disk but unknown to the database ---------------------------
    now = time.time()
    for chunk_id, found in actual.items():
        if chunk_id in expected:
            continue
        if now - found["mtime"] < config.ORPHAN_GRACE_SECONDS:
            continue  # too young: might be an upload still in progress
        if db.has_replica(chunk_id, worker_id):
            continue  # recorded after our snapshot -> legitimate
        try:
            await cluster.client.delete_chunk(worker["url"], chunk_id)
            result["orphans"] += 1
            cluster.log_event("orphan_removed", f"deleted unreferenced chunk {chunk_id} from {worker_id}")
        except (WorkerUnreachable, WorkerError):
            pass
    return result


# ===========================================================================
# 3. Repair: restore the replication factor
# ===========================================================================
async def repair_chunks(cluster: Cluster) -> dict:
    """
    Walk every chunk and bring it to exactly REPLICATION_FACTOR live copies.

    Copies on a worker that died RECENTLY (< REPAIR_GRACE_SECONDS ago) still
    "count": we give the worker a chance to come back before moving data.
    Caller must hold cluster.maintenance_lock.
    """
    db = cluster.db
    rf = config.REPLICATION_FACTOR
    now = time.time()
    workers = {w["worker_id"]: w for w in db.list_workers()}
    alive = {i for i, w in workers.items() if w["status"] == "alive"}
    recently_dead = {
        i
        for i, w in workers.items()
        if w["status"] == "dead"
        and w["dead_since"] is not None
        and now - w["dead_since"] < config.REPAIR_GRACE_SECONDS
    }
    load = db.worker_load()  # kept up to date locally while we trim

    stats = {"copied": 0, "trimmed": 0, "unavailable": 0, "waiting": 0, "failed": 0}

    for chunk in list(db.all_chunks_with_holders().values()):
        holders = chunk["holders"]
        live = [h for h in holders if h in alive]
        counted = len(live) + sum(1 for h in holders if h in recently_dead)

        # ---------------- too many copies -> trim the surplus ------------------
        if len(live) > rf:
            surplus = len(live) - rf
            # Remove from the busiest workers first: this also rebalances.
            victims = sorted(live, key=lambda h: load.get(h, {}).get("chunks", 0), reverse=True)[:surplus]
            for victim in victims:
                db.remove_replica(chunk["chunk_id"], victim)
                try:
                    await cluster.client.delete_chunk(workers[victim]["url"], chunk["chunk_id"])
                except (WorkerUnreachable, WorkerError):
                    pass  # leftover file becomes an orphan and is swept later
                if victim in load:
                    load[victim]["chunks"] -= 1
                stats["trimmed"] += 1
            continue

        # ---------------- enough copies (or still waiting) -----------------------
        if counted >= rf:
            if len(live) < rf:
                stats["waiting"] += 1  # a recently-dead worker may still return
            continue

        # ---------------- too few copies -> re-replicate ---------------------------
        if not live:
            stats["unavailable"] += 1  # no source to copy from; wait for a worker
            continue
        need = rf - counted
        targets = cluster.choose_workers(need, exclude=holders)
        if len(targets) < need:
            stats["failed"] += 1  # not enough free live workers right now
        try:
            data = await cluster.fetch_chunk(chunk)  # verified read from a good copy
        except ChunkUnavailable:
            stats["unavailable"] += 1
            continue
        for target in targets:
            failure = await cluster._try_put(target, chunk["chunk_id"], data, chunk["checksum"])
            if failure is None:
                db.add_replica(chunk["chunk_id"], target["worker_id"], time.time())
                load.setdefault(target["worker_id"], {"chunks": 0, "bytes": 0})["chunks"] += 1
                stats["copied"] += 1
                log.info("re-replicated %s -> %s", chunk["chunk_id"], target["worker_id"])
            else:
                stats["failed"] += 1

    if stats["copied"] or stats["trimmed"]:
        cluster.log_event(
            "repair",
            f"restored {stats['copied']} missing cop{'y' if stats['copied'] == 1 else 'ies'}, "
            f"trimmed {stats['trimmed']} surplus cop{'y' if stats['trimmed'] == 1 else 'ies'}",
        )
    return stats


# ===========================================================================
# 4. Orchestration
# ===========================================================================
async def _purge_stale_uploads(cluster: Cluster) -> int:
    """Discard uploads that started long ago and never completed."""
    cutoff = utc_iso(time.time() - config.STALE_UPLOAD_SECONDS)
    ids = cluster.db.stale_upload_ids(cutoff)
    for object_id in ids:
        await cluster.discard_object(object_id)
        cluster.log_event("stale_upload_removed", f"discarded unfinished upload {object_id}")
    return len(ids)


async def maintenance_pass(cluster: Cluster) -> dict:
    """Reconcile workers that just joined, then repair, then tidy up."""
    async with cluster.maintenance_lock:
        reconciled: dict[str, dict] = {}
        retry: list[str] = []
        pending, cluster.pending_reconcile = list(cluster.pending_reconcile), set()
        for wid in pending:
            res = await reconcile_worker(cluster, wid)
            if res is None:
                w = cluster.db.get_worker(wid)
                if w and w["status"] == "alive":
                    retry.append(wid)  # alive but scan failed: try again next pass
            else:
                reconciled[wid] = res
        cluster.pending_reconcile.update(retry)
        repair = await repair_chunks(cluster)
        purged = await _purge_stale_uploads(cluster)
        return {"reconciled": reconciled, "repair": repair, "stale_uploads_removed": purged}


async def scrub_all(cluster: Cluster) -> dict:
    """Deep-scan EVERY alive worker for corruption, then repair what was found."""
    async with cluster.maintenance_lock:
        scanned: dict[str, dict | None] = {}
        for w in cluster.alive_workers():
            scanned[w["worker_id"]] = await reconcile_worker(cluster, w["worker_id"])
        repair = await repair_chunks(cluster)
        return {"scanned": scanned, "repair": repair}


async def maintenance_loop(cluster: Cluster) -> None:
    """Run a maintenance pass when woken, or every REPAIR_INTERVAL seconds."""
    while True:
        try:
            await asyncio.wait_for(cluster.repair_event.wait(), timeout=config.REPAIR_INTERVAL)
        except asyncio.TimeoutError:
            pass
        # Clear BEFORE the pass: anything that happens during the pass sets the
        # event again and triggers one more pass, so nothing is missed.
        cluster.repair_event.clear()
        try:
            await maintenance_pass(cluster)
        except Exception:  # noqa: BLE001
            log.exception("maintenance pass failed")


async def scrub_loop(cluster: Cluster) -> None:
    """Periodic integrity scan (disabled when SCRUB_INTERVAL <= 0)."""
    if config.SCRUB_INTERVAL <= 0:
        return
    while True:
        await asyncio.sleep(config.SCRUB_INTERVAL)
        try:
            await scrub_all(cluster)
        except Exception:  # noqa: BLE001
            log.exception("scrub failed")
