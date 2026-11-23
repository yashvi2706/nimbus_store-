"""
Controller-side INTERNAL API (used by workers) and ADMIN API (used by operators
and the demo). Neither is part of the client-facing /api/v1/objects contract.

Internal (spec section 6, controller half):
    POST /internal/workers/register
    POST /internal/workers/heartbeat
    GET  /internal/workers/{worker_id}/chunks     <- the controller's RECORD of a worker's chunks

Admin (extra, for visibility/demo; D-16):
    GET  /api/v1/admin/cluster      workers + replication health
    GET  /api/v1/admin/events       recent operational events
    POST /api/v1/admin/scrub        deep-scan all workers for corruption NOW
    POST /api/v1/admin/repair       run a maintenance/repair pass NOW
    GET  /api/v1/admin/chunkmap     which worker holds which chunk (feeds the dashboard)
    GET  /dashboard                 live HTML dashboard with the chunk map
    GET  /health                    liveness probe
"""
import re
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import config
from .errors import ApiError
from .maintenance import maintenance_pass, scrub_all

internal = APIRouter(tags=["internal"])
admin = APIRouter(prefix="/api/v1/admin", tags=["admin"])

_WORKER_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
# The dashboard is one static HTML file that polls the admin API (D-28).
_DASHBOARD_FILE = Path(__file__).parent / "static" / "dashboard.html"


# ---------------------------------------------------------------------------
# Request bodies (pydantic validates types; invalid -> 400 via errors.py)
# ---------------------------------------------------------------------------
class RegisterRequest(BaseModel):
    worker_id: str = Field(..., description="unique worker name, e.g. worker2")
    url: str = Field(..., description="address the controller uses to reach the worker")


class HeartbeatRequest(BaseModel):
    worker_id: str
    # Optional statistics; the spec says request formats may be extended.
    chunk_count: int = 0
    used_bytes: int = 0
    free_bytes: Optional[int] = None


# ---------------------------------------------------------------------------
# Internal API
# ---------------------------------------------------------------------------
@internal.post("/internal/workers/register")
async def register_worker(body: RegisterRequest, request: Request):
    """A worker announces itself (on start-up, and again if we forgot it)."""
    if not _WORKER_ID.match(body.worker_id):
        raise ApiError(400, "invalid_worker_id", "worker_id may only contain letters, digits, _ . -")
    if not body.url.startswith(("http://", "https://")):
        raise ApiError(400, "invalid_url", "url must start with http:// or https://")
    cluster = request.app.state.cluster
    previous = cluster.db.upsert_worker(body.worker_id, body.url.rstrip("/"), time.time())
    cluster.worker_seen(body.worker_id, previous)
    return {
        "status": "registered",
        "worker_id": body.worker_id,
        # Tell the worker the cadence we expect, so there is one source of truth.
        "heartbeat_timeout_seconds": config.HEARTBEAT_TIMEOUT,
    }


@internal.post("/internal/workers/heartbeat")
async def heartbeat(body: HeartbeatRequest, request: Request):
    """
    "I am alive" signal. 404 for an unknown worker makes it re-register - this
    is how workers recover if the controller's database was reset (D-11).
    """
    cluster = request.app.state.cluster
    previous = cluster.db.touch_heartbeat(body.worker_id, time.time(), body.model_dump())
    if previous is None:
        raise ApiError(404, "unknown_worker", f"worker {body.worker_id} is not registered")
    cluster.worker_seen(body.worker_id, previous)
    return {"status": "ok"}


@internal.get("/internal/workers/{worker_id}/chunks")
async def chunks_of_worker(worker_id: str, request: Request):
    """
    What the CONTROLLER believes a worker stores. (The worker serves the same
    path to describe what it REALLY stores; see worker/app/main.py.)
    """
    db = request.app.state.cluster.db
    if db.get_worker(worker_id) is None:
        raise ApiError(404, "unknown_worker", f"worker {worker_id} is not registered")
    rows = db.chunks_on_worker(worker_id)
    return {
        "worker_id": worker_id,
        "chunk_count": len(rows),
        "chunks": [
            {
                "chunk_id": r["chunk_id"],
                "object_id": r["object_id"],
                "index": r["idx"],
                "size": r["size"],
                "checksum": r["checksum"],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Admin API
# ---------------------------------------------------------------------------
@admin.get("/cluster")
async def cluster_status(request: Request):
    """Snapshot of the whole cluster: every worker plus replication health."""
    cluster = request.app.state.cluster
    now = time.time()
    load = cluster.db.worker_load()
    workers = []
    for w in cluster.db.list_workers():
        alive = w["status"] == "alive"
        workers.append(
            {
                "worker_id": w["worker_id"],
                "url": w["url"],
                "status": w["status"],
                "seconds_since_heartbeat": round(now - w["last_heartbeat"], 1),
                "seconds_dead": None if alive or w["dead_since"] is None else round(now - w["dead_since"], 1),
                # What the database says is on this worker...
                "chunks_recorded": load.get(w["worker_id"], {}).get("chunks", 0),
                "bytes_recorded": load.get(w["worker_id"], {}).get("bytes", 0),
                # ...and what the worker itself last reported.
                "chunks_reported": w["reported_chunks"],
                "free_bytes": w["free_bytes"],
            }
        )
    return {
        "workers": workers,
        "alive_workers": sum(1 for w in workers if w["status"] == "alive"),
        "replication": cluster.replication_report(),
    }


def _chunk_state(live_copies: int, rf: int) -> str:
    """Same classification as Cluster.replication_report, for one chunk."""
    if live_copies == 0:
        return "unavailable"
    if live_copies < rf:
        return "under_replicated"
    if live_copies == rf:
        return "healthy"
    return "over_replicated"


@admin.get("/chunkmap")
async def chunk_map(request: Request, max_objects: int = Query(100, ge=1, le=1000)):
    """
    Which worker holds which chunk, per object (newest objects first).

    This is the CONTROLLER'S RECORD (the database), so it shows what the system
    believes; the live/dead status of each holder comes from the heartbeats.
    `summary` covers every object; `objects` is cut at `max_objects`.
    """
    db = request.app.state.cluster.db
    rf = config.REPLICATION_FACTOR
    status_of = {w["worker_id"]: w["status"] for w in db.list_workers()}

    by_object: dict[str, list[dict]] = {}
    summary = {"healthy": 0, "under_replicated": 0, "unavailable": 0, "over_replicated": 0}
    held_by_worker: dict[str, int] = {}
    for c in db.all_chunks_with_holders().values():
        live = sum(1 for h in c["holders"] if status_of.get(h) == "alive")
        state = _chunk_state(live, rf)
        summary[state] += 1
        for h in c["holders"]:
            held_by_worker[h] = held_by_worker.get(h, 0) + 1
        by_object.setdefault(c["object_id"], []).append(
            {
                "chunk_id": c["chunk_id"],
                "index": c["idx"],
                "size": c["size"],
                "live_copies": live,
                "state": state,
                "holders": [{"worker_id": h, "status": status_of.get(h, "unknown")} for h in c["holders"]],
            }
        )

    total_objects = db.count_objects()
    objects = []
    for obj in db.list_objects(max_objects, 0):  # newest first
        chunks = by_object.get(obj["object_id"], [])
        objects.append(
            {
                "object_id": obj["object_id"],
                "name": obj["name"],
                "size": obj["size"],
                "created_at": obj["created_at"],
                "num_chunks": obj["num_chunks"],
                "chunks": chunks,
            }
        )

    return {
        "replication_factor": rf,
        "chunk_size": config.CHUNK_SIZE,
        "workers": [
            {
                "worker_id": w,
                "status": status_of[w],
                "chunks": held_by_worker.get(w, 0),
            }
            for w in sorted(status_of)
        ],
        "summary": {**summary, "total_chunks": sum(summary.values())},
        "objects_total": total_objects,
        "objects_shown": len(objects),
        "objects": objects,
    }


@admin.get("/events")
async def events(request: Request, limit: int = Query(50, ge=1, le=500)):
    """Most recent operational events, oldest first."""
    return {"events": request.app.state.cluster.db.recent_events(limit)}


@admin.post("/scrub")
async def run_scrub(request: Request):
    """Deep-scan every alive worker for corruption right now, then repair."""
    return await scrub_all(request.app.state.cluster)


@admin.post("/repair")
async def run_repair(request: Request):
    """Run a reconcile + repair pass right now (instead of waiting for the timer)."""
    return await maintenance_pass(request.app.state.cluster)


@internal.get("/dashboard", include_in_schema=False)
async def dashboard():
    """Serve the live dashboard page (static/dashboard.html)."""
    if not _DASHBOARD_FILE.is_file():
        raise ApiError(404, "dashboard_not_installed", "controller/app/static/dashboard.html is missing")
    return FileResponse(_DASHBOARD_FILE, media_type="text/html")


@internal.get("/health")
async def health():
    """Liveness probe used by Docker's healthcheck."""
    return {"status": "ok"}
