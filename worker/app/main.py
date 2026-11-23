"""
Storage worker.

A worker is deliberately "dumb": it stores, returns and deletes opaque chunks
and tells the controller it is alive. All decisions (where chunks go, when to
repair) belong to the controller. See DECISIONS.md D-03.

Internal API implemented here (all under /internal, never exposed to clients):
    PUT    /internal/chunks/{chunk_id}             store a chunk
    GET    /internal/chunks/{chunk_id}             read a chunk (checksum-verified)
    DELETE /internal/chunks/{chunk_id}             delete a chunk (idempotent)
    GET    /internal/workers/{worker_id}/chunks    deep-scan listing of what is on disk
    GET    /health                                 liveness probe for Docker
"""
import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response

from . import config
from .store import (
    ChecksumMismatchOnWrite,
    ChunkCorrupt,
    ChunkNotFound,
    ChunkStore,
    InvalidChunkId,
)

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{config.WORKER_ID}] %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

store = ChunkStore(config.DATA_DIR)


def _error(status: int, code: str, message: str) -> JSONResponse:
    """All error responses share one JSON shape: {"error": ..., "message": ...}."""
    return JSONResponse(status_code=status, content={"error": code, "message": message})


# --------------------------------------------------------------------------
# Background task: register with the controller, then heartbeat forever.
# --------------------------------------------------------------------------
async def controller_link_loop() -> None:
    """
    Keeps this worker known to the controller.

    * Until registration succeeds we retry every heartbeat interval - the
      controller may simply still be starting (docker compose starts everything
      at roughly the same time).
    * Once registered we send a heartbeat every HEARTBEAT_INTERVAL seconds.
    * If a heartbeat gets 404 the controller forgot us (e.g. its DB was wiped),
      so we register again.
    * Errors never kill the loop: a controller outage must not crash workers.
    """
    registered = False
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                if not registered:
                    r = await client.post(
                        f"{config.CONTROLLER_URL}/internal/workers/register",
                        json={"worker_id": config.WORKER_ID, "url": config.WORKER_URL},
                    )
                    r.raise_for_status()
                    registered = True
                    log.info("registered with controller at %s", config.CONTROLLER_URL)
                else:
                    stats = await run_in_threadpool(store.stats)
                    r = await client.post(
                        f"{config.CONTROLLER_URL}/internal/workers/heartbeat",
                        json={"worker_id": config.WORKER_ID, **stats},
                    )
                    if r.status_code == 404:
                        log.warning("controller does not know us; re-registering")
                        registered = False
                        continue  # retry immediately
                    r.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - loop must survive anything
                log.warning("controller unreachable (%s); will retry", exc.__class__.__name__)
            await asyncio.sleep(config.HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the heartbeat task with the app and stop it on shutdown."""
    task = asyncio.create_task(controller_link_loop())
    log.info("worker %s up, data dir %s", config.WORKER_ID, config.DATA_DIR)
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title=f"Storage worker {config.WORKER_ID}", lifespan=lifespan)


# --------------------------------------------------------------------------
# Internal chunk API
# --------------------------------------------------------------------------
@app.put("/internal/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    """
    Store a chunk. The body is the raw bytes. The optional X-Chunk-SHA256 header
    carries the sender's checksum; if it does not match the bytes we received
    the write is rejected (data was damaged in transit).
    """
    data = await request.body()
    if len(data) > config.MAX_CHUNK_BYTES:
        return _error(413, "chunk_too_large", f"chunk exceeds {config.MAX_CHUNK_BYTES} bytes")
    try:
        # Hashing + fsync are blocking -> run them off the event loop.
        sha = await run_in_threadpool(
            store.put, chunk_id, data, request.headers.get("X-Chunk-SHA256")
        )
    except InvalidChunkId as exc:
        return _error(400, "invalid_chunk_id", str(exc))
    except ChecksumMismatchOnWrite as exc:
        return _error(400, "checksum_mismatch", str(exc))
    except OSError as exc:  # disk full, permissions, ...
        log.error("write failed for %s: %s", chunk_id, exc)
        return _error(500, "storage_error", f"could not write chunk: {exc}")
    return JSONResponse(
        status_code=201, content={"chunk_id": chunk_id, "size": len(data), "sha256": sha}
    )


@app.get("/internal/chunks/{chunk_id}")
def get_chunk(chunk_id: str):
    """
    Return the chunk bytes. Plain `def` (not async): FastAPI runs it in a thread
    pool, which is what we want for blocking file reads.

    404 -> we do not have it.
    422 -> we have it but it fails verification (corrupted); we refuse to serve it.
    """
    try:
        data, sha = store.get(chunk_id)
    except InvalidChunkId as exc:
        return _error(400, "invalid_chunk_id", str(exc))
    except ChunkNotFound:
        return _error(404, "chunk_not_found", f"chunk {chunk_id} not stored here")
    except ChunkCorrupt as exc:
        log.error("CORRUPTION detected on read: %s", exc)
        return _error(422, "chunk_corrupted", str(exc))
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"X-Chunk-SHA256": sha},
    )


@app.delete("/internal/chunks/{chunk_id}")
def delete_chunk(chunk_id: str):
    """Delete a chunk. Idempotent: deleting a missing chunk is still 204."""
    try:
        store.delete(chunk_id)
    except InvalidChunkId as exc:
        return _error(400, "invalid_chunk_id", str(exc))
    return Response(status_code=204)


@app.get("/internal/workers/{worker_id}/chunks")
def list_chunks(worker_id: str):
    """
    Deep-scan listing of every chunk physically on this worker.

    NOTE: the path contains a worker id because the spec defines it that way.
    The controller exposes an endpoint with the same path describing ITS records
    for that worker; this one describes what is REALLY on disk. Comparing the two
    is how the controller finds corruption, missing chunks and orphans (D-12).
    """
    if worker_id != config.WORKER_ID:
        return _error(404, "wrong_worker", f"this is {config.WORKER_ID}, not {worker_id}")
    chunks = store.list_chunks()
    return {"worker_id": config.WORKER_ID, "chunk_count": len(chunks), "chunks": chunks}


@app.get("/health")
def health():
    """Liveness probe used by Docker's healthcheck."""
    return {"status": "ok", "worker_id": config.WORKER_ID}
