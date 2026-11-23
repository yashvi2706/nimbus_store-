"""
Controller entry point: builds the FastAPI app and starts the background jobs.

Run with:  uvicorn app.main:app --host 0.0.0.0 --port 8000
(The Dockerfile does exactly this.) Interactive API docs: http://localhost:8000/docs
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config
from .cluster import Cluster
from .db import Database
from .errors import install_error_handlers
from .maintenance import maintenance_loop, monitor_loop, scrub_loop
from .routes_client import router as client_router
from .routes_internal import admin as admin_router
from .routes_internal import internal as internal_router
from .worker_client import WorkerClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [controller] %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("controller")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start-up: open the database, create the Cluster, launch the background loops.
    Shutdown: stop the loops and close connections cleanly.
    """
    db = Database(config.DB_PATH)

    # An 'uploading' object at start-up means the previous controller died
    # mid-upload. It was never visible to clients, so simply discard it; its
    # stray chunk files on workers are removed later by the orphan sweep.
    for stale in db.stale_upload_ids("9999"):  # "9999" sorts after every real timestamp
        db.delete_object(stale)
        db.add_event("stale_upload_removed", f"discarded unfinished upload {stale} found at start-up")

    client = WorkerClient()
    cluster = Cluster(db, client)
    app.state.cluster = cluster  # handlers reach the cluster via request.app.state

    tasks = [
        asyncio.create_task(monitor_loop(cluster), name="monitor"),
        asyncio.create_task(maintenance_loop(cluster), name="maintenance"),
        asyncio.create_task(scrub_loop(cluster), name="scrub"),
    ]
    log.info(
        "controller ready: chunk_size=%d replication_factor=%d db=%s",
        config.CHUNK_SIZE, config.REPLICATION_FACTOR, config.DB_PATH,
    )
    yield
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.aclose()
    db.close()


app = FastAPI(
    title="Mini Distributed Cloud Object Store - Controller",
    description="Client REST API, internal worker API and admin endpoints.",
    version="1.0.0",
    lifespan=lifespan,
)
install_error_handlers(app)
app.include_router(client_router)
app.include_router(internal_router)
app.include_router(admin_router)
