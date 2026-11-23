"""
Client-facing REST API:  /api/v1/objects/...

    POST   /api/v1/objects                         upload (multipart/form-data)     -> 201
    GET    /api/v1/objects                         list (paginated)                 -> 200
    GET    /api/v1/objects/{id}                    download                         -> 200
    GET    /api/v1/objects/{id}/metadata           metadata + chunk locations       -> 200
    PATCH  /api/v1/objects/{id}/metadata           add/update custom metadata       -> 200
    DELETE /api/v1/objects/{id}                    delete                           -> 204

Clients only ever talk to the controller; they never learn (or need) worker
addresses to upload or download (spec 5.2, D-04).
"""
import hashlib
import json
import logging
import uuid
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import config
from .db import utc_iso
from .errors import ApiError

log = logging.getLogger("controller.api")
router = APIRouter(prefix="/api/v1/objects", tags=["objects"])

# Custom metadata values must be simple JSON scalars (see D-09).
_SCALARS = (str, int, float, bool)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_metadata(meta, allow_null: bool) -> dict:
    """
    Check a custom-metadata dict: string keys, scalar values, bounded size.
    `allow_null` is True only for PATCH, where null means "delete this key".
    Raises ApiError(400) with a precise message otherwise.
    """
    if not isinstance(meta, dict):
        raise ApiError(400, "invalid_metadata", "metadata must be a JSON object of key/value pairs")
    for key, value in meta.items():
        if not isinstance(key, str) or key == "":
            raise ApiError(400, "invalid_metadata", "metadata keys must be non-empty strings")
        if value is None and allow_null:
            continue
        # bool is a subclass of int in Python, so it is covered by _SCALARS.
        if not isinstance(value, _SCALARS):
            raise ApiError(
                400,
                "invalid_metadata",
                f"value for key {key!r} must be a string, number or boolean"
                + ("" if allow_null else " (null is not allowed here)"),
            )
    if len(json.dumps(meta).encode()) > config.MAX_METADATA_BYTES:
        raise ApiError(400, "metadata_too_large", f"metadata exceeds {config.MAX_METADATA_BYTES} bytes")
    return meta


async def _read_full_chunk(upload: UploadFile) -> bytes:
    """
    Read up to CHUNK_SIZE bytes from the upload. Loops because a single read()
    is allowed to return fewer bytes than requested; we want every chunk to be
    exactly CHUNK_SIZE except the last one.
    """
    parts: list[bytes] = []
    remaining = config.CHUNK_SIZE
    while remaining > 0:
        piece = await upload.read(remaining)
        if not piece:
            break  # end of file
        parts.append(piece)
        remaining -= len(piece)
    return b"".join(parts)


def object_document(request: Request, obj) -> dict:
    """
    Build the full JSON description of an object: system metadata, custom
    metadata AND where every chunk is stored (spec section 3).
    """
    cluster = request.app.state.cluster
    db = cluster.db
    status_of = {w["worker_id"]: w["status"] for w in db.list_workers()}
    replicas = db.replicas_for_object(obj["object_id"])

    chunks = []
    for c in db.get_chunks(obj["object_id"]):
        holders = replicas.get(c["chunk_id"], [])
        chunks.append(
            {
                "chunk_id": c["chunk_id"],
                "index": c["idx"],
                "size": c["size"],
                "checksum": c["checksum"],
                # Where the copies live, and whether each worker is up right now.
                "locations": [
                    {"worker_id": h, "worker_status": status_of.get(h, "unknown")} for h in holders
                ],
                "live_copies": sum(1 for h in holders if status_of.get(h) == "alive"),
            }
        )
    return {
        "object_id": obj["object_id"],
        "name": obj["name"],
        "size": obj["size"],
        "content_type": obj["content_type"],
        "created_at": obj["created_at"],
        "modified_at": obj["modified_at"],
        "checksum": obj["checksum"],
        "checksum_algorithm": "sha256",
        "num_chunks": obj["num_chunks"],
        "chunk_size": config.CHUNK_SIZE,
        "replication_factor": config.REPLICATION_FACTOR,
        "custom_metadata": json.loads(obj["custom_metadata"]),
        "chunks": chunks,
    }


def _get_or_404(request: Request, object_id: str):
    obj = request.app.state.cluster.db.get_object(object_id)
    if obj is None:
        raise ApiError(404, "object_not_found", f"object {object_id} does not exist")
    return obj


# ---------------------------------------------------------------------------
# POST /api/v1/objects  - upload
# ---------------------------------------------------------------------------
@router.post("", status_code=201)
async def upload_object(
    request: Request,
    file: UploadFile = File(..., description="the binary object data"),
    metadata: Optional[str] = Form(None, description="JSON object with custom key/value metadata"),
):
    """
    Upload flow:
      1. validate input, make sure enough workers are alive
      2. create the object row in 'uploading' state (invisible to clients)
      3. for each 1 MiB slice: checksum it, record it, write it to 2 workers
      4. flip the object to 'ready' (now visible)
    If anything fails after step 2 the partial object is rolled back, so an
    upload is all-or-nothing (D-08).
    """
    cluster = request.app.state.cluster
    db = cluster.db

    # ---- 1. validate ---------------------------------------------------------
    custom: dict = {}
    if metadata:
        try:
            custom = json.loads(metadata)
        except ValueError:
            raise ApiError(400, "invalid_metadata", "the 'metadata' field is not valid JSON")
        _validate_metadata(custom, allow_null=False)

    if len(cluster.alive_workers()) < config.REPLICATION_FACTOR:
        # Fail fast instead of discovering this after reading a huge upload.
        raise ApiError(
            503,
            "insufficient_workers",
            f"need at least {config.REPLICATION_FACTOR} live storage workers to store an object",
        )

    name = file.filename or "unnamed"
    content_type = file.content_type or "application/octet-stream"
    object_id = uuid.uuid4().hex  # 32 hex chars, no dashes -> safe inside chunk ids/file names
    db.create_object(object_id, name, content_type, custom, utc_iso())

    # ---- 2/3. chunk, checksum, replicate --------------------------------------
    whole = hashlib.sha256()  # checksum of the entire object, built incrementally
    size = 0
    index = 0
    try:
        while True:
            data = await _read_full_chunk(file)
            if not data:
                break
            # Deterministic, unique, order-preserving id: "<object>_<6-digit index>".
            chunk_id = f"{object_id}_{index:06d}"
            chunk_sha = hashlib.sha256(data).hexdigest()
            # Record the chunk BEFORE writing copies, so a crash mid-write leaves
            # rows that rollback/cleanup can find (instead of invisible files).
            db.add_chunk(chunk_id, object_id, index, len(data), chunk_sha)
            await cluster.store_chunk(chunk_id, data, chunk_sha)
            whole.update(data)
            size += len(data)
            index += 1
        # ---- 4. commit ----------------------------------------------------------
        db.finalize_object(object_id, size, whole.hexdigest(), index)
    except Exception:
        log.exception("upload %s failed; rolling back", object_id)
        await cluster.discard_object(object_id)
        raise  # ApiError -> proper status; anything else -> 500

    cluster.log_event("object_stored", f"{object_id} '{name}' {size} bytes in {index} chunk(s)")
    doc = object_document(request, db.get_object(object_id))
    return JSONResponse(
        status_code=201, content=doc, headers={"Location": f"/api/v1/objects/{object_id}"}
    )


# ---------------------------------------------------------------------------
# GET /api/v1/objects  - list
# ---------------------------------------------------------------------------
@router.get("")
async def list_objects(
    request: Request,
    limit: int = Query(100, ge=1, le=1000, description="page size"),
    offset: int = Query(0, ge=0, description="number of objects to skip"),
):
    db = request.app.state.cluster.db
    rows = db.list_objects(limit, offset)
    return {
        "total": db.count_objects(),
        "limit": limit,
        "offset": offset,
        # Minimum required fields (spec 5.6) plus a couple of cheap extras.
        "objects": [
            {
                "object_id": r["object_id"],
                "name": r["name"],
                "size": r["size"],
                "content_type": r["content_type"],
                "created_at": r["created_at"],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# GET /api/v1/objects/{id}  - download
# ---------------------------------------------------------------------------
@router.get("/{object_id}")
async def download_object(object_id: str, request: Request):
    """
    Stream the object back chunk by chunk (memory use stays ~1 MiB even for
    huge objects). Every chunk is checksum-verified before it is sent; bad
    copies are skipped in favour of good ones.
    """
    cluster = request.app.state.cluster
    obj = _get_or_404(request, object_id)
    chunks = cluster.db.get_chunks(object_id)
    cluster.check_available(chunks)  # 503 up-front if any chunk has no live copy

    ascii_name = obj["name"].encode("ascii", "replace").decode().replace('"', "'")
    headers = {
        "Content-Length": str(obj["size"]),
        # filename* carries the real (possibly non-ASCII) name per RFC 6266.
        "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(obj['name'])}",
        # ETag + custom header let a client verify the whole object end-to-end.
        "ETag": f"\"{obj['checksum']}\"",
        "X-Object-SHA256": obj["checksum"],
        "X-Object-Id": obj["object_id"],
    }
    return StreamingResponse(
        cluster.stream_object(chunks), media_type=obj["content_type"], headers=headers
    )


# ---------------------------------------------------------------------------
# GET / PATCH /api/v1/objects/{id}/metadata
# ---------------------------------------------------------------------------
@router.get("/{object_id}/metadata")
async def get_metadata(object_id: str, request: Request):
    """Everything about the object EXCEPT its bytes."""
    return object_document(request, _get_or_404(request, object_id))


@router.patch("/{object_id}/metadata")
async def update_metadata(object_id: str, request: Request):
    """
    Merge the JSON body into the object's custom metadata (JSON-Merge-Patch
    style, D-09): present keys are added/overwritten, keys set to null are
    removed, absent keys are untouched. The object's data is not involved at
    all, so nothing is re-uploaded; only `modified_at` changes.
    """
    obj = _get_or_404(request, object_id)
    try:
        patch = await request.json()
    except ValueError:
        raise ApiError(400, "invalid_json", "request body is not valid JSON")
    _validate_metadata(patch, allow_null=True)

    merged = json.loads(obj["custom_metadata"])
    for key, value in patch.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value
    _validate_metadata(merged, allow_null=False)  # re-check the size of the RESULT

    db = request.app.state.cluster.db
    db.update_custom_metadata(object_id, merged, utc_iso())
    return object_document(request, db.get_object(object_id))


# ---------------------------------------------------------------------------
# DELETE /api/v1/objects/{id}
# ---------------------------------------------------------------------------
@router.delete("/{object_id}", status_code=204)
async def delete_object(object_id: str, request: Request):
    """
    Database first (the object vanishes for clients instantly and atomically),
    then the physical chunk files. Copies on a currently-dead worker are removed
    later by the orphan sweep when that worker returns (D-14).
    """
    cluster = request.app.state.cluster
    _get_or_404(request, object_id)
    await cluster.discard_object(object_id)
    cluster.log_event("object_deleted", f"{object_id} deleted")
    return Response(status_code=204)
