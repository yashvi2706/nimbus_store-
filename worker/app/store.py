"""
On-disk chunk storage for a worker.

Layout inside DATA_DIR (a persistent Docker volume):

    <DATA_DIR>/chunks/<chunk_id>.chunk   <- the raw chunk bytes
    <DATA_DIR>/chunks/<chunk_id>.meta    <- small JSON sidecar: sha256 + size

Why a sidecar? It lets the WORKER ITSELF notice bit-rot or tampering without
asking the controller: on every read we re-hash the bytes and compare with the
checksum recorded when the chunk was written. (The controller ALSO keeps its own
copy of every checksum and double-checks - see DECISIONS.md D-10.)

Everything in this module is synchronous/blocking disk I/O. The FastAPI layer
runs it in a thread pool so the event loop is never blocked.
"""
import hashlib
import json
import os
import re
import shutil
import uuid
from typing import Optional

# Chunk ids become FILE NAMES, so they must never contain "/" or ".." etc.
# Allowing only this small alphabet makes path traversal impossible.
# (Controller-generated ids look like "<32 hex chars>_000003".)
_VALID_CHUNK_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class InvalidChunkId(ValueError):
    """Raised when a chunk id could be unsafe to use as a file name."""


class ChunkNotFound(Exception):
    """The chunk file does not exist on this worker."""


class ChunkCorrupt(Exception):
    """The chunk file exists but its bytes do not match the recorded checksum."""


class ChecksumMismatchOnWrite(Exception):
    """The bytes we received do not match the checksum the sender claimed."""


def sha256_hex(data: bytes) -> str:
    """SHA-256 of `data` as a lowercase hex string."""
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    def __init__(self, root: str):
        self.chunks_dir = os.path.join(root, "chunks")
        # exist_ok: the directory persists across container restarts.
        os.makedirs(self.chunks_dir, exist_ok=True)

    # ------------------------------------------------------------------ paths
    def _paths(self, chunk_id: str) -> tuple[str, str]:
        """Return (data_path, meta_path) after validating the id."""
        if not _VALID_CHUNK_ID.match(chunk_id) or chunk_id in (".", ".."):
            raise InvalidChunkId(f"invalid chunk id: {chunk_id!r}")
        base = os.path.join(self.chunks_dir, chunk_id)
        return base + ".chunk", base + ".meta"

    @staticmethod
    def _atomic_write(path: str, payload: bytes) -> None:
        """
        Write `payload` to `path` so that readers see either the OLD file or the
        COMPLETE new file, never a half-written one:
          1. write to a uniquely named temp file in the same directory
          2. fsync (force the bytes onto the physical disk)
          3. rename over the final name (atomic on POSIX file systems)
        """
        tmp = f"{path}.{uuid.uuid4().hex}.tmp"
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    # ------------------------------------------------------------------ write
    def put(self, chunk_id: str, data: bytes, expected_sha256: Optional[str]) -> str:
        """
        Store a chunk. Returns the SHA-256 of what was stored.

        If the sender supplied a checksum we verify it BEFORE touching the disk,
        so corruption that happened in transit is rejected instead of being
        stored as if it were good data.
        """
        data_path, meta_path = self._paths(chunk_id)
        actual = sha256_hex(data)
        if expected_sha256 and expected_sha256.lower() != actual:
            raise ChecksumMismatchOnWrite(
                f"checksum mismatch: sender said {expected_sha256}, received {actual}"
            )
        # Data first, sidecar second. If we crash in between, the chunk has no
        # sidecar; the controller's own checksum still protects it.
        self._atomic_write(data_path, data)
        self._atomic_write(
            meta_path, json.dumps({"sha256": actual, "size": len(data)}).encode()
        )
        return actual

    # ------------------------------------------------------------------- read
    def get(self, chunk_id: str) -> tuple[bytes, str]:
        """
        Read a chunk and verify it against the sidecar checksum.
        Returns (bytes, sha256). Raises ChunkNotFound / ChunkCorrupt.
        """
        data_path, meta_path = self._paths(chunk_id)
        try:
            with open(data_path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            raise ChunkNotFound(chunk_id)

        actual = sha256_hex(data)
        recorded = self._read_meta(meta_path)
        if recorded is not None and recorded.get("sha256") != actual:
            # Bytes on disk changed since we wrote them -> refuse to serve them.
            raise ChunkCorrupt(
                f"{chunk_id}: stored checksum {recorded.get('sha256')} != actual {actual}"
            )
        return data, actual

    @staticmethod
    def _read_meta(meta_path: str) -> Optional[dict]:
        """Read the JSON sidecar; None if it is missing or unreadable."""
        try:
            with open(meta_path, "rb") as f:
                return json.loads(f.read())
        except (FileNotFoundError, ValueError):
            return None

    # ----------------------------------------------------------------- delete
    def delete(self, chunk_id: str) -> None:
        """Delete a chunk and its sidecar. Idempotent: missing files are fine."""
        data_path, meta_path = self._paths(chunk_id)
        for p in (data_path, meta_path):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    # ---------------------------------------------------------------- listing
    def list_chunks(self) -> list[dict]:
        """
        Describe every chunk on disk, re-hashing each file ("deep scan").

        The controller uses this to (a) find corrupted chunks, (b) find chunks it
        thinks we have but we don't, and (c) find orphans. We report the ACTUAL
        hash of the bytes, not the sidecar's claim, because the sidecar itself
        could be damaged.
        """
        out = []
        for name in sorted(os.listdir(self.chunks_dir)):
            if not name.endswith(".chunk"):
                continue  # skip .meta and leftover .tmp files
            chunk_id = name[: -len(".chunk")]
            data_path, meta_path = self._paths(chunk_id)
            try:
                with open(data_path, "rb") as f:
                    data = f.read()
                st = os.stat(data_path)
            except FileNotFoundError:
                continue  # deleted while we were scanning
            actual = sha256_hex(data)
            recorded = self._read_meta(meta_path)
            out.append(
                {
                    "chunk_id": chunk_id,
                    "size": len(data),
                    "sha256": actual,
                    # True when the sidecar agrees with the bytes.
                    "sidecar_ok": bool(recorded and recorded.get("sha256") == actual),
                    # Last-modified time; the controller uses it to avoid
                    # deleting chunks that were JUST written (see D-14).
                    "mtime": st.st_mtime,
                }
            )
        return out

    # ------------------------------------------------------------------ stats
    def stats(self) -> dict:
        """Cheap numbers for the heartbeat (no hashing)."""
        count, used = 0, 0
        for name in os.listdir(self.chunks_dir):
            if name.endswith(".chunk"):
                count += 1
                try:
                    used += os.path.getsize(os.path.join(self.chunks_dir, name))
                except FileNotFoundError:
                    pass
        free = shutil.disk_usage(self.chunks_dir).free
        return {"chunk_count": count, "used_bytes": used, "free_bytes": free}
