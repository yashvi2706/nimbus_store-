"""
Thin async wrapper around the workers' internal HTTP API.

Why a wrapper? Raw httpx can fail in a dozen different ways. The rest of the
controller only cares about FOUR outcomes, so we translate everything into them:

    WorkerUnreachable  - could not talk to the worker at all (down / network / timeout)
    ChunkMissing       - worker answered 404: it does not have the chunk
    ChunkCorrupt       - worker answered 422: it has the chunk but refuses to serve it
    WorkerError        - any other unexpected answer
"""
import httpx

from . import config


class WorkerUnreachable(Exception):
    """
    Network-level failure. `connect_failed` is True when we could not even open
    a connection - strong evidence the worker is down (container stopped), as
    opposed to a mere slow response.
    """

    def __init__(self, message: str, connect_failed: bool = False):
        super().__init__(message)
        self.connect_failed = connect_failed


class WorkerError(Exception):
    """The worker replied, but with an unexpected status."""

    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


class ChunkMissing(WorkerError):
    pass


class ChunkCorrupt(WorkerError):
    pass


class WorkerClient:
    def __init__(self) -> None:
        # ONE shared client = connection pooling (no TCP handshake per chunk).
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(
                config.WORKER_IO_TIMEOUT, connect=config.WORKER_CONNECT_TIMEOUT
            )
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Send a request, converting transport problems into WorkerUnreachable."""
        try:
            return await self._http.request(method, url, **kwargs)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            raise WorkerUnreachable(f"cannot connect to {url}: {exc}", connect_failed=True)
        except httpx.HTTPError as exc:  # read timeouts, dropped connections, ...
            raise WorkerUnreachable(f"{method} {url} failed: {exc.__class__.__name__}")

    @staticmethod
    def _detail(resp: httpx.Response) -> str:
        """Extract the worker's JSON error message if it sent one."""
        try:
            return resp.json().get("message", resp.text)
        except ValueError:
            return resp.text[:200]

    # --------------------------------------------------------------- chunks
    async def put_chunk(self, base_url: str, chunk_id: str, data: bytes, sha256: str) -> None:
        """Store a chunk. We send our checksum so the worker verifies what it received."""
        resp = await self._request(
            "PUT",
            f"{base_url}/internal/chunks/{chunk_id}",
            content=data,
            headers={"X-Chunk-SHA256": sha256, "Content-Type": "application/octet-stream"},
        )
        if resp.status_code not in (200, 201):
            raise WorkerError(resp.status_code, self._detail(resp))

    async def get_chunk(self, base_url: str, chunk_id: str) -> bytes:
        """Fetch a chunk's bytes. The CALLER must verify them against its own checksum."""
        resp = await self._request("GET", f"{base_url}/internal/chunks/{chunk_id}")
        if resp.status_code == 200:
            return resp.content
        if resp.status_code == 404:
            raise ChunkMissing(404, self._detail(resp))
        if resp.status_code == 422:
            raise ChunkCorrupt(422, self._detail(resp))
        raise WorkerError(resp.status_code, self._detail(resp))

    async def delete_chunk(self, base_url: str, chunk_id: str) -> None:
        resp = await self._request("DELETE", f"{base_url}/internal/chunks/{chunk_id}")
        if resp.status_code not in (200, 204, 404):
            raise WorkerError(resp.status_code, self._detail(resp))

    async def list_chunks(self, base_url: str, worker_id: str) -> list[dict]:
        """Deep-scan listing of what is physically on the worker (hashes recomputed)."""
        resp = await self._request("GET", f"{base_url}/internal/workers/{worker_id}/chunks")
        if resp.status_code != 200:
            raise WorkerError(resp.status_code, self._detail(resp))
        return resp.json()["chunks"]
