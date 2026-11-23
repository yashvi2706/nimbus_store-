"""
Test harness: launches a REAL cluster (1 controller + 4 workers) as local
processes - no Docker needed - and gives tests helpers to poke it.

Why not mock everything? The interesting behaviour (heartbeats, failover,
repair) only exists when the real processes talk over real HTTP. Killing a
worker process here is the local equivalent of `docker stop worker2`
(see DECISIONS.md D-18).

Timing values are shortened so the whole suite runs in about a minute.
"""
import hashlib
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parent.parent
NUM_WORKERS = 4


def free_port() -> int:
    """Ask the OS for an unused TCP port."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class LocalCluster:
    """Owns the controller and worker subprocesses plus their temp directories."""

    def __init__(self, tmp: str):
        self.tmp = Path(tmp)
        self.controller_port = free_port()
        self.base = f"http://127.0.0.1:{self.controller_port}"
        self.worker_ports = {f"worker{i}": free_port() for i in range(1, NUM_WORKERS + 1)}
        self.procs: dict[str, subprocess.Popen] = {}

    # ------------------------------------------------------------ processes
    def _spawn(self, name: str, cwd: Path, env: dict) -> None:
        log = open(self.tmp / f"{name}.log", "ab")
        port = env["PORT"]
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=cwd, env={**os.environ, **env}, stdout=log, stderr=log,
        )

    def start_controller(self) -> None:
        self._spawn("controller", ROOT / "controller", {
            "PORT": str(self.controller_port),
            "DB_PATH": str(self.tmp / "controller" / "controller.db"),
            # Fast timers so failure/repair is visible within seconds.
            "HEARTBEAT_TIMEOUT": "2.0",
            "MONITOR_INTERVAL": "0.3",
            "REPAIR_GRACE_SECONDS": "4.0",
            "REPAIR_INTERVAL": "0.5",
            "SCRUB_INTERVAL": "0",             # tests trigger scrubs explicitly
            "ORPHAN_GRACE_SECONDS": "1.0",
        })

    def start_worker(self, name: str) -> None:
        port = self.worker_ports[name]
        self._spawn(name, ROOT / "worker", {
            "PORT": str(port),
            "WORKER_ID": name,
            "WORKER_URL": f"http://127.0.0.1:{port}",
            "CONTROLLER_URL": self.base,
            "DATA_DIR": str(self.tmp / name),  # "persistent volume" = temp dir
            "HEARTBEAT_INTERVAL": "0.5",
        })

    def stop_worker(self, name: str) -> None:
        """Equivalent of `docker stop <worker>`: the process disappears abruptly."""
        proc = self.procs[name]
        proc.send_signal(signal.SIGKILL)
        proc.wait()

    def restart_controller(self) -> None:
        """Kill the controller abruptly and start a new one on the SAME database file."""
        proc = self.procs["controller"]
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        self.start_controller()

    def stop_all(self) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    # ---------------------------------------------------------- API helpers
    def api(self, path: str) -> str:
        return self.base + path

    def status(self) -> dict:
        return requests.get(self.api("/api/v1/admin/cluster"), timeout=5).json()

    def events(self, limit: int = 200) -> list[dict]:
        return requests.get(self.api(f"/api/v1/admin/events?limit={limit}"), timeout=5).json()["events"]

    def upload(self, data: bytes, name="blob.bin", content_type="application/octet-stream", metadata=None):
        files = {"file": (name, data, content_type)}
        form = {"metadata": __import__("json").dumps(metadata)} if metadata is not None else {}
        return requests.post(self.api("/api/v1/objects"), files=files, data=form, timeout=60)

    def download(self, object_id: str) -> requests.Response:
        return requests.get(self.api(f"/api/v1/objects/{object_id}"), timeout=60)

    def metadata(self, object_id: str) -> dict:
        return requests.get(self.api(f"/api/v1/objects/{object_id}/metadata"), timeout=5).json()

    # ------------------------------------------------------- disk helpers
    def chunk_path(self, worker: str, chunk_id: str) -> Path:
        return self.tmp / worker / "chunks" / f"{chunk_id}.chunk"

    def wait_for(self, predicate, timeout=30.0, interval=0.2, what="condition"):
        """Poll until predicate() is truthy; fail the test with a clear message on timeout."""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except requests.RequestException:
                pass
            time.sleep(interval)
        raise AssertionError(f"timed out after {timeout}s waiting for {what}")


@pytest.fixture(scope="session")
def cluster():
    """One cluster shared by the whole test session (starting 5 processes is slow-ish)."""
    with tempfile.TemporaryDirectory(prefix="objstore-test-") as tmp:
        c = LocalCluster(tmp)
        c.start_controller()
        c.wait_for(lambda: requests.get(c.api("/health"), timeout=1).ok, what="controller up")
        for name in c.worker_ports:
            c.start_worker(name)
        c.wait_for(lambda: c.status()["alive_workers"] == NUM_WORKERS, what="4 workers registered")
        try:
            yield c
        finally:
            c.stop_all()
