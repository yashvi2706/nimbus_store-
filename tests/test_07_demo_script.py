"""
Runs the REAL demo script (demo/run_demo.py) end to end against the local test
cluster. The only thing swapped out is the Docker layer: instead of
`docker stop/start/exec` we kill/relaunch processes and read/write the worker's
data directory directly. Everything else - every API call, every check the demo
prints - is the genuine demo logic.
"""
import sys

from .conftest import ROOT

sys.path.insert(0, str(ROOT / "demo"))
import run_demo  # noqa: E402


class LocalOps:
    """Same interface as run_demo.DockerOps, backed by the local test cluster."""

    def __init__(self, cluster):
        self.c = cluster

    def stop(self, worker):
        self.c.stop_worker(worker)

    def start(self, worker):
        self.c.start_worker(worker)

    def chunk_sha256(self, worker, chunk_id):
        import hashlib
        return hashlib.sha256(self.c.chunk_path(worker, chunk_id).read_bytes()).hexdigest()

    def corrupt(self, worker, chunk_id):
        with open(self.c.chunk_path(worker, chunk_id), "r+b") as f:   # same effect as dd conv=notrunc
            f.seek(4096)
            f.write(b"CORRUPT")


def test_full_demo_passes(cluster, capsys):
    code = run_demo.main(["--url", cluster.base, "--cleanup"], ops=LocalOps(cluster))
    out = capsys.readouterr().out
    assert "[FAIL]" not in out, out
    assert code == 0
    assert "ALL CHECKS PASSED" in out
