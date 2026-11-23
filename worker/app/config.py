"""
Worker configuration.

Every setting comes from an environment variable so the SAME code runs
  * inside Docker  (values come from docker-compose.yml), and
  * as a plain local process (values come from tests/conftest.py).

See DECISIONS.md (D-02) for why configuration is environment-driven.
"""
import os


def _float(name: str, default: float) -> float:
    """Read a float from the environment, falling back to `default`."""
    return float(os.environ.get(name, default))


def _int(name: str, default: int) -> int:
    """Read an int from the environment, falling back to `default`."""
    return int(os.environ.get(name, default))


# Unique name of this worker, e.g. "worker2". REQUIRED - there is no sensible
# default, and silently defaulting would let two containers share one identity.
WORKER_ID = os.environ["WORKER_ID"]

# The address the CONTROLLER must use to reach this worker. Inside Docker this is
# the compose service name (http://worker2:8000), not "localhost".
WORKER_URL = os.environ["WORKER_URL"]

# Where the controller lives (http://controller:8000 inside Docker).
CONTROLLER_URL = os.environ["CONTROLLER_URL"]

# Root directory for persistent data. In Docker this is a named volume.
DATA_DIR = os.environ.get("DATA_DIR", "/data")

# How often (seconds) the worker tells the controller "I am alive".
HEARTBEAT_INTERVAL = _float("HEARTBEAT_INTERVAL", 2.0)

# Largest chunk this worker will accept. Equals the project's 1 MiB chunk size.
# A sanity limit: it stops a buggy client from filling the disk with huge blobs.
MAX_CHUNK_BYTES = _int("MAX_CHUNK_BYTES", 1024 * 1024)
