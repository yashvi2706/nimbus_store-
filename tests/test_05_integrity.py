"""
Spec section 8: detect corruption, never return corrupted data when a valid copy
exists, and restore the damaged copy.
"""
import os

import pytest
import requests

from .conftest import sha256

MIB = 1024 * 1024


def corrupt_file(path):
    """Flip one byte IN PLACE (same size) - silent bit-rot, undetectable by length."""
    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    path.write_bytes(bytes(raw))


def _healthy(cluster):
    return cluster.status()["replication"]["replication_ok"]


def test_corrupt_copy_is_never_served_and_is_repaired(cluster):
    data = os.urandom(600_000)
    doc = cluster.upload(data).json()
    chunk = doc["chunks"][0]
    bad_worker = chunk["locations"][0]["worker_id"]
    bad_path = cluster.chunk_path(bad_worker, chunk["chunk_id"])

    corrupt_file(bad_path)
    assert sha256(bad_path.read_bytes()) != chunk["checksum"]      # really corrupted

    # Read path: replicas are tried in random order, so over many downloads we
    # will hit the bad copy. EVERY response must still be correct.
    for _ in range(12):
        r = cluster.download(doc["object_id"])
        assert r.status_code == 200 and sha256(r.content) == sha256(data)

    # Scrub path: deterministic detection even if reads never touched the bad copy.
    requests.post(cluster.api("/api/v1/admin/scrub"), timeout=30)
    assert "corruption_detected" in [e["kind"] for e in cluster.events()]

    # Restoration: two healthy copies again, and BOTH files now match the checksum.
    cluster.wait_for(lambda: _healthy(cluster), what="replication restored after corruption")
    meta = cluster.metadata(doc["object_id"])["chunks"][0]
    assert len(meta["locations"]) == 2
    for loc in meta["locations"]:
        assert sha256(cluster.chunk_path(loc["worker_id"], chunk["chunk_id"]).read_bytes()) == chunk["checksum"]


def test_missing_chunk_file_is_detected_and_restored(cluster):
    doc = cluster.upload(os.urandom(100_000)).json()
    chunk = doc["chunks"][0]
    victim = chunk["locations"][0]["worker_id"]
    cluster.chunk_path(victim, chunk["chunk_id"]).unlink()         # simulate lost file
    requests.post(cluster.api("/api/v1/admin/scrub"), timeout=30)
    cluster.wait_for(lambda: _healthy(cluster), what="restored after missing chunk")
    assert "replica_missing" in [e["kind"] for e in cluster.events()]
    assert cluster.download(doc["object_id"]).status_code == 200


def test_corruption_of_every_copy_never_returns_bad_data(cluster):
    data = os.urandom(200_000)
    doc = cluster.upload(data).json()
    chunk = doc["chunks"][0]
    for loc in chunk["locations"]:
        corrupt_file(cluster.chunk_path(loc["worker_id"], chunk["chunk_id"]))
    # No valid copy exists. The ONLY unacceptable outcome is "200 + wrong bytes".
    # Our design aborts the stream, so the client sees a truncated body
    # (requests raises ChunkedEncodingError because of the Content-Length mismatch).
    try:
        r = cluster.download(doc["object_id"])
    except requests.exceptions.RequestException:
        pass  # truncated stream -> detected by the client: correct behaviour
    else:
        assert r.status_code != 200, "a corrupted object must never be served as success"
    kinds = [e["kind"] for e in cluster.events()]
    assert "read_failed" in kinds
    requests.delete(cluster.api(f"/api/v1/objects/{doc['object_id']}"))


def test_orphan_chunks_are_swept(cluster):
    """A chunk file the controller has never heard of is garbage and gets removed."""
    import time
    stray = cluster.chunk_path("worker1", "ffffffffffffffffffffffffffffffff_000000")
    stray.write_bytes(b"leftover from a deleted object")
    time.sleep(1.5)                           # test cluster uses a 1s orphan grace period
    requests.post(cluster.api("/api/v1/admin/scrub"), timeout=30)
    assert not stray.exists()
