"""
Spec section 6: the worker's internal chunk API, tested directly (bypassing the
controller). Includes the security check that chunk ids can't escape the data dir.
"""
import hashlib

import requests

from .conftest import sha256


def _url(cluster, path):
    return f"http://127.0.0.1:{cluster.worker_ports['worker4']}{path}"


def test_put_get_delete_roundtrip(cluster):
    data = b"direct worker test" * 100
    cid = "testdirect_000001"
    r = requests.put(_url(cluster, f"/internal/chunks/{cid}"), data=data,
                     headers={"X-Chunk-SHA256": sha256(data)})
    assert r.status_code == 201 and r.json()["sha256"] == sha256(data)
    g = requests.get(_url(cluster, f"/internal/chunks/{cid}"))
    assert g.status_code == 200 and g.content == data and g.headers["X-Chunk-SHA256"] == sha256(data)
    assert requests.delete(_url(cluster, f"/internal/chunks/{cid}")).status_code == 204
    assert requests.delete(_url(cluster, f"/internal/chunks/{cid}")).status_code == 204   # idempotent
    assert requests.get(_url(cluster, f"/internal/chunks/{cid}")).status_code == 404


def test_write_with_wrong_checksum_is_rejected_and_not_stored(cluster):
    cid = "testdirect_000002"
    r = requests.put(_url(cluster, f"/internal/chunks/{cid}"), data=b"abc",
                     headers={"X-Chunk-SHA256": "0" * 64})
    assert r.status_code == 400 and r.json()["error"] == "checksum_mismatch"
    assert requests.get(_url(cluster, f"/internal/chunks/{cid}")).status_code == 404


def test_worker_refuses_to_serve_a_corrupted_chunk(cluster):
    cid = "testdirect_000003"
    requests.put(_url(cluster, f"/internal/chunks/{cid}"), data=b"good data " * 50)
    path = cluster.chunk_path("worker4", cid)
    raw = bytearray(path.read_bytes()); raw[3] ^= 0xFF; path.write_bytes(bytes(raw))
    g = requests.get(_url(cluster, f"/internal/chunks/{cid}"))
    assert g.status_code == 422 and g.json()["error"] == "chunk_corrupted"
    requests.delete(_url(cluster, f"/internal/chunks/{cid}"))


def test_unsafe_chunk_ids_are_rejected(cluster):
    r = requests.put(_url(cluster, "/internal/chunks/bad$id"), data=b"x")
    assert r.status_code == 400 and r.json()["error"] == "invalid_chunk_id"
    # A path-traversal attempt can't even match the route.
    r = requests.get(_url(cluster, "/internal/chunks/..%2F..%2Fetc%2Fpasswd"))
    assert r.status_code in (400, 404)


def test_oversized_chunk_rejected(cluster):
    r = requests.put(_url(cluster, "/internal/chunks/testdirect_big"), data=b"x" * (1024 * 1024 + 1))
    assert r.status_code == 413


def test_listing_reports_real_hashes_and_checks_worker_id(cluster):
    data = b"listing test"
    requests.put(_url(cluster, "/internal/chunks/testdirect_000004"), data=data)
    ok = requests.get(_url(cluster, "/internal/workers/worker4/chunks")).json()
    entry = next(c for c in ok["chunks"] if c["chunk_id"] == "testdirect_000004")
    assert entry["sha256"] == hashlib.sha256(data).hexdigest() and entry["size"] == len(data)
    assert requests.get(_url(cluster, "/internal/workers/worker9/chunks")).status_code == 404
    requests.delete(_url(cluster, "/internal/chunks/testdirect_000004"))


def test_controller_view_of_worker_chunks(cluster):
    doc = cluster.upload(b"x" * 5000).json()
    wid = doc["chunks"][0]["locations"][0]["worker_id"]
    view = requests.get(cluster.api(f"/internal/workers/{wid}/chunks")).json()
    assert doc["chunks"][0]["chunk_id"] in [c["chunk_id"] for c in view["chunks"]]
    assert requests.get(cluster.api("/internal/workers/nope/chunks")).status_code == 404


def test_controller_internal_register_and_heartbeat_validation(cluster):
    assert requests.post(cluster.api("/internal/workers/heartbeat"),
                         json={"worker_id": "ghost"}).status_code == 404
    assert requests.post(cluster.api("/internal/workers/register"),
                         json={"worker_id": "bad id!", "url": "http://x"}).status_code == 400
    assert requests.post(cluster.api("/internal/workers/register"), json={}).status_code == 400
