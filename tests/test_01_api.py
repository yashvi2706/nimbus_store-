"""
Tests for the client REST API contract (spec section 5): status codes, error
bodies, content types, listing/pagination, deletion.
"""
import os

import requests

from .conftest import sha256


def test_upload_returns_201_with_full_description(cluster):
    data = os.urandom(10_000)
    r = cluster.upload(data, name="hello.bin", content_type="application/x-custom")
    assert r.status_code == 201
    doc = r.json()
    assert doc["name"] == "hello.bin"
    assert doc["size"] == 10_000
    assert doc["content_type"] == "application/x-custom"
    assert doc["checksum"] == sha256(data)
    assert doc["num_chunks"] == 1
    assert len(doc["object_id"]) == 32
    assert r.headers["Location"].endswith(doc["object_id"])


def test_roundtrip_is_byte_identical_and_keeps_content_type(cluster):
    data = os.urandom(300_000)
    doc = cluster.upload(data, name="pic.png", content_type="image/png").json()
    r = cluster.download(doc["object_id"])
    assert r.status_code == 200
    assert r.content == data                       # byte-for-byte identical
    assert r.headers["Content-Type"] == "image/png"
    assert r.headers["X-Object-SHA256"] == sha256(data)
    assert "pic.png" in r.headers["Content-Disposition"]


def test_empty_object_is_supported(cluster):
    doc = cluster.upload(b"", name="empty.txt").json()
    assert doc["size"] == 0 and doc["num_chunks"] == 0
    r = cluster.download(doc["object_id"])
    assert r.status_code == 200 and r.content == b""


def test_object_ids_are_unique(cluster):
    ids = {cluster.upload(b"same bytes").json()["object_id"] for _ in range(5)}
    assert len(ids) == 5


def test_missing_objects_return_404_json(cluster):
    base = "/api/v1/objects/does-not-exist"
    for method, path in [("GET", base), ("GET", base + "/metadata"),
                         ("PATCH", base + "/metadata"), ("DELETE", base)]:
        r = requests.request(method, cluster.api(path), json={"a": "b"} if method == "PATCH" else None)
        assert r.status_code == 404, (method, path)
        body = r.json()
        assert body["error"] == "object_not_found" and "does-not-exist" in body["message"]


def test_upload_without_file_is_400(cluster):
    r = requests.post(cluster.api("/api/v1/objects"), data={"metadata": "{}"})
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_request"


def test_invalid_custom_metadata_is_400(cluster):
    assert cluster.upload(b"x", metadata=None).status_code == 201
    bad_json = requests.post(cluster.api("/api/v1/objects"),
                             files={"file": ("a", b"x")}, data={"metadata": "{not json"})
    assert bad_json.status_code == 400
    nested = cluster.upload(b"x", metadata={"k": {"nested": 1}})
    assert nested.status_code == 400
    not_object = cluster.upload(b"x", metadata=["a", "b"])
    assert not_object.status_code == 400


def test_failed_upload_leaves_no_visible_object(cluster):
    before = requests.get(cluster.api("/api/v1/objects")).json()["total"]
    cluster.upload(b"x", metadata={"k": {"nested": 1}})  # rejected
    after = requests.get(cluster.api("/api/v1/objects")).json()["total"]
    assert before == after


def test_list_objects_and_pagination(cluster):
    for i in range(3):
        cluster.upload(b"list-me", name=f"list{i}.txt", content_type="text/plain")
    page = requests.get(cluster.api("/api/v1/objects?limit=2&offset=0")).json()
    assert len(page["objects"]) == 2 and page["total"] >= 3 and page["limit"] == 2
    entry = page["objects"][0]
    for field in ("object_id", "name", "size", "content_type"):   # minimum fields (5.6)
        assert field in entry
    page2 = requests.get(cluster.api("/api/v1/objects?limit=2&offset=2")).json()
    assert {o["object_id"] for o in page["objects"]}.isdisjoint(o["object_id"] for o in page2["objects"])
    assert requests.get(cluster.api("/api/v1/objects?limit=0")).status_code == 400


def test_delete_returns_204_then_404_and_frees_worker_disks(cluster):
    doc = cluster.upload(os.urandom(1_500_000)).json()
    paths = [cluster.chunk_path(loc["worker_id"], c["chunk_id"])
             for c in doc["chunks"] for loc in c["locations"]]
    assert paths and all(p.exists() for p in paths)
    r = requests.delete(cluster.api(f"/api/v1/objects/{doc['object_id']}"))
    assert r.status_code == 204 and r.content == b""
    assert cluster.download(doc["object_id"]).status_code == 404
    assert not any(p.exists() for p in paths)       # chunk files really removed


def test_unknown_route_returns_json_404(cluster):
    r = requests.get(cluster.api("/api/v1/nope"))
    assert r.status_code == 404 and r.json()["error"] == "not_found"
