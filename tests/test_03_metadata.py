"""Spec 3 / 5.3 / 5.4: system metadata, custom metadata, update without re-upload."""
import os
import time

import requests

from .conftest import sha256


def test_custom_metadata_roundtrip_and_system_fields(cluster):
    data = os.urandom(2000)
    meta = {"author": "alice", "year": 2026, "draft": True, "score": 1.5}
    doc = cluster.upload(data, name="m.bin", content_type="text/plain", metadata=meta).json()
    got = cluster.metadata(doc["object_id"])
    assert got["custom_metadata"] == meta
    # Every system field required by spec section 3:
    for field in ("object_id", "name", "size", "content_type", "created_at", "modified_at",
                  "checksum", "num_chunks", "chunks"):
        assert field in got
    assert got["checksum"] == sha256(data)
    assert got["created_at"] == got["modified_at"]
    assert got["chunks"][0]["locations"]                      # storage locations present


def test_patch_merges_updates_and_removes_without_reupload(cluster):
    data = os.urandom(3000)
    doc = cluster.upload(data, metadata={"a": "1", "b": "2"}).json()
    oid = doc["object_id"]
    time.sleep(0.05)
    r = requests.patch(cluster.api(f"/api/v1/objects/{oid}/metadata"),
                       json={"b": "two", "c": "3", "a": None})   # update b, add c, delete a
    assert r.status_code == 200
    new = r.json()
    assert new["custom_metadata"] == {"b": "two", "c": "3"}
    # Data and identity are untouched; only modified_at moved.
    assert new["checksum"] == doc["checksum"]
    assert new["chunks"] == doc["chunks"]
    assert new["created_at"] == doc["created_at"]
    assert new["modified_at"] > doc["modified_at"]
    assert cluster.download(oid).content == data
    assert cluster.metadata(oid)["custom_metadata"] == {"b": "two", "c": "3"}   # persisted


def test_patch_rejects_invalid_bodies(cluster):
    oid = cluster.upload(b"x").json()["object_id"]
    url = cluster.api(f"/api/v1/objects/{oid}/metadata")
    assert requests.patch(url, data="not json", headers={"Content-Type": "application/json"}).status_code == 400
    assert requests.patch(url, json=["list"]).status_code == 400
    assert requests.patch(url, json={"k": {"nested": 1}}).status_code == 400
    assert requests.patch(url, json={"k": "x" * 20000}).status_code == 400    # too large


def test_metadata_survives_object_listing_and_other_objects_unaffected(cluster):
    a = cluster.upload(b"a", metadata={"who": "a"}).json()["object_id"]
    b = cluster.upload(b"b", metadata={"who": "b"}).json()["object_id"]
    requests.patch(cluster.api(f"/api/v1/objects/{a}/metadata"), json={"who": "changed"})
    assert cluster.metadata(b)["custom_metadata"] == {"who": "b"}
