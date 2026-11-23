"""
Spec section 7: worker failure. The local equivalent of
    docker stop worker2 ... docker start worker2
is killing and re-launching the worker process. Its "volume" (temp dir) survives.
"""
import os

import requests

from .conftest import sha256

MIB = 1024 * 1024


def _worker_state(cluster, name):
    return next(w for w in cluster.status()["workers"] if w["worker_id"] == name)


def test_worker_failure_detection_failover_repair_and_rejoin(cluster):
    data = os.urandom(5 * MIB)
    doc = cluster.upload(data).json()
    oid = doc["object_id"]
    victim = doc["chunks"][0]["locations"][0]["worker_id"]   # a worker that holds data

    # ---------------- BEFORE ----------------
    assert cluster.status()["replication"]["replication_ok"]

    # ---------------- DURING ----------------
    cluster.stop_worker(victim)
    cluster.wait_for(lambda: _worker_state(cluster, victim)["status"] == "dead", what="failure detected")

    # Object is still fully readable: another copy exists for every chunk.
    r = cluster.download(oid)
    assert r.status_code == 200 and sha256(r.content) == sha256(data)

    # The system NOTICES that replication is no longer maintained...
    cluster.wait_for(lambda: cluster.status()["replication"]["under_replicated"] > 0,
                     what="under-replication reported")
    # ...and (after the grace period) restores it on the remaining workers.
    cluster.wait_for(lambda: cluster.status()["replication"]["replication_ok"],
                     timeout=30, what="replication restored while worker is down")
    meta = cluster.metadata(oid)
    for c in meta["chunks"]:
        live = [l["worker_id"] for l in c["locations"] if l["worker_status"] == "alive"]
        assert len(live) == 2 and victim not in live

    # ---------------- AFTER ----------------
    cluster.start_worker(victim)
    cluster.wait_for(lambda: _worker_state(cluster, victim)["status"] == "alive", what="worker back")
    # Its old copies make some chunks temporarily over-replicated; they get trimmed.
    cluster.wait_for(lambda: cluster.status()["replication"]["replication_ok"],
                     timeout=30, what="exactly 2 copies of every chunk after rejoin")
    rep = cluster.status()["replication"]
    assert rep["over_replicated"] == 0 and rep["unavailable"] == 0
    r = cluster.download(oid)
    assert r.status_code == 200 and sha256(r.content) == sha256(data)

    kinds = [e["kind"] for e in cluster.events()]
    assert "worker_down" in kinds and "worker_up" in kinds and "repair" in kinds


def test_upload_still_works_with_one_worker_down(cluster):
    cluster.stop_worker("worker3")
    cluster.wait_for(lambda: _worker_state(cluster, "worker3")["status"] == "dead", what="detect")
    data = os.urandom(3 * MIB)
    doc = cluster.upload(data).json()
    assert all("worker3" not in [l["worker_id"] for l in c["locations"]] for c in doc["chunks"])
    assert cluster.download(doc["object_id"]).content == data
    cluster.start_worker("worker3")
    cluster.wait_for(lambda: _worker_state(cluster, "worker3")["status"] == "alive", what="back")
    cluster.wait_for(lambda: cluster.status()["replication"]["replication_ok"], timeout=30, what="healthy")


def test_all_copies_down_gives_503_and_recovers(cluster):
    data = os.urandom(1000)
    doc = cluster.upload(data).json()
    holders = [l["worker_id"] for l in doc["chunks"][0]["locations"]]
    for w in holders:
        cluster.stop_worker(w)
    cluster.wait_for(lambda: all(_worker_state(cluster, w)["status"] == "dead" for w in holders), what="both dead")

    r = cluster.download(doc["object_id"])
    assert r.status_code == 503 and r.json()["error"] == "storage_unavailable"
    # Metadata does not need the data, so it still works while chunks are unavailable.
    assert requests.get(cluster.api(f"/api/v1/objects/{doc['object_id']}/metadata")).status_code == 200
    # Only 2 workers remain -> uploads (needing 2 copies) still succeed.
    assert cluster.upload(b"still ok").status_code == 201

    for w in holders:
        cluster.start_worker(w)
    cluster.wait_for(lambda: cluster.status()["alive_workers"] == 4, what="all back")
    cluster.wait_for(lambda: cluster.status()["replication"]["replication_ok"], timeout=30, what="healthy")
    assert cluster.download(doc["object_id"]).content == data


def test_upload_rejected_with_503_when_fewer_than_two_workers_alive(cluster):
    for w in ("worker1", "worker2", "worker3"):
        cluster.stop_worker(w)
    cluster.wait_for(lambda: cluster.status()["alive_workers"] == 1, what="3 dead")
    r = cluster.upload(b"cannot store 2 copies")
    assert r.status_code == 503 and r.json()["error"] == "insufficient_workers"
    for w in ("worker1", "worker2", "worker3"):
        cluster.start_worker(w)
    cluster.wait_for(lambda: cluster.status()["alive_workers"] == 4, what="all back")
    cluster.wait_for(lambda: cluster.status()["replication"]["replication_ok"], timeout=30, what="healthy")
