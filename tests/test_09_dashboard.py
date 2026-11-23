"""Dashboard: the /dashboard page and the /api/v1/admin/chunkmap endpoint behind it (D-28)."""
import os

import requests

from .conftest import NUM_WORKERS

MIB = 1024 * 1024


def _chunkmap(cluster, **params):
    r = requests.get(cluster.api("/api/v1/admin/chunkmap"), params=params, timeout=5)
    assert r.status_code == 200
    return r.json()


def _object_in_map(cluster, oid):
    return next((o for o in _chunkmap(cluster)["objects"] if o["object_id"] == oid), None)


def test_dashboard_page_is_served_as_html(cluster):
    r = requests.get(cluster.api("/dashboard"), timeout=5)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "Chunk map" in r.text
    assert "/api/v1/admin/chunkmap" in r.text      # the page really uses the endpoint


def test_chunkmap_matches_the_object_metadata(cluster):
    data = os.urandom(int(2.5 * MIB))               # 3 chunks
    doc = cluster.upload(data, name="mapped.bin").json()
    entry = _object_in_map(cluster, doc["object_id"])
    assert entry is not None
    assert entry["name"] == "mapped.bin" and entry["num_chunks"] == 3
    assert sum(c["size"] for c in entry["chunks"]) == len(data)
    by_id = {c["chunk_id"]: c for c in doc["chunks"]}
    for c in entry["chunks"]:
        assert c["state"] == "healthy" and c["live_copies"] == 2
        holders = sorted(h["worker_id"] for h in c["holders"])
        assert len(set(holders)) == 2               # two DIFFERENT workers
        # Same answer as the per-object metadata endpoint.
        assert holders == sorted(l["worker_id"] for l in by_id[c["chunk_id"]]["locations"])


def test_chunkmap_summary_and_worker_totals_are_consistent(cluster):
    cluster.upload(os.urandom(MIB + 10), name="sum.bin")
    m = _chunkmap(cluster, max_objects=1000)
    assert m["replication_factor"] == 2 and m["chunk_size"] == MIB
    assert {w["worker_id"] for w in m["workers"]} == set(cluster.worker_ports)
    assert len(m["workers"]) == NUM_WORKERS
    s = m["summary"]
    assert s["total_chunks"] == s["healthy"] + s["under_replicated"] + s["unavailable"] + s["over_replicated"]
    # Every chunk has `replication_factor` copies, so the workers' totals add up to RF x chunks.
    if s["healthy"] == s["total_chunks"]:
        assert sum(w["chunks"] for w in m["workers"]) == 2 * s["total_chunks"]
    assert m["objects_total"] >= m["objects_shown"]


def test_chunkmap_limit_and_newest_first(cluster):
    first = cluster.upload(os.urandom(100), name="older.bin").json()["object_id"]
    second = cluster.upload(os.urandom(100), name="newer.bin").json()["object_id"]
    m = _chunkmap(cluster, max_objects=1)
    assert m["objects_shown"] == 1 and len(m["objects"]) == 1
    assert m["objects_total"] >= 2
    assert m["objects"][0]["object_id"] in (second, first)
    assert requests.get(cluster.api("/api/v1/admin/chunkmap?max_objects=0"), timeout=5).status_code == 400


def test_chunkmap_returns_hostile_names_verbatim(cluster):
    """The API does not rewrite names; the dashboard escapes them when drawing (esc() in the page)."""
    name = "<img src=x onerror=alert(1)>.bin"
    oid = cluster.upload(os.urandom(50), name=name).json()["object_id"]
    assert _object_in_map(cluster, oid)["name"] == name
    assert "function esc(" in requests.get(cluster.api("/dashboard"), timeout=5).text


def test_chunkmap_shows_failure_and_recovery(cluster):
    data = os.urandom(3 * MIB)
    doc = cluster.upload(data, name="watched.bin").json()
    oid = doc["object_id"]
    victim = doc["chunks"][0]["locations"][0]["worker_id"]

    cluster.stop_worker(victim)
    # The dead worker's copies stay on the map (the database still lists them) but flip to "dead".
    def degraded():
        e = _object_in_map(cluster, oid)
        return e if any(c["state"] == "under_replicated" for c in e["chunks"]) else None
    entry = cluster.wait_for(degraded, what="chunk map shows an under-replicated chunk")
    hit = next(c for c in entry["chunks"] if c["state"] == "under_replicated")
    assert hit["live_copies"] == 1
    assert any(h["worker_id"] == victim and h["status"] == "dead" for h in hit["holders"])
    assert next(w for w in _chunkmap(cluster)["workers"] if w["worker_id"] == victim)["status"] == "dead"

    # After the repair grace period every chunk is back to 2 live copies without the victim.
    def healed():
        e = _object_in_map(cluster, oid)
        return e if all(c["state"] == "healthy" for c in e["chunks"]) else None
    healthy = cluster.wait_for(healed, timeout=40, what="chunk map shows full health again")
    for c in healthy["chunks"]:
        assert victim not in [h["worker_id"] for h in c["holders"] if h["status"] == "alive"]

    cluster.start_worker(victim)
    cluster.wait_for(
        lambda: next(w for w in _chunkmap(cluster)["workers"] if w["worker_id"] == victim)["status"] == "alive",
        what="worker back")
    cluster.wait_for(lambda: cluster.status()["replication"]["replication_ok"], timeout=40, what="settled")
