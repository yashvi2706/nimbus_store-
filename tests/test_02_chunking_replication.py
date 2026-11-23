"""
Spec section 4: 1 MiB chunks, ordering, unique ids, two copies on DIFFERENT
workers, spread across workers.
"""
import os

from .conftest import sha256

MIB = 1024 * 1024


def test_object_is_split_into_1mib_chunks_with_smaller_last_chunk(cluster):
    size = 2 * MIB + 300_000                       # 2 full chunks + remainder
    data = os.urandom(size)
    doc = cluster.upload(data).json()
    assert doc["num_chunks"] == 3
    sizes = [c["size"] for c in doc["chunks"]]
    assert sizes == [MIB, MIB, 300_000]
    assert [c["index"] for c in doc["chunks"]] == [0, 1, 2]       # order preserved
    ids = [c["chunk_id"] for c in doc["chunks"]]
    assert len(set(ids)) == 3                                      # unique ids
    assert doc["chunk_size"] == MIB


def test_exact_multiple_of_chunk_size_has_no_empty_trailing_chunk(cluster):
    doc = cluster.upload(os.urandom(2 * MIB)).json()
    assert [c["size"] for c in doc["chunks"]] == [MIB, MIB]


def test_chunk_checksums_match_the_data(cluster):
    data = os.urandom(MIB + 5)
    doc = cluster.upload(data).json()
    assert doc["chunks"][0]["checksum"] == sha256(data[:MIB])
    assert doc["chunks"][1]["checksum"] == sha256(data[MIB:])


def test_every_chunk_has_two_copies_on_different_workers_with_files_on_disk(cluster):
    doc = cluster.upload(os.urandom(5 * MIB)).json()
    for c in doc["chunks"]:
        workers = [loc["worker_id"] for loc in c["locations"]]
        assert len(workers) == 2 and len(set(workers)) == 2        # 2 DISTINCT workers
        for w in workers:
            assert cluster.chunk_path(w, c["chunk_id"]).exists()   # physically stored
            assert cluster.chunk_path(w, c["chunk_id"]).stat().st_size == c["size"]


def test_chunks_are_spread_over_all_workers(cluster):
    doc = cluster.upload(os.urandom(8 * MIB)).json()               # 8 chunks, 16 copies
    used = {loc["worker_id"] for c in doc["chunks"] for loc in c["locations"]}
    assert len(used) == 4
    counts = {}
    for c in doc["chunks"]:
        for loc in c["locations"]:
            counts[loc["worker_id"]] = counts.get(loc["worker_id"], 0) + 1
    assert max(counts.values()) - min(counts.values()) <= 2        # reasonably balanced


def test_multi_chunk_roundtrip_reassembles_in_order(cluster):
    data = os.urandom(4 * MIB + 123)
    doc = cluster.upload(data).json()
    assert cluster.download(doc["object_id"]).content == data


def test_cluster_reports_full_replication(cluster):
    rep = cluster.status()["replication"]
    assert rep["replication_ok"] and rep["under_replicated"] == 0
