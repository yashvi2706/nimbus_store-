"""The controller's metadata must survive a controller crash/restart (SQLite on disk)."""
import os

from .conftest import sha256


def test_metadata_and_data_survive_controller_restart(cluster):
    data = os.urandom(2_500_000)
    doc = cluster.upload(data, name="persist.bin", metadata={"k": "v"}).json()

    cluster.restart_controller()
    cluster.wait_for(lambda: cluster.status()["alive_workers"] == 4, what="workers re-attached")

    meta = cluster.metadata(doc["object_id"])
    assert meta["custom_metadata"] == {"k": "v"} and meta["checksum"] == doc["checksum"]
    assert meta["chunks"] == [
        {**c, "locations": [{**l, "worker_status": "alive"} for l in c["locations"]]} for c in doc["chunks"]
    ]
    r = cluster.download(doc["object_id"])
    assert sha256(r.content) == sha256(data)
    assert cluster.status()["replication"]["replication_ok"]
