#!/usr/bin/env python3
"""
Automated demonstration of the Mini Distributed Cloud Object Store.

Runs the six demonstrations required by the project specification (section 10)
against the live Docker cluster and PRINTS evidence for each one:

    1. Object storage        upload + download, byte-for-byte comparison
    2. Chunking/distribution a >1 MiB object split into chunks spread over workers
    3. Replication           two copies per chunk on different workers (verified ON DISK)
    4. Metadata              upload custom metadata, read it, change it WITHOUT re-uploading
    5. Worker failure        `docker stop worker2` ... `docker start worker2`
    6. Data integrity        deliberately corrupt a stored chunk; detect, avoid, repair

Usage (cluster must be running: `docker compose up --build -d`):

    python demo/run_demo.py                # runs everything, no pauses
    python demo/run_demo.py --pause        # waits for ENTER between steps (good for presenting)
    python demo/run_demo.py --only 5 6     # run just some steps
    python demo/run_demo.py --url http://localhost:8000 --victim worker2

Exit code is 0 only if EVERY check passed.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time

import requests

MIB = 1024 * 1024


# ===========================================================================
# Operations that need the Docker CLI (isolated so they can be swapped in tests)
# ===========================================================================
class DockerOps:
    """The only place that shells out to `docker`."""

    @staticmethod
    def _run(cmd: list[str]) -> str:
        shown = " ".join(cmd)
        print(f"    $ {shown}")
        out = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return out.stdout.strip()

    def stop(self, worker: str) -> None:
        """Simulate a worker crash: stop its container (volume is kept)."""
        self._run(["docker", "stop", worker])

    def start(self, worker: str) -> None:
        """Bring the worker back; it re-attaches to the SAME persistent volume."""
        self._run(["docker", "start", worker])

    def chunk_sha256(self, worker: str, chunk_id: str) -> str:
        """Hash of the chunk file as it exists on that worker's disk."""
        out = subprocess.run(
            ["docker", "exec", worker, "sha256sum", f"/data/chunks/{chunk_id}.chunk"],
            check=True, capture_output=True, text=True,
        ).stdout
        return out.split()[0]

    def corrupt(self, worker: str, chunk_id: str) -> None:
        """
        Silent bit-rot: overwrite 7 bytes in the middle of the chunk file.
        `conv=notrunc` keeps the file size identical, so only a CHECKSUM can notice.
        """
        path = f"/data/chunks/{chunk_id}.chunk"
        self._run([
            "docker", "exec", worker, "sh", "-c",
            f"printf 'CORRUPT' | dd of={path} bs=1 seek=4096 conv=notrunc 2>/dev/null",
        ])


# ===========================================================================
# Output helpers
# ===========================================================================
class Demo:
    def __init__(self, base: str, ops, victim: str, pause: bool):
        self.base, self.ops, self.victim, self.pause = base, ops, victim, pause
        self.failures = 0
        self.created: list[str] = []

    # ------------------------------------------------------------- printing
    def banner(self, title: str) -> None:
        print("\n" + "=" * 78)
        print(f"  {title}")
        print("=" * 78)
        if self.pause:
            input("  [press ENTER to run this step] ")

    def info(self, msg: str) -> None:
        print(f"  {msg}")

    def check(self, condition: bool, msg: str) -> bool:
        print(f"  [{'PASS' if condition else 'FAIL'}] {msg}")
        if not condition:
            self.failures += 1
        return condition

    # ------------------------------------------------------------- API helpers
    def url(self, path: str) -> str:
        return self.base + path

    def cluster(self) -> dict:
        return requests.get(self.url("/api/v1/admin/cluster"), timeout=10).json()

    def events(self, limit=100) -> list[dict]:
        return requests.get(self.url(f"/api/v1/admin/events?limit={limit}"), timeout=10).json()["events"]

    def upload(self, data: bytes, name: str, ctype: str, metadata=None) -> dict:
        form = {"metadata": json.dumps(metadata)} if metadata is not None else {}
        r = requests.post(self.url("/api/v1/objects"), files={"file": (name, data, ctype)},
                          data=form, timeout=120)
        assert r.status_code == 201, r.text
        doc = r.json()
        self.created.append(doc["object_id"])
        return doc

    def download(self, object_id: str) -> requests.Response:
        return requests.get(self.url(f"/api/v1/objects/{object_id}"), timeout=120)

    def metadata(self, object_id: str) -> dict:
        return requests.get(self.url(f"/api/v1/objects/{object_id}/metadata"), timeout=10).json()

    def print_cluster(self, label: str) -> dict:
        c = self.cluster()
        print(f"\n  --- cluster state: {label} ---")
        print(f"  {'worker':<9} {'status':<6} {'chunks':>6}")
        for w in c["workers"]:
            note = f"  (down {w['seconds_dead']}s)" if w["status"] == "dead" else ""
            print(f"  {w['worker_id']:<9} {w['status'].upper():<6} {w['chunks_recorded']:>6}{note}")
        r = c["replication"]
        print(f"  chunks={r['total_chunks']}  healthy={r['healthy']}  under-replicated="
              f"{r['under_replicated']}  unavailable={r['unavailable']}  over-replicated={r['over_replicated']}")
        print(f"  replication factor {r['replication_factor']} maintained? "
              f"{'YES' if r['replication_ok'] else 'NO'}")
        return c

    def print_chunk_table(self, doc: dict) -> None:
        print(f"\n  {'#':<3} {'chunk_id':<42} {'bytes':>9}  stored on")
        for c in doc["chunks"]:
            where = ", ".join(
                l["worker_id"] + ("" if l["worker_status"] == "alive" else "(DOWN)") for l in c["locations"]
            )
            print(f"  {c['index']:<3} {c['chunk_id']:<42} {c['size']:>9}  {where}")

    def wait_until(self, predicate, what: str, timeout: float = 90.0, interval: float = 1.0) -> bool:
        """Poll `predicate` and report how long it took."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                if predicate():
                    print(f"  ... {what}: done after {time.time() - start:.1f}s")
                    return True
            except requests.RequestException:
                pass
            time.sleep(interval)
        print(f"  ... {what}: TIMED OUT after {timeout:.0f}s")
        return False

    # ===================================================================
    # Step 0: make sure the cluster is up
    # ===================================================================
    def step0_preflight(self) -> None:
        self.banner("STEP 0 - Cluster pre-flight")
        ok = self.wait_until(lambda: self.cluster()["alive_workers"] == 4, "4 workers registered", timeout=60)
        self.print_cluster("initial")
        if not self.check(ok, "controller reachable and all 4 workers alive"):
            sys.exit("cluster is not ready - run: docker compose up --build -d")

    # ===================================================================
    # Step 1: object storage
    # ===================================================================
    def step1_storage(self) -> None:
        self.banner("STEP 1 - Object storage: upload, retrieve, compare with the original")
        original = os.urandom(300_000)
        doc = self.upload(original, "demo-small.bin", "application/octet-stream")
        self.info(f"uploaded 'demo-small.bin' -> object_id {doc['object_id']}")
        r = self.download(doc["object_id"])
        self.info(f"original  sha256: {hashlib.sha256(original).hexdigest()}")
        self.info(f"retrieved sha256: {hashlib.sha256(r.content).hexdigest()}")
        self.check(r.status_code == 200 and r.content == original, "retrieved object is byte-identical to the original")

    # ===================================================================
    # Step 2: chunking + distribution   (keeps self.big for later steps)
    # ===================================================================
    def step2_chunking(self) -> None:
        self.banner("STEP 2 - Chunking and distribution (object larger than 1 MiB)")
        self.big_data = os.urandom(5 * MIB + 512 * 1024)  # 5.5 MiB
        self.big = self.upload(self.big_data, "demo-big.bin", "application/octet-stream")
        doc = self.big
        self.info(f"uploaded 5.5 MiB object -> {doc['object_id']}")
        self.print_chunk_table(doc)
        sizes = [c["size"] for c in doc["chunks"]]
        self.check(doc["num_chunks"] == 6, f"split into 6 chunks (got {doc['num_chunks']})")
        self.check(sizes[:5] == [MIB] * 5 and sizes[5] == 512 * 1024, "five full 1 MiB chunks + a smaller final chunk (0.5 MiB)")
        self.check([c["index"] for c in doc["chunks"]] == list(range(6)), "chunk order preserved (indexes 0..5)")
        self.check(len({c["chunk_id"] for c in doc["chunks"]}) == 6, "every chunk has a unique id")
        used = {l["worker_id"] for c in doc["chunks"] for l in c["locations"]}
        self.check(len(used) >= 3, f"chunks distributed across multiple workers: {sorted(used)}")
        self.print_cluster("after big upload")
        r = self.download(doc["object_id"])
        self.check(r.content == self.big_data, "multi-chunk object reassembled correctly on download")

    # ===================================================================
    # Step 3: replication
    # ===================================================================
    def step3_replication(self) -> None:
        self.banner("STEP 3 - Replication: two copies of every chunk, on different workers")
        doc = self.metadata(self.big["object_id"])
        self.print_chunk_table(doc)
        self.info("verifying each copy PHYSICALLY on the worker's disk (docker exec sha256sum):")
        all_ok = True
        for c in doc["chunks"]:
            workers = [l["worker_id"] for l in c["locations"]]
            all_ok &= len(workers) == 2 and len(set(workers)) == 2
            for w in workers:
                on_disk = self.ops.chunk_sha256(w, c["chunk_id"])
                match = on_disk == c["checksum"]
                all_ok &= match
                self.info(f"  chunk {c['index']} on {w}: sha256 {on_disk[:16]}... {'== expected' if match else 'MISMATCH'}")
        self.check(all_ok, "all 6 chunks have exactly 2 intact copies on 2 DIFFERENT workers")

    # ===================================================================
    # Step 4: metadata
    # ===================================================================
    def step4_metadata(self) -> None:
        self.banner("STEP 4 - Metadata: upload, retrieve and modify WITHOUT re-uploading")
        payload = os.urandom(50_000)
        doc = self.upload(payload, "report.pdf", "application/pdf",
                          metadata={"author": "alice", "project": "cloud-lab", "year": 2026})
        oid = doc["object_id"]
        self.info(f"uploaded 'report.pdf' with custom metadata -> {oid}")
        before = self.metadata(oid)
        self.info(f"GET metadata  : custom = {json.dumps(before['custom_metadata'])}")
        self.info(f"                system = size {before['size']}, type {before['content_type']}, "
                  f"chunks {before['num_chunks']}, checksum {before['checksum'][:16]}...")
        self.info(f"                created {before['created_at']}   modified {before['modified_at']}")
        self.check(before["custom_metadata"] == {"author": "alice", "project": "cloud-lab", "year": 2026},
                   "custom metadata stored and returned")

        time.sleep(0.2)
        patch = {"author": "bob", "reviewed": True, "project": None}   # change, add, delete
        self.info(f"PATCH metadata: {json.dumps(patch)}   (no file is sent)")
        r = requests.patch(self.url(f"/api/v1/objects/{oid}/metadata"), json=patch, timeout=10)
        after = self.metadata(oid)
        self.info(f"GET metadata  : custom = {json.dumps(after['custom_metadata'])}")
        self.info(f"                modified {after['modified_at']}")
        self.check(r.status_code == 200 and after["custom_metadata"] == {"author": "bob", "year": 2026, "reviewed": True},
                   "metadata updated (key changed, key added, key removed)")
        self.check(after["checksum"] == before["checksum"] and after["chunks"] == before["chunks"],
                   "object data/chunks untouched - nothing was re-uploaded")
        self.check(after["modified_at"] > before["modified_at"] and after["created_at"] == before["created_at"],
                   "modified_at advanced, created_at unchanged")
        self.check(self.download(oid).content == payload, "object still downloads identically")

    # ===================================================================
    # Step 5: worker failure
    # ===================================================================
    def step5_failure(self) -> None:
        v = self.victim
        self.banner(f"STEP 5 - Worker failure: docker stop {v} ... docker start {v}")
        oid = self.big["object_id"]

        print("\n  ##### BEFORE the failure #####")
        before = self.print_cluster("before")
        self.check(before["replication"]["replication_ok"], "replication factor is maintained before the failure")

        print(f"\n  ##### DURING the failure #####")
        self.ops.stop(v)
        self.wait_until(lambda: next(w for w in self.cluster()["workers"] if w["worker_id"] == v)["status"] == "dead",
                        f"controller detects {v} is down (missed heartbeats)", timeout=40)
        c = self.print_cluster("worker down, repair not started yet")
        under = c["replication"]["under_replicated"]
        self.check(under > 0, f"controller DETECTED that replication is no longer maintained ({under} chunk(s) under-replicated)")

        r = self.download(oid)
        self.check(r.status_code == 200 and r.content == self.big_data,
                   f"object is STILL downloadable and identical (served from the surviving copies)")

        self.info("waiting for automatic re-replication onto the surviving workers "
                  "(starts after the repair grace period)...")
        ok = self.wait_until(lambda: self.cluster()["replication"]["replication_ok"],
                             "replication factor restored WITHOUT worker2", timeout=120)
        c = self.print_cluster(f"worker still down, after automatic repair")
        self.check(ok, f"replication factor restored to 2 copies per chunk using the 3 remaining workers")
        self.print_chunk_table(self.metadata(oid))

        print(f"\n  ##### AFTER restarting {v} #####")
        self.ops.start(v)
        self.wait_until(lambda: next(w for w in self.cluster()["workers"] if w["worker_id"] == v)["status"] == "alive",
                        f"{v} re-registers (its persistent volume kept its chunks)", timeout=60)
        self.info("the returned worker still holds OLD copies; some chunks now have 3 copies -> surplus is trimmed")
        ok = self.wait_until(lambda: self.cluster()["replication"]["replication_ok"],
                             "every chunk back to exactly 2 copies", timeout=60)
        c = self.print_cluster("after worker restart")
        rep = c["replication"]
        self.check(ok and rep["over_replicated"] == 0 and rep["under_replicated"] == 0 and rep["unavailable"] == 0,
                   "replication factor is maintained after the restart (exactly 2 copies of every chunk)")
        self.check(self.download(oid).content == self.big_data, "object downloads identically after recovery")

        print("\n  recent controller events:")
        for e in self.events(12):
            print(f"    {e['time']}  {e['kind']:<18} {e['message']}")

    # ===================================================================
    # Step 6: integrity
    # ===================================================================
    def step6_integrity(self) -> None:
        self.banner("STEP 6 - Data integrity: intentional corruption of stored data")
        doc = self.metadata(self.big["object_id"])
        chunk = doc["chunks"][0]
        cid, expected = chunk["chunk_id"], chunk["checksum"]
        bad_worker = chunk["locations"][0]["worker_id"]
        good_worker = chunk["locations"][1]["worker_id"]
        self.info(f"target: chunk 0 ({cid}) held by {bad_worker} and {good_worker}")
        self.info(f"recorded checksum        : {expected}")
        self.info(f"{bad_worker} file hash BEFORE : {self.ops.chunk_sha256(bad_worker, cid)}")
        self.ops.corrupt(bad_worker, cid)
        corrupted = self.ops.chunk_sha256(bad_worker, cid)
        self.info(f"{bad_worker} file hash AFTER  : {corrupted}   <-- silently corrupted (same file size)")
        self.check(corrupted != expected, f"chunk file on {bad_worker} is now corrupted")

        self.info("\n  Downloading repeatedly: the controller verifies every chunk's checksum and "
                  "must never return bad bytes.")
        detected = False
        for attempt in range(1, 16):
            r = self.download(self.big["object_id"])
            good = r.status_code == 200 and r.content == self.big_data
            self.check(good, f"download #{attempt} is byte-identical to the original")
            if any(e["kind"] == "corruption_detected" and cid in e["message"] for e in self.events(50)):
                detected = True
                self.info("    -> the controller detected the corrupt copy DURING A READ and used the good copy")
                break
        if not detected:
            self.info("    (reads happened to pick the good copy each time; the background scrub finds it instead)")

        self.info("\n  Triggering the integrity scrub (also runs automatically every 60 s):")
        scrub = requests.post(self.url("/api/v1/admin/scrub"), timeout=60).json()
        self.info(f"    scrub result for {bad_worker}: {scrub['scanned'].get(bad_worker)}")
        evs = [e for e in self.events(100) if e["kind"] == "corruption_detected" and cid in e["message"]]
        self.check(bool(evs), "corruption of the chunk was DETECTED and logged")
        for e in evs[-2:]:
            self.info(f"    event: {e['kind']}: {e['message']}")

        ok = self.wait_until(lambda: self.cluster()["replication"]["replication_ok"], "corrupt copy replaced", timeout=60)
        doc = self.metadata(self.big["object_id"])
        holders = [l["worker_id"] for l in doc["chunks"][0]["locations"]]
        self.print_chunk_table(doc)
        intact = all(self.ops.chunk_sha256(w, cid) == expected for w in holders)
        self.check(ok and len(holders) == 2 and intact, f"chunk 0 restored: 2 copies on {holders}, both match the checksum")
        self.check(self.download(self.big["object_id"]).content == self.big_data, "final download is byte-identical")

    # ===================================================================
    def cleanup(self) -> None:
        for oid in self.created:
            requests.delete(self.url(f"/api/v1/objects/{oid}"), timeout=30)
        print(f"\n  deleted {len(self.created)} demo object(s)")


# ===========================================================================
def main(argv=None, ops=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default=os.environ.get("OBJSTORE_URL", "http://localhost:8000"))
    p.add_argument("--victim", default="worker2", help="worker to stop/start in step 5 (default: worker2)")
    p.add_argument("--pause", action="store_true", help="wait for ENTER before each step")
    p.add_argument("--only", nargs="+", type=int, metavar="N", help="run only these steps (1-6)")
    p.add_argument("--cleanup", action="store_true", help="delete the demo objects at the end")
    args = p.parse_args(argv)

    demo = Demo(args.url.rstrip("/"), ops or DockerOps(), args.victim, args.pause)
    steps = {1: demo.step1_storage, 2: demo.step2_chunking, 3: demo.step3_replication,
             4: demo.step4_metadata, 5: demo.step5_failure, 6: demo.step6_integrity}
    wanted = args.only or list(steps)
    # Steps 3, 5 and 6 reuse the big object made by step 2, so make sure it exists.
    if any(n in wanted for n in (3, 5, 6)) and 2 not in wanted:
        wanted = [2] + wanted
    try:
        demo.step0_preflight()
        for n in sorted(set(wanted)):
            steps[n]()
    except requests.ConnectionError:
        sys.exit(f"cannot reach the controller at {demo.base} - is the cluster running?")
    finally:
        if args.cleanup:
            demo.cleanup()
    print("\n" + "=" * 78)
    print("  DEMO RESULT:", "ALL CHECKS PASSED" if demo.failures == 0 else f"{demo.failures} CHECK(S) FAILED")
    print("=" * 78)
    return 0 if demo.failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
