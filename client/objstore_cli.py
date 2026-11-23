#!/usr/bin/env python3
"""
Command-line client for the Mini Distributed Cloud Object Store.

It talks ONLY to the controller's public REST API - it never needs to know where
the chunks live (spec 5.2). Run any number of copies of it, from any number of
terminals/machines, to act as "1 or more clients".

Examples (after `source .venv/bin/activate`):

    python client/objstore_cli.py upload photo.jpg --meta author=alice --meta year=2026
    python client/objstore_cli.py list
    python client/objstore_cli.py info   <object_id>          # metadata + chunk locations
    python client/objstore_cli.py download <object_id> -o copy.jpg
    python client/objstore_cli.py meta-set <object_id> reviewed=true author=bob
    python client/objstore_cli.py meta-set <object_id> author=null   # null deletes a key
    python client/objstore_cli.py delete <object_id>

    python client/objstore_cli.py cluster                      # workers + replication health
    python client/objstore_cli.py events                       # failures / repairs / corruption log

Server address: --url http://host:8000   or   env OBJSTORE_URL   (default http://localhost:8000)
"""
import argparse
import hashlib
import json
import mimetypes
import os
import sys

import requests


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def human(n: int) -> str:
    """12345678 -> '11.8 MiB'."""
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return str(n)


def fail(resp: requests.Response) -> None:
    """Print the server's JSON error message and exit non-zero."""
    try:
        body = resp.json()
        msg = f"{body.get('error')}: {body.get('message')}"
    except ValueError:
        msg = resp.text[:300]
    sys.exit(f"ERROR {resp.status_code}  {msg}")


def check(resp: requests.Response, expected: int) -> requests.Response:
    if resp.status_code != expected:
        fail(resp)
    return resp


def parse_pairs(pairs: list[str]) -> dict:
    """
    ['author=alice', 'year=2026', 'draft=true', 'x=null'] -> typed dict.
    Values that look like JSON (numbers, true/false/null) become that type;
    everything else stays a string.
    """
    out = {}
    for pair in pairs or []:
        if "=" not in pair:
            sys.exit(f"bad key=value pair: {pair!r}")
        key, raw = pair.split("=", 1)
        try:
            out[key] = json.loads(raw)
        except ValueError:
            out[key] = raw
    return out


def print_object(doc: dict, show_chunks: bool = True) -> None:
    """Human-friendly view of an object description."""
    print(f"Object ID    : {doc['object_id']}")
    print(f"Name         : {doc['name']}")
    print(f"Size         : {doc['size']} bytes ({human(doc['size'])})")
    print(f"Content-Type : {doc['content_type']}")
    print(f"Created      : {doc['created_at']}")
    print(f"Modified     : {doc['modified_at']}")
    print(f"Checksum     : sha256:{doc['checksum']}")
    print(f"Chunks       : {doc['num_chunks']} x up to {human(doc['chunk_size'])}, "
          f"replication factor {doc['replication_factor']}")
    print(f"Custom meta  : {json.dumps(doc['custom_metadata'])}")
    if show_chunks and doc["chunks"]:
        print("\n  #  chunk_id                                  size        copies on")
        for c in doc["chunks"]:
            where = ", ".join(
                f"{l['worker_id']}{'' if l['worker_status'] == 'alive' else ' (DOWN)'}"
                for l in c["locations"]
            )
            print(f"  {c['index']:<2} {c['chunk_id']}  {c['size']:>10}  {where}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_upload(args, base):
    path = args.file
    if not os.path.isfile(path):
        sys.exit(f"no such file: {path}")
    ctype = args.content_type or mimetypes.guess_type(path)[0] or "application/octet-stream"
    data = {"metadata": json.dumps(parse_pairs(args.meta))} if args.meta else {}
    with open(path, "rb") as f:
        # Passing the open file lets `requests` stream it instead of loading it all.
        resp = requests.post(
            f"{base}/api/v1/objects",
            files={"file": (os.path.basename(path), f, ctype)},
            data=data,
        )
    check(resp, 201)
    print("Uploaded.\n")
    print_object(resp.json())


def cmd_download(args, base):
    resp = requests.get(f"{base}/api/v1/objects/{args.object_id}", stream=True)
    check(resp, 200)
    out = args.output or resp.headers.get("X-Object-Id", args.object_id)
    if not args.output:  # derive a file name from Content-Disposition if possible
        cd = resp.headers.get("Content-Disposition", "")
        if 'filename="' in cd:
            out = cd.split('filename="', 1)[1].split('"', 1)[0]
    digest = hashlib.sha256()
    written = 0
    with open(out, "wb") as f:
        for block in resp.iter_content(chunk_size=1024 * 1024):
            f.write(block)
            digest.update(block)
            written += len(block)
    expected = resp.headers.get("X-Object-SHA256")
    ok = expected == digest.hexdigest()
    print(f"Saved {written} bytes to {out}")
    print(f"Checksum  expected sha256:{expected}")
    print(f"Checksum  computed sha256:{digest.hexdigest()}  -> {'MATCH' if ok else 'MISMATCH!'}")
    sys.exit(0 if ok else 2)


def cmd_info(args, base):
    doc = check(requests.get(f"{base}/api/v1/objects/{args.object_id}/metadata"), 200).json()
    if args.json:
        print(json.dumps(doc, indent=2))
    else:
        print_object(doc)


def cmd_meta_set(args, base):
    resp = requests.patch(
        f"{base}/api/v1/objects/{args.object_id}/metadata", json=parse_pairs(args.pairs)
    )
    check(resp, 200)
    doc = resp.json()
    print("Metadata updated (object data was NOT re-uploaded).")
    print(f"Custom meta : {json.dumps(doc['custom_metadata'])}")
    print(f"Modified    : {doc['modified_at']}")


def cmd_list(args, base):
    body = check(
        requests.get(f"{base}/api/v1/objects", params={"limit": args.limit, "offset": args.offset}), 200
    ).json()
    print(f"{body['total']} object(s) stored; showing {len(body['objects'])}\n")
    print(f"{'OBJECT ID':<34} {'SIZE':>10}  {'CONTENT-TYPE':<26} NAME")
    for o in body["objects"]:
        print(f"{o['object_id']:<34} {human(o['size']):>10}  {o['content_type']:<26} {o['name']}")


def cmd_delete(args, base):
    check(requests.delete(f"{base}/api/v1/objects/{args.object_id}"), 204)
    print(f"Deleted {args.object_id}")


def cmd_cluster(args, base):
    body = check(requests.get(f"{base}/api/v1/admin/cluster"), 200).json()
    if args.json:
        print(json.dumps(body, indent=2))
        return
    print(f"{'WORKER':<10} {'STATUS':<7} {'CHUNKS':>7} {'DATA':>10}  LAST HEARTBEAT")
    for w in body["workers"]:
        extra = "" if w["status"] == "alive" else f"  (down for {w['seconds_dead']}s)"
        print(f"{w['worker_id']:<10} {w['status'].upper():<7} {w['chunks_recorded']:>7} "
              f"{human(w['bytes_recorded']):>10}  {w['seconds_since_heartbeat']}s ago{extra}")
    r = body["replication"]
    print(f"\nReplication (factor {r['replication_factor']}), {r['total_chunks']} chunks: "
          f"healthy={r['healthy']}  under-replicated={r['under_replicated']}  "
          f"unavailable={r['unavailable']}  over-replicated={r['over_replicated']}")
    print("=> replication factor is MAINTAINED" if r["replication_ok"]
          else "=> replication factor is NOT currently maintained")


def cmd_events(args, base):
    body = check(requests.get(f"{base}/api/v1/admin/events", params={"limit": args.limit}), 200).json()
    for e in body["events"]:
        print(f"{e['time']}  {e['kind']:<20} {e['message']}")


def cmd_scrub(args, base):
    print(json.dumps(check(requests.post(f"{base}/api/v1/admin/scrub"), 200).json(), indent=2))


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Client for the Mini Distributed Cloud Object Store")
    p.add_argument("--url", default=os.environ.get("OBJSTORE_URL", "http://localhost:8000"),
                   help="controller address (default: $OBJSTORE_URL or http://localhost:8000)")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("upload", help="upload a file")
    s.add_argument("file")
    s.add_argument("--meta", action="append", metavar="KEY=VALUE", help="custom metadata (repeatable)")
    s.add_argument("--content-type", help="override the guessed content type")
    s.set_defaults(fn=cmd_upload)

    s = sub.add_parser("download", help="download an object and verify its checksum")
    s.add_argument("object_id")
    s.add_argument("-o", "--output", help="output file (default: the original name)")
    s.set_defaults(fn=cmd_download)

    s = sub.add_parser("info", help="show metadata and chunk locations")
    s.add_argument("object_id")
    s.add_argument("--json", action="store_true", help="raw JSON")
    s.set_defaults(fn=cmd_info)

    s = sub.add_parser("meta-set", help="add/update custom metadata (KEY=null deletes a key)")
    s.add_argument("object_id")
    s.add_argument("pairs", nargs="+", metavar="KEY=VALUE")
    s.set_defaults(fn=cmd_meta_set)

    s = sub.add_parser("list", help="list stored objects")
    s.add_argument("--limit", type=int, default=100)
    s.add_argument("--offset", type=int, default=0)
    s.set_defaults(fn=cmd_list)

    s = sub.add_parser("delete", help="delete an object")
    s.add_argument("object_id")
    s.set_defaults(fn=cmd_delete)

    s = sub.add_parser("cluster", help="worker status and replication health")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_cluster)

    s = sub.add_parser("events", help="recent failure/repair/corruption events")
    s.add_argument("--limit", type=int, default=30)
    s.set_defaults(fn=cmd_events)

    s = sub.add_parser("scrub", help="force a deep integrity scan of all workers now")
    s.set_defaults(fn=cmd_scrub)
    return p


def main() -> None:
    args = build_parser().parse_args()
    base = args.url.rstrip("/")
    try:
        args.fn(args, base)
    except requests.ConnectionError:
        sys.exit(f"cannot reach the controller at {base} - is the cluster running? "
                 "(docker compose up -d)")


if __name__ == "__main__":
    main()
