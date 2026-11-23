# Mini Distributed Cloud Object Store

**Cloud Computing, Infrastructure Project**

A distributed object storage system inspired by Amazon S3. Files are split into **1 MiB chunks**, with each chunk stored on **two different workers**. The system detects worker failures and corrupted data and automatically restores missing or invalid copies.

**Deployment:** 1 controller and 4 storage workers using Docker Compose.  
**Team:** Raj Gandhi and Yashvi Mehta.

## 1. Architecture

- **Controller:** FastAPI service that handles client requests, stores metadata in SQLite, selects workers, and manages recovery.
- **Workers:** Store chunk files on persistent Docker volumes and send regular heartbeats.
- **Client:** Uses a REST API or the Python CLI.
- **Replication:** Each chunk is stored on two different workers.
- **Integrity:** SHA-256 checksums detect corrupted chunks. Good replicas are used to repair bad or missing copies.

### Main workflow

1. On upload, the controller splits a file into 1 MiB chunks and stores each chunk on two workers.
2. On download, it verifies each chunk's checksum before returning it.
3. If a worker fails, the controller detects the failure and recreates missing replicas on healthy workers.
4. A background scrub checks stored chunks and repairs corrupted or missing copies.

## 2. Project structure

```text
mini-object-store/
├── docker-compose.yml
├── controller/
│   └── app/
│       ├── main.py
│       ├── routes_client.py
│       ├── routes_internal.py
│       ├── cluster.py
│       ├── maintenance.py
│       └── db.py
├── worker/
│   └── app/
│       ├── main.py
│       └── store.py
├── client/objstore_cli.py
├── demo/run_demo.py
├── scripts/corrupt_chunk.sh
└── tests/
```

## 3. Prerequisites

- Docker
- Docker Compose v2
- Python 3.10 or newer
- Around 1 GB of free disk space

## 4. Run the project

Run these commands from the project folder.

### Build and start

```bash
docker compose build
docker compose up -d
docker compose ps
```

Wait until the controller and all four workers are healthy.

### Set up the client environment

```bash
./setup.sh
source .venv/bin/activate
```

### Check the cluster

```bash
python client/objstore_cli.py cluster
```

### Upload and list a file

```bash
echo "hello world" > hello.txt
python client/objstore_cli.py upload hello.txt --meta author=me
python client/objstore_cli.py list
```

Open the API documentation at `http://localhost:8000/docs`.

## 5. Client REST API

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/objects` | Upload an object |
| GET | `/api/v1/objects/{object_id}` | Download an object |
| GET | `/api/v1/objects/{object_id}/metadata` | Get metadata |
| PATCH | `/api/v1/objects/{object_id}/metadata` | Update metadata |
| DELETE | `/api/v1/objects/{object_id}` | Delete an object |
| GET | `/api/v1/objects` | List objects |

The Python CLI provides the same main operations:

```bash
python client/objstore_cli.py upload <file>
python client/objstore_cli.py download <object_id> -o <output_file>
python client/objstore_cli.py info <object_id>
python client/objstore_cli.py meta-set <object_id> author=Raj
python client/objstore_cli.py delete <object_id>
python client/objstore_cli.py list
python client/objstore_cli.py cluster
```

## 6. Required demonstration

Run the complete demonstration:

```bash
python demo/run_demo.py --pause
```

It demonstrates:

1. Uploading and downloading an object.
2. Splitting an object into 1 MiB chunks and distributing them.
3. Verifying that every chunk has two replicas on different workers.
4. Adding and updating metadata.
5. Stopping a worker and verifying automatic recovery.
6. Corrupting a chunk and verifying that the system detects and repairs it.

Press Enter between steps. The final output should be `ALL CHECKS PASSED`.

### Manual worker-failure check

In one terminal, monitor the cluster:

```bash
watch -n 1 '.venv/bin/python client/objstore_cli.py cluster'
```

In another terminal, stop a worker:

```bash
docker stop worker2
```

The controller should detect the failure and recreate missing replicas. Start the worker again:

```bash
docker start worker2
```

### Persistence check

```bash
docker compose down
docker compose up -d
```

Stored data should still be available because worker data and controller metadata use persistent volumes.

## 7. Tests

Run the automated tests:

```bash
source .venv/bin/activate
python -m pytest tests -q
```

The test suite covers the REST API, chunking, replication, metadata, worker failures, data integrity, internal worker APIs, the demo, and controller restarts.

## 8. Configuration

The main settings are in `docker-compose.yml`.

| Variable | Default | Purpose |
|---|---:|---|
| `CHUNK_SIZE` | `1048576` | Chunk size (1 MiB) |
| `REPLICATION_FACTOR` | `2` | Copies of each chunk |
| `HEARTBEAT_TIMEOUT` | `6` | Seconds before a worker is marked dead |
| `REPAIR_GRACE_SECONDS` | `15` | Wait before recreating replicas |
| `SCRUB_INTERVAL` | `60` | Seconds between integrity scans |

## 9. Division of work

### Raj Gandhi — Controller, client API and metadata

- Client REST API and error handling
- Object and metadata model, SQLite schema
- Chunking, worker selection, upload and download
- Python command-line client
- API, chunking, metadata and controller-restart tests


### Yashvi Mehta — Workers, failure handling, integrity and deployment

- Worker service and chunk storage
- Docker Compose, volumes and setup scripts
- Worker registration, heartbeats and failure detection
- Automatic repair and corruption handling
- Failure, integrity, worker API and demo tests


## 10. Verification status

The original project notes report that all **40 automated tests** and all six required demonstration checks passed. The persistence demo and optional worker-port override should be tested separately before presenting them.
