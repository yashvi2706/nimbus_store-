#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Deliberately corrupt ONE stored chunk, for the data-integrity demonstration.
#
#   scripts/corrupt_chunk.sh <worker> <chunk_id>
#   scripts/corrupt_chunk.sh worker2 3f2a...e1_000000
#
# Find <worker> and <chunk_id> with:
#   python client/objstore_cli.py info <object_id>
#
# It overwrites 7 bytes in the middle of the chunk file inside the worker's
# container. `conv=notrunc` keeps the file the SAME SIZE, i.e. silent bit-rot
# that only a checksum can notice.
# -----------------------------------------------------------------------------
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: $0 <worker> <chunk_id>" >&2; exit 1; }
WORKER="$1"; CHUNK="$2"
FILE="/data/chunks/${CHUNK}.chunk"

echo "before: $(docker exec "$WORKER" sha256sum "$FILE")"
docker exec "$WORKER" sh -c "printf 'CORRUPT' | dd of='$FILE' bs=1 seek=4096 conv=notrunc 2>/dev/null"
echo "after : $(docker exec "$WORKER" sha256sum "$FILE")"
echo "Chunk $CHUNK on $WORKER is now corrupted."
