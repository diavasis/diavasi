#!/bin/sh
set -eu
mkdir -p /var/lib/diavasi
diavasi serve \
  --bind 0.0.0.0:7700 \
  --data-bind 0.0.0.0:7710 \
  --store /var/lib/diavasi/state \
  --token "$DIAVASI_API_TOKEN" &
pid=$!
trap 'kill "$pid" >/dev/null 2>&1 || true; wait "$pid" >/dev/null 2>&1 || true; exit 0' TERM INT
i=0
while [ "$i" -lt 100 ]; do
  if curl -sf http://127.0.0.1:7700/health >/dev/null; then
    break
  fi
  i=$((i + 1))
  sleep 0.2
done
curl -sf -o /dev/null -X DELETE \
  -H "Authorization: Bearer $DIAVASI_API_TOKEN" \
  http://127.0.0.1:7700/v1/groups/demo || true
curl -sf -o /dev/null \
  -H "Authorization: Bearer $DIAVASI_API_TOKEN" \
  -H "content-type: application/json" \
  -d '{"group_id":"demo","total_records":8,"payload_size":8,"max_buffer_records":64,"max_buffer_bytes":65536,"batch_max_records":4,"batch_timeout_ms":200,"ordering_contract":"synthetic-u64"}' \
  http://127.0.0.1:7700/v1/groups
curl -sf -o /dev/null -X POST \
  -H "Authorization: Bearer $DIAVASI_API_TOKEN" \
  http://127.0.0.1:7700/v1/groups/demo/start
echo "synthetic group demo is running"
touch /var/lib/diavasi/ready
wait "$pid"
