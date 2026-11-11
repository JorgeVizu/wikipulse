#!/bin/bash
set -euo pipefail
for attempt in {1..60}; do
  if curl -fsS http://jobmanager:8081/taskmanagers | grep -q '"id"'; then
    exec /opt/flink/bin/sql-client.sh -f /opt/flink/pipeline.sql
  fi
  sleep 2
done
echo "No Flink TaskManager registered" >&2
exit 1
