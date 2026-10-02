#!/usr/bin/env bash
# Stock vs tuned test databases (durability off, data on tmpfs) on a real DB-heavy test module.
set -uo pipefail
BACKEND=$1
MODULE=airflow-core/tests/unit/models/test_dagrun.py
export SKIP_IMAGE_UPGRADE_CHECK=true MOUNT_SOURCES=skip
tune() {
  python3 - "$BACKEND" <<'PY'
import sys, re
backend = sys.argv[1]
path = f"scripts/ci/docker-compose/backend-{backend}.yml"
s = open(path).read()
if backend == "postgres":
    s = s.replace("    restart: \"on-failure\"\nvolumes:", "    restart: \"on-failure\"\n    command: [\"postgres\", \"-c\", \"fsync=off\", \"-c\", \"synchronous_commit=off\", \"-c\", \"full_page_writes=off\"]\n    tmpfs:\n      - /var/lib/postgresql/data\nvolumes:", 1)
    s = s.replace("    volumes:\n      - postgres-data-volume:${POSTGRES_DATA_VOLUME_PATH:-/var/lib/postgresql/data}\n", "", 1)
elif backend == "mysql":
    s = s.replace("      '--collation-server=utf8mb4_unicode_ci',\n    ]", "      '--collation-server=utf8mb4_unicode_ci',\n      '--innodb-flush-log-at-trx-commit=0',\n      '--sync-binlog=0',\n      '--skip-log-bin',\n      '--innodb-doublewrite=OFF',\n    ]\n    tmpfs:\n      - /var/lib/mysql", 1)
    s = s.replace("      - mysql-db-volume:/var/lib/mysql\n", "", 1)
else:
    s = s.rstrip("\n") + "\n    tmpfs:\n      - /root/airflow/sqlite\n"
open(path, "w").write(s)
PY
}
run() {
  local label=$1 s e
  s=$(date +%s.%N)
  breeze testing core-tests --backend "$BACKEND" --db-reset "$MODULE" > /tmp/run.log 2>&1 || true
  e=$(date +%s.%N)
  summary=$(sed -E 's/\x1b\[[0-9;]*m//g' /tmp/run.log | grep -oE "[0-9]+ passed.* in [0-9.]+s" | tail -1)
  echo "$label wall=$(awk -v a=$s -v b=$e 'BEGIN{printf "%.1f", b-a}')s pytest: ${summary:-?}"
  breeze down >/dev/null 2>&1 || true
}
for round in 1 2; do
  git checkout -q -- scripts/ci/docker-compose/; run "round $round stock"
  tune; run "round $round tuned"; git checkout -q -- scripts/ci/docker-compose/
done
