#!/usr/bin/env bash
# Round 2: measure the proposed mechanisms themselves on a GitHub runner.
set -uo pipefail
IMAGE=ghcr.io/apache/airflow/main/ci/python3.10:latest
ENV=(-e AIRFLOW__CORE__UNIT_TEST_MODE=True -e AIRFLOW_HOME=/root/airflow)
t() { local s rc; s=$(date +%s.%N); "$@" >/tmp/t.out 2>&1; rc=$?; awk -v a="$s" -v b="$(date +%s.%N)" -v r="$rc" 'BEGIN{printf "%.1f(rc=%d)", b-a, r}'; [ "$rc" -ne 0 ] && tail -3 /tmp/t.out >&2; true; }
collect='cd /opt/airflow && python -m pytest providers/amazon/tests --collect-only -q -p no:cacheprovider'

echo "== P1 shared bytecode dir across containers (PYTHONPYCACHEPREFIX on a host dir)"
mkdir -p /mnt/pyc-bench && sudo chmod 777 /mnt/pyc-bench
for i in 1 2 3; do
  echo "no cache (today)       collect=$(t docker run --rm "${ENV[@]}" -e PYTHONDONTWRITEBYTECODE=true --entrypoint /bin/bash "$IMAGE" -c "$collect")s"
done
for i in 1 2 3; do
  echo "shared pycache run $i  collect=$(t docker run --rm "${ENV[@]}" -e PYTHONDONTWRITEBYTECODE= -e PYTHONPYCACHEPREFIX=/pyc -v /mnt/pyc-bench:/pyc --entrypoint /bin/bash "$IMAGE" -c "$collect")s"
done
echo "pycache dir size: $(du -sh /mnt/pyc-bench | cut -f1)"

echo "== L1 lowest-deps uv sync: cold vs warm cache, per target"
sudo mkdir -p /mnt/uvc && sudo chmod 777 /mnt/uvc
for target in providers/amazon providers/google providers/snowflake providers/smtp airflow-core; do
  sync="cd /opt/airflow/$target && UV_LOCK_TIMEOUT=200 uv sync --resolution lowest-direct --no-binary-package lxml --no-binary-package xmlsec --all-extras --no-python-downloads --no-managed-python"
  cold=$(t docker run --rm "${ENV[@]}" --entrypoint /bin/bash "$IMAGE" -c "$sync")
  first=$(t docker run --rm "${ENV[@]}" -v /mnt/uvc:/root/.cache/uv --entrypoint /bin/bash "$IMAGE" -c "$sync")
  warm=$(t docker run --rm "${ENV[@]}" -v /mnt/uvc:/root/.cache/uv --entrypoint /bin/bash "$IMAGE" -c "$sync")
  echo "$target cold=${cold}s first-into-shared=${first}s warm=${warm}s"
done
echo "uv cache after 5 targets: $(sudo du -sh /mnt/uvc | cut -f1); zstd -3 size: $(sudo tar -C /mnt/uvc -cf - . | zstd -3 -T0 -q | wc -c) bytes"

echo "== Z3 docker save then zstd vs piped (CI image, 8 GB)"
s=$(date +%s.%N); docker save "$IMAGE" -o /mnt/img.tar; m=$(date +%s.%N); zstd -6 -T0 -q --rm /mnt/img.tar -o /mnt/img.tar.zst; e=$(date +%s.%N)
awk -v a=$s -v b=$m -v c=$e 'BEGIN{printf "sequential: save %.1fs + zstd %.1fs = %.1fs\n", b-a, c-b, c-a}'; ls -l /mnt/img.tar.zst | awk '{print "  size", $5}'; rm -f /mnt/img.tar.zst
s=$(date +%s.%N); docker save "$IMAGE" | zstd -6 -T0 -q -o /mnt/img.tar.zst; e=$(date +%s.%N)
awk -v a=$s -v b=$e 'BEGIN{printf "piped: %.1fs\n", b-a}'; ls -l /mnt/img.tar.zst | awk '{print "  size", $5}'; rm -f /mnt/img.tar.zst
