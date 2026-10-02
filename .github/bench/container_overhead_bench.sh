#!/usr/bin/env bash
# Per-container fixed costs in the Airflow CI image (run on a native-arch daemon; never prunes anything).
#   E1 cold imports (no bytecode)        E2 cold pytest collection of one provider
#   E3 airflow db reset (sqlite)         E4 bytecode precompile cost + E1/E2 again with .pyc present
#   E5 lowest-deps uv sync, cold cache vs a uv cache shared between containers
set -uo pipefail
IMAGE=${IMAGE:-ghcr.io/apache/airflow/main/ci/python3.10:latest}
PROVIDER=${PROVIDER:-amazon}
RUNS=${RUNS:-3}
ENV=(-e AIRFLOW__CORE__UNIT_TEST_MODE=True -e AIRFLOW_HOME=/root/airflow -e PYTHONDONTWRITEBYTECODE= )
run() { docker run --rm "${ENV[@]}" --entrypoint /bin/bash "$@"; }
t() { local s rc; s=$(date +%s.%N); "$@" >/tmp/t.out 2>&1; rc=$?; awk -v a="$s" -v b="$(date +%s.%N)" -v r="$rc" 'BEGIN{printf "%.1f(rc=%d)", b-a, r}'; [ "$rc" -ne 0 ] && tail -3 /tmp/t.out >&2; true; }

imports='python -c "import airflow.models, airflow.providers.'"${PROVIDER}"'"'
collect="cd /opt/airflow && python -m pytest providers/${PROVIDER}/tests --collect-only -q -p no:cacheprovider"

echo "== E1/E2/E3 cold (image as published), $RUNS runs"
for i in $(seq 1 "$RUNS"); do
  echo "imports=$(t run "$IMAGE" -c "$imports")s collect=$(t run "$IMAGE" -c "$collect")s db_reset=$(t run "$IMAGE" -c 'airflow db reset -y')s"
done

echo "== E4 precompile bytecode into a temporary image"
cid=$(docker create "${ENV[@]}" --entrypoint /bin/bash "$IMAGE" -c \
  'python -m compileall -q -j0 /usr/python/lib/python3.10/site-packages /opt/airflow >/dev/null 2>&1; true')
s=$(date +%s); docker start -a "$cid" >/dev/null; echo "compileall took $(( $(date +%s) - s ))s"
docker commit "$cid" ci-bench-pyc:latest >/dev/null; docker rm "$cid" >/dev/null
echo "image size: published $(docker image inspect "$IMAGE" --format '{{.Size}}') pyc $(docker image inspect ci-bench-pyc:latest --format '{{.Size}}')"
for i in $(seq 1 "$RUNS"); do
  echo "pyc imports=$(t run ci-bench-pyc:latest -c "$imports")s collect=$(t run ci-bench-pyc:latest -c "$collect")s"
done
docker rmi ci-bench-pyc:latest >/dev/null

echo "== E5 lowest-deps uv sync for providers/${PROVIDER}"
sync='cd /opt/airflow/providers/'"${PROVIDER/.//}"' && UV_LOCK_TIMEOUT=200 uv sync --resolution lowest-direct --no-binary-package lxml --no-binary-package xmlsec --all-extras --no-python-downloads --no-managed-python'
vol=uv-bench-cache-$$
docker volume create "$vol" >/dev/null
echo "cold cache: $(t run "$IMAGE" -c "$sync")s"
echo "shared cache, 1st container: $(t run -v "$vol":/root/.cache/uv "$IMAGE" -c "$sync")s"
echo "shared cache, 2nd container: $(t run -v "$vol":/root/.cache/uv "$IMAGE" -c "$sync")s"
docker volume rm "$vol" >/dev/null

echo "== bytecode footprint if precompiled"
docker run --rm --entrypoint /bin/bash "$IMAGE" -c \
  'python -m compileall -q -j0 /usr/python/lib/python3.10/site-packages /opt/airflow >/dev/null 2>&1;
   find /usr/python/lib/python3.10/site-packages /opt/airflow -name "*.pyc" -print0 | du -ch --files0-from=- | tail -1;
   find /usr/python/lib/python3.10/site-packages /opt/airflow -name "*.pyc" | tar -cf - -T - 2>/dev/null | zstd -6 -T0 -q | wc -c'
