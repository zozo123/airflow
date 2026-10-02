#!/usr/bin/env bash
# Restore the CI image with `docker load` (today) vs by restoring a snapshot of Docker's data directory.
set -uo pipefail
IMAGE=ghcr.io/apache/airflow/main/ci/python3.10:latest
ROOT=/var/lib/docker
ts() { date +%s.%N; }
dt() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.1f", b-a}'; }
./scripts/ci/move_docker_to_mnt.sh >/dev/null 2>&1
sudo chmod 777 /mnt
docker pull -q "$IMAGE" >/dev/null
docker save "$IMAGE" | zstd -6 -T0 -q -o /mnt/ci.tar.zst; ls -l /mnt/ci.tar.zst | awk '{print "stash size", $5}'
wipe() { sudo systemctl stop docker docker.socket; sudo find "$ROOT" -mindepth 1 -maxdepth 1 -exec rm -rf {} +; sudo systemctl start docker; }
for i in 1 2; do
  wipe; s=$(ts); docker load -i /mnt/ci.tar.zst >/dev/null; e=$(ts); echo "round $i docker load: $(dt $s $e)s"
done
sudo systemctl stop docker docker.socket
s=$(ts); sudo tar -C "$ROOT" --xattrs --acls --numeric-owner -cf - . | zstd -3 -T0 -q -o /mnt/snap.tar.zst; e=$(ts)
ls -l /mnt/snap.tar.zst | awk '{print "snapshot size", $5}'; echo "snapshot creation: $(dt $s $e)s"
sudo systemctl start docker
for i in 1 2; do
  sudo systemctl stop docker docker.socket; sudo find "$ROOT" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  s=$(ts); zstd -d -T0 -q -c /mnt/snap.tar.zst | sudo tar -C "$ROOT" --xattrs --acls --numeric-owner -xf -; m=$(ts)
  sudo systemctl start docker; docker image inspect "$IMAGE" >/dev/null && e=$(ts)
  echo "round $i snapshot restore: extract $(dt $s $m)s + daemon start $(dt $m $e)s = $(dt $s $e)s"
  docker run --rm --entrypoint /bin/bash "$IMAGE" -c 'python -c "import airflow, airflow.providers.amazon; print(\"image works\", airflow.__version__)"'
done
