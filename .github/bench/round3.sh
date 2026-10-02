#!/usr/bin/env bash
# Round 3: docker save then zstd (what ci-image-build.yml does today) vs docker save piped into zstd.
set -uo pipefail
IMAGE=ghcr.io/apache/airflow/main/ci/python3.10:latest
sudo chmod 777 /mnt
for round in 1 2; do
  s=$(date +%s.%N); docker save "$IMAGE" -o /mnt/img.tar; m=$(date +%s.%N)
  zstd -6 -T0 -q --rm /mnt/img.tar -o /mnt/img.tar.zst; e=$(date +%s.%N)
  awk -v a=$s -v b=$m -v c=$e -v r=$round 'BEGIN{printf "round %d sequential: save %.1fs + zstd %.1fs = %.1fs\n", r, b-a, c-b, c-a}'
  ls -l /mnt/img.tar.zst | awk '{print "  size", $5}'; rm -f /mnt/img.tar.zst
  s=$(date +%s.%N); docker save "$IMAGE" | zstd -6 -T0 -q -o /mnt/img.tar.zst; e=$(date +%s.%N)
  awk -v a=$s -v b=$e -v r=$round 'BEGIN{printf "round %d piped: %.1fs\n", r, b-a}'
  ls -l /mnt/img.tar.zst | awk '{print "  size", $5}'; zstd -t -q /mnt/img.tar.zst && echo "  integrity ok"; rm -f /mnt/img.tar.zst
done
