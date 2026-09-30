#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Throwaway benchmark helper for the CI image stash format A/B (bench/zstd-image-stash only).
# Every subcommand records into ${RUNNER_TEMP}/bench/*.jsonl and never fails the job, so a
# failed arm is a data point rather than an aborted job.
set -uo pipefail

BENCH_DIR="${RUNNER_TEMP:-/tmp}/bench"
mkdir -p "${BENCH_DIR}"
MNT_BENCH=/mnt/bench

now() { date +%s.%N; }
sub() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", a-b}'; }

arm_file_name() {
    # Same file names CI uses (v3 = plain tar, v4 = tar.zst); the bench arms only differ in level.
    local arm=$1 plat=$2
    case "${arm}" in
        v3|raw) echo "ci-image-save-v3-linux_${plat}-3.10.tar" ;;
        zst*) echo "ci-image-save-v4-linux_${plat}-3.10.tar.zst" ;;
    esac
}

drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

clean_state() {
    sudo rm -rf "${MNT_BENCH}"
    mkdir -p "${MNT_BENCH}"
    local ctrs imgs
    ctrs=$(docker ps -aq)
    [[ -n "${ctrs}" ]] && docker rm -f ${ctrs} >/dev/null
    imgs=$(docker images -aq)
    [[ -n "${imgs}" ]] && docker image rm -f ${imgs} >/dev/null
    docker system prune -af --volumes >/dev/null
    drop_caches
}

jline() {
    # jline file key=value ... (values are JSON-encoded strings unless prefixed with '#')
    local out=$1; shift
    python3 - "$out" "$@" <<'PY'
import json, sys
out, *kv = sys.argv[1:]
rec = {}
for item in kv:
    k, v = item.split("=", 1)
    if k.startswith("#"):
        k = k[1:]
        try:
            v = json.loads(v)
        except Exception:
            v = None
    rec[k] = v
with open(out, "a") as f:
    f.write(json.dumps(rec) + "\n")
PY
}

cmd=${1:-}; shift || true
case "${cmd}" in
    sysinfo)
        role=$1
        {
            echo "nproc=$(nproc)"
            free -b | awk '/Mem:/{print "mem_bytes="$2}'
            echo "cpu_model=$(grep -m1 -E 'model name|^Model' /proc/cpuinfo | cut -d: -f2- | xargs || true)"
            echo "docker_version=$(docker version -f '{{.Server.Version}}')"
            echo "docker_driver=$(docker info -f '{{.Driver}}')"
            echo "zstd_version=$(zstd --version 2>&1 | head -1)"
            echo "gh_version=$(gh --version | head -1)"
            echo "kernel=$(uname -r)"
        } > "${BENCH_DIR}/sysinfo-${role}.txt"
        cat "${BENCH_DIR}/sysinfo-${role}.txt"
        df -B1 / /mnt
        lsblk || true
        ;;

    # ---------------- producer ----------------
    source-image)
        # Load the real Airflow CI image: the ci-image-save-v3 artifact of an apache/airflow main run.
        artifact_id=$1
        mkdir -p /mnt/src
        t0=$(now)
        curl -sSfL --retry 3 -H "Authorization: Bearer ${GH_TOKEN}" \
            -o /mnt/src/src.zip "https://api.github.com/repos/apache/airflow/actions/artifacts/${artifact_id}/zip"
        dl_rc=$?
        t1=$(now)
        (cd /mnt/src && unzip -q src.zip && rm -f src.zip)
        t2=$(now)
        tarf=$(ls /mnt/src/*.tar | head -1)
        tar_bytes=$(stat -c %s "${tarf}")
        load_out=$(docker image load -i "${tarf}")
        load_rc=$?
        t3=$(now)
        echo "${load_out}"
        image=$(echo "${load_out}" | sed -n 's/^Loaded image: //p' | head -1)
        image_id=$(docker image inspect -f '{{.Id}}' "${image}")
        image_size=$(docker image inspect -f '{{.Size}}' "${image}")
        rm -rf /mnt/src
        echo "IMAGE=${image}" >> "${GITHUB_ENV}"
        jline "${BENCH_DIR}/producer.jsonl" kind=source artifact_id="${artifact_id}" \
            "#download_s=$(sub "$t1" "$t0")" "#unzip_s=$(sub "$t2" "$t1")" \
            "#load_s=$(sub "$t3" "$t2")" "#download_rc=${dl_rc}" "#load_rc=${load_rc}" \
            "#tar_bytes=${tar_bytes}" image="${image}" image_id="${image_id}" "#image_size=${image_size}"
        docker images
        ;;
    make)
        arm=$1 plat=$2
        sudo rm -rf "${MNT_BENCH}"; mkdir -p "${MNT_BENCH}/${arm}"
        f="${MNT_BENCH}/${arm}/$(arm_file_name "${arm}" "${plat}")"
        drop_caches
        t0=$(now)
        case "${arm}" in
            v3|raw) docker image save -o "${f}" "${IMAGE}"; rc=$? ;;
            zst1) docker image save "${IMAGE}" | zstd -1 -T0 -o "${f}"; rc=$? ;;
            zst3) docker image save "${IMAGE}" | zstd -3 -T0 -o "${f}"; rc=$? ;;
            zst6) docker image save "${IMAGE}" | zstd -6 -T0 -o "${f}"; rc=$? ;;
        esac
        t1=$(now)
        bytes=$(stat -c %s "${f}" 2>/dev/null || echo null)
        ls -l "${f}"
        now > "${BENCH_DIR}/upload-${arm}.t0"
        jline "${BENCH_DIR}/producer.jsonl" kind=make arm="${arm}" "#rc=${rc}" \
            "#save_s=$(sub "$t1" "$t0")" "#archive_bytes=${bytes}"
        ;;
    uploaded)
        arm=$1 outcome=$2 stash_id=$3
        t1=$(now)
        t0=$(cat "${BENCH_DIR}/upload-${arm}.t0")
        sudo rm -rf "${MNT_BENCH}"
        jline "${BENCH_DIR}/producer.jsonl" kind=upload arm="${arm}" outcome="${outcome}" \
            stash_id="${stash_id}" "#upload_s=$(sub "$t1" "$t0")"
        ;;

    # ---------------- consumer ----------------
    prep)
        arm=$1
        t_clean=$(now)
        clean_state
        t0=$(now)
        echo "${t0}" > "${BENCH_DIR}/restore-${arm}.t0"
        echo "clean took $(sub "$t0" "$t_clean") s"
        df -B1 / /mnt
        ;;
    load)
        arm=$1 pos=$2 outcome=$3
        t1=$(now)
        t0=$(cat "${BENCH_DIR}/restore-${arm}.t0" 2>/dev/null || echo "${t1}")
        restore_s=$(sub "$t1" "$t0")
        f=$(ls "${MNT_BENCH}/${arm}"/* 2>/dev/null | head -1)
        bytes=null; load_s=null; load_rc=null; image_ids=""
        if [[ -n "${f}" ]]; then
            bytes=$(stat -c %s "${f}")
            ls -l "${f}"
            t2=$(now)
            docker image load -i "${f}"
            load_rc=$?
            t3=$(now)
            load_s=$(sub "$t3" "$t2")
            image_ids=$(docker images --no-trunc -q | sort -u | paste -sd, -)
            docker images
        fi
        jline "${BENCH_DIR}/consumer.jsonl" arm="${arm}" "#pos=${pos}" restore_outcome="${outcome}" \
            "#restore_s=${restore_s}" "#file_bytes=${bytes}" "#load_s=${load_s}" "#load_rc=${load_rc}" \
            image_ids="${image_ids}" file="${f}" "#t_restore_start=${t0}"
        sudo rm -rf "${MNT_BENCH}"
        ;;
    *)
        echo "unknown subcommand ${cmd}"
        ;;
esac
exit 0
