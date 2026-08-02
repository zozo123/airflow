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
"""System DAG for the deferrable Docker Sandbox job operator."""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.sandbox.operators import DockerSandboxJobOperator

from tests_common.test_utils.system_tests import get_test_run

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_docker_sandbox_job_operator"
SCRATCH_ROOT = os.environ.get("AIRFLOW__DOCKER_SANDBOX__WORKSPACE_ROOT", "/tmp/airflow-docker-sandbox")
TEMPLATE = os.environ.get("AIRFLOW__DOCKER_SANDBOX__JOB_TEMPLATE", "python:3.13")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "docker", "sandbox"],
) as dag:
    success = DockerSandboxJobOperator(
        task_id="success",
        template=TEMPLATE,
        command=["python", "-c", "print('docker sandbox job ok')"],
        scratch_root=SCRATCH_ROOT,
        deferrable=True,
    )

    nonzero_exit = DockerSandboxJobOperator(
        task_id="nonzero_exit",
        template=TEMPLATE,
        command=["python", "-c", "raise SystemExit(17)"],
        scratch_root=SCRATCH_ROOT,
        deferrable=True,
    )
    nonzero_exit.trigger_rule = "all_done"

    success >> nonzero_exit

    from tests_common.test_utils.watcher import watcher

    nonzero_exit >> watcher()

    test_run = get_test_run(dag)
