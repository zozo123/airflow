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

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.common.sandbox.models import SandboxHandle
from airflow.providers.docker.sandbox.operators import DockerSandboxJobOperator


@pytest.fixture
def operator(tmp_path):
    return DockerSandboxJobOperator(
        task_id="sandbox_job",
        template="python:3.13",
        command=["python", "-c", "print('ok')"],
        scratch_root=str(tmp_path),
    )


@pytest.fixture
def handle():
    return SandboxHandle(
        data={
            "request_id": "234e4567-e89b-12d3-a456-426614174000",
            "sandbox_name": "airflow-234e4567-e89b-12d3-a456-426614174000",
            "sandbox_id": "sandbox-id",
            "schema_version": 1,
        },
        display_name="airflow-234e4567-e89b-12d3-a456-426614174000",
    )


def event_for(handle, **values):
    return {
        "handle_data": handle.data,
        "display_name": handle.display_name,
        **values,
    }


def test_request_id_is_stable_for_one_task_attempt(operator):
    context = {
        "ti": SimpleNamespace(
            dag_id="dag",
            run_id="run",
            task_id="sandbox_job",
            map_index=-1,
            try_number=1,
        )
    }
    assert operator._request_id(context) == operator._request_id(context)


def test_execute_defers_with_serialized_handle(operator, handle):
    driver = mock.Mock()
    driver.launch = mock.AsyncMock(return_value=handle)
    driver.close = mock.AsyncMock()
    operator._driver = mock.Mock(return_value=driver)
    operator._request_id = mock.Mock(return_value="234e4567-e89b-12d3-a456-426614174000")
    operator.defer = mock.Mock(side_effect=RuntimeError("deferred"))

    with pytest.raises(RuntimeError, match="deferred"):
        operator.execute({"ti": mock.Mock()})

    trigger = operator.defer.call_args.kwargs["trigger"]
    assert trigger.handle_data == handle.data
    assert trigger.display_name == handle.display_name


def test_execute_complete_reconstructs_handle_after_restart(operator, handle):
    operator._handle = None
    operator._terminate = mock.Mock()

    result = operator.execute_complete(
        {},
        event_for(handle, state="succeeded", exit_code=0, message=None),
    )

    assert result["state"] == "succeeded"
    operator._terminate.assert_called_once_with(handle)


def test_execute_complete_raises_on_failure_and_terminates(operator, handle):
    operator._terminate = mock.Mock()

    with pytest.raises(AirflowException, match="exit code 17"):
        operator.execute_complete(
            {},
            event_for(handle, state="failed", exit_code=17, message="boom"),
        )

    operator._terminate.assert_called_once_with(handle)


def test_execute_complete_rejects_missing_handle(operator):
    with pytest.raises(AirflowException, match="no durable handle"):
        operator.execute_complete({}, {"state": "succeeded"})


def test_on_kill_terminates_current_job(operator, handle):
    operator._handle = handle
    operator._terminate = mock.Mock()

    operator.on_kill()

    operator._terminate.assert_called_once_with(handle)
