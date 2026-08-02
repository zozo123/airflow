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

_REQUEST_ID = "234e4567-e89b-12d3-a456-426614174000"
_HANDLE_DATA = {
    "request_id": _REQUEST_ID,
    "sandbox_name": f"airflow-{_REQUEST_ID}",
    "sandbox_id": "sandbox-id",
    "schema_version": 1,
}


@pytest.fixture
def operator(tmp_path):
    return DockerSandboxJobOperator(
        task_id="sandbox_job",
        template="python:3.13",
        command=["python", "-c", "print('ok')"],
        scratch_root=str(tmp_path),
    )


def _event(state: str, *, exit_code: int | None = None, message: str | None = None):
    return {
        "state": state,
        "exit_code": exit_code,
        "message": message,
        "handle_data": _HANDLE_DATA,
        "display_name": f"airflow-{_REQUEST_ID}",
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


def test_execute_defers_with_persisted_handle(operator):
    handle = SandboxHandle(data=_HANDLE_DATA, display_name=f"airflow-{_REQUEST_ID}")
    driver = mock.Mock()
    driver.launch = mock.AsyncMock(return_value=handle)
    driver.close = mock.AsyncMock()
    operator._driver = mock.Mock(return_value=driver)
    operator._request_id = mock.Mock(return_value=_REQUEST_ID)
    operator.defer = mock.Mock(side_effect=RuntimeError("deferred"))

    with pytest.raises(RuntimeError, match="deferred"):
        operator.execute({"ti": mock.Mock()})

    assert operator._handle == handle
    trigger = operator.defer.call_args.kwargs["trigger"]
    assert trigger.handle_data == _HANDLE_DATA
    assert trigger.keep is False


def test_execute_complete_recovers_handle_from_event(operator):
    operator._handle = None
    operator._terminate_current_handle = mock.Mock()

    result = operator.execute_complete({}, _event("succeeded", exit_code=0))

    assert result == {"state": "succeeded", "exit_code": 0, "message": None}
    assert operator._handle == SandboxHandle(data=_HANDLE_DATA, display_name=f"airflow-{_REQUEST_ID}")
    operator._terminate_current_handle.assert_called_once_with()


def test_execute_complete_rejects_event_without_handle(operator):
    with pytest.raises(AirflowException, match="persisted handle"):
        operator.execute_complete({}, {"state": "succeeded"})


def test_execute_complete_raises_on_failure_and_terminates(operator):
    operator._terminate_current_handle = mock.Mock()

    with pytest.raises(AirflowException, match="exit code 17"):
        operator.execute_complete({}, _event("failed", exit_code=17, message="boom"))

    operator._terminate_current_handle.assert_called_once_with()


def test_keep_skips_completion_cleanup(operator):
    operator.keep = True
    operator._terminate_current_handle = mock.Mock()

    operator.execute_complete({}, _event("succeeded", exit_code=0))

    operator._terminate_current_handle.assert_not_called()


def test_on_kill_terminates_current_job(operator):
    operator._terminate_current_handle = mock.Mock()

    operator.on_kill()

    operator._terminate_current_handle.assert_called_once_with()
