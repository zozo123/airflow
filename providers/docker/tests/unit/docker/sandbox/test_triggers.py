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

import asyncio
from unittest import mock

import pytest

from airflow.providers.common.sandbox.models import SandboxResult, SandboxState
from airflow.providers.docker.sandbox.triggers import DockerSandboxJobTrigger

_REQUEST_ID = "234e4567-e89b-12d3-a456-426614174000"
_HANDLE_DATA = {
    "request_id": _REQUEST_ID,
    "sandbox_name": f"airflow-{_REQUEST_ID}",
    "sandbox_id": "sandbox-id",
    "schema_version": 1,
}


def _trigger(*, keep: bool = False) -> DockerSandboxJobTrigger:
    return DockerSandboxJobTrigger(
        handle_data=_HANDLE_DATA,
        display_name=f"airflow-{_REQUEST_ID}",
        scratch_root="/tmp/airflow-sandbox-test",
        keep=keep,
    )


def test_serialize_contains_complete_restart_state():
    trigger = _trigger(keep=True)

    classpath, payload = trigger.serialize()

    assert classpath == "airflow.providers.docker.sandbox.triggers.DockerSandboxJobTrigger"
    assert payload["handle_data"] == _HANDLE_DATA
    assert payload["keep"] is True


@pytest.mark.asyncio
async def test_terminal_event_returns_handle(monkeypatch):
    driver = mock.Mock()
    driver.get_status = mock.AsyncMock(return_value=SandboxResult(SandboxState.SUCCEEDED, exit_code=0))
    driver.close = mock.AsyncMock()
    monkeypatch.setattr(
        "airflow.providers.docker.sandbox.triggers.DockerSandboxDriver",
        mock.Mock(return_value=driver),
    )

    events = [event async for event in _trigger().run()]

    assert len(events) == 1
    assert events[0].payload["state"] == "succeeded"
    assert events[0].payload["handle_data"] == _HANDLE_DATA
    driver.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_trigger_terminates_external_job(monkeypatch):
    driver = mock.Mock()

    async def block_status(handle):
        del handle
        raise asyncio.CancelledError

    driver.get_status = mock.AsyncMock(side_effect=block_status)
    driver.terminate = mock.AsyncMock()
    driver.close = mock.AsyncMock()
    monkeypatch.setattr(
        "airflow.providers.docker.sandbox.triggers.DockerSandboxDriver",
        mock.Mock(return_value=driver),
    )

    with pytest.raises(asyncio.CancelledError):
        [event async for event in _trigger().run()]

    driver.terminate.assert_awaited_once()
    driver.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_trigger_respects_keep(monkeypatch):
    driver = mock.Mock()
    driver.get_status = mock.AsyncMock(side_effect=asyncio.CancelledError)
    driver.terminate = mock.AsyncMock()
    driver.close = mock.AsyncMock()
    monkeypatch.setattr(
        "airflow.providers.docker.sandbox.triggers.DockerSandboxDriver",
        mock.Mock(return_value=driver),
    )

    with pytest.raises(asyncio.CancelledError):
        [event async for event in _trigger(keep=True).run()]

    driver.terminate.assert_not_awaited()
    driver.close.assert_awaited_once()
