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
"""Deferrable trigger for one Docker Sandboxes job."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.common.sandbox.models import SandboxHandle, SandboxState
from airflow.providers.docker.sandbox.driver import DockerSandboxDriver
from airflow.providers.docker.sandbox.models import DockerSandboxDriverConfig
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DockerSandboxJobTrigger(BaseTrigger):
    """Wait asynchronously for a Docker Sandboxes job to reach a terminal state."""

    def __init__(
        self,
        *,
        handle_data: dict[str, Any],
        display_name: str | None,
        scratch_root: str,
        sbx_binary: str = "sbx",
        poll_interval: float = 1.0,
    ) -> None:
        super().__init__()
        self.handle_data = handle_data
        self.display_name = display_name
        self.scratch_root = scratch_root
        self.sbx_binary = sbx_binary
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.docker.sandbox.triggers.DockerSandboxJobTrigger",
            {
                "handle_data": self.handle_data,
                "display_name": self.display_name,
                "scratch_root": self.scratch_root,
                "sbx_binary": self.sbx_binary,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        driver = DockerSandboxDriver(
            DockerSandboxDriverConfig(
                scratch_root=self.scratch_root,
                sbx_binary=self.sbx_binary,
            )
        )
        handle = SandboxHandle(data=self.handle_data, display_name=self.display_name)
        try:
            while True:
                result = await driver.get_status(handle)
                if result.state in {SandboxState.SUCCEEDED, SandboxState.FAILED, SandboxState.GONE}:
                    yield TriggerEvent(
                        {
                            "state": result.state.value,
                            "exit_code": result.exit_code,
                            "message": result.message,
                        }
                    )
                    return
                await asyncio.sleep(result.retry_after or self.poll_interval)
        except Exception as error:
            yield TriggerEvent({"state": "error", "message": str(error)})
        finally:
            await driver.close()
