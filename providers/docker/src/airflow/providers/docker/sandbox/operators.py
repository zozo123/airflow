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
"""Task-scoped jobs executed through Docker Sandboxes and the ``sbx`` CLI."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_URL, uuid5

from airflow.exceptions import AirflowException
from airflow.providers.common.sandbox.models import SandboxHandle, SandboxLaunchRequest
from airflow.providers.docker.sandbox.driver import DockerSandboxDriver
from airflow.providers.docker.sandbox.models import DockerSandboxDriverConfig
from airflow.providers.docker.sandbox.triggers import DockerSandboxJobTrigger
from airflow.providers.docker.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DockerSandboxJobOperator(BaseOperator):
    """Run one author-declared job in a local Docker Sandbox."""

    template_fields = ("command", "env", "workdir")

    def __init__(
        self,
        *,
        template: str,
        command: list[str] | tuple[str, ...],
        scratch_root: str,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        cpus: int | None = None,
        memory: str | None = None,
        timeout_seconds: int = 3600,
        ttl_seconds: int = 86400,
        keep: bool = False,
        sbx_binary: str = "sbx",
        poll_interval: float = 1.0,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.template = template
        self.command = tuple(command)
        self.scratch_root = scratch_root
        self.env = dict(env or {})
        self.workdir = workdir
        self.cpus = cpus
        self.memory = memory
        self.timeout_seconds = timeout_seconds
        self.ttl_seconds = ttl_seconds
        self.keep = keep
        self.sbx_binary = sbx_binary
        self.poll_interval = poll_interval
        self.deferrable = deferrable
        self._handle: SandboxHandle | None = None

    def execute(self, context: Context) -> dict[str, Any]:
        driver = self._driver()
        try:
            self._handle = asyncio.run(
                driver.launch(
                    SandboxLaunchRequest(
                        request_id=self._request_id(context),
                        command=self.command,
                        env=self.env,
                        provider_config={"template": self.template, "cpus": self.cpus, "memory": self.memory},
                        workdir=self.workdir,
                        timeout_seconds=self.timeout_seconds,
                        ttl_seconds=self.ttl_seconds,
                        keep=self.keep,
                    )
                )
            )
        finally:
            asyncio.run(driver.close())

        if not self.deferrable:
            return self._wait_synchronously(self._handle)

        self.defer(
            trigger=DockerSandboxJobTrigger(
                handle_data=self._handle.data,
                display_name=self._handle.display_name,
                scratch_root=self.scratch_root,
                sbx_binary=self.sbx_binary,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )
        raise AssertionError("unreachable after defer")

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        del context
        handle = self._handle_from_event(event)
        state = event.get("state")
        try:
            if state == "succeeded":
                return {"state": state, "exit_code": event.get("exit_code"), "message": event.get("message")}
            if state == "failed":
                raise AirflowException(
                    f"Docker Sandbox job failed with exit code {event.get('exit_code')}: "
                    f"{event.get('message') or 'no diagnostic message'}"
                )
            if state == "gone":
                raise AirflowException(
                    "Docker Sandbox disappeared before reporting a terminal result: "
                    f"{event.get('message') or 'no diagnostic message'}"
                )
            raise AirflowException(
                f"Docker Sandbox trigger failed: {event.get('message') or f'unexpected state {state!r}'}"
            )
        finally:
            if not self.keep:
                self._terminate(handle)

    def on_kill(self) -> None:
        if self._handle is not None and not self.keep:
            self._terminate(self._handle)

    def _wait_synchronously(self, handle: SandboxHandle) -> dict[str, Any]:
        driver = self._driver()
        try:
            while True:
                result = asyncio.run(driver.get_status(handle))
                if result.state.value == "succeeded":
                    return {"state": "succeeded", "exit_code": result.exit_code, "message": result.message}
                if result.state.value in {"failed", "gone"}:
                    raise AirflowException(
                        f"Docker Sandbox job ended in {result.state.value}: "
                        f"{result.message or 'no diagnostic message'}"
                    )
                time.sleep(result.retry_after or self.poll_interval)
        finally:
            if not self.keep:
                asyncio.run(driver.terminate(handle))
            asyncio.run(driver.close())

    def _terminate(self, handle: SandboxHandle) -> None:
        driver = self._driver()
        try:
            asyncio.run(driver.terminate(handle))
        finally:
            asyncio.run(driver.close())

    @staticmethod
    def _handle_from_event(event: dict[str, Any]) -> SandboxHandle:
        handle_data = event.get("handle_data")
        if not isinstance(handle_data, dict):
            raise AirflowException("Docker Sandbox trigger event has no durable handle")
        display_name = event.get("display_name")
        if display_name is not None and not isinstance(display_name, str):
            raise AirflowException("Docker Sandbox trigger event has an invalid display name")
        return SandboxHandle(data=handle_data, display_name=display_name)

    def _driver(self) -> DockerSandboxDriver:
        return DockerSandboxDriver(
            DockerSandboxDriverConfig(scratch_root=self.scratch_root, sbx_binary=self.sbx_binary)
        )

    def _request_id(self, context: Context) -> str:
        ti = context["ti"]
        identity = "|".join((ti.dag_id, ti.run_id, ti.task_id, str(ti.map_index), str(ti.try_number)))
        return str(uuid5(NAMESPACE_URL, f"airflow-docker-sandbox-job:{identity}"))
