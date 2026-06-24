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
"""
A toolset that runs agent-generated Python in a genuinely isolated sandbox.

Unlike code_mode's in-process Monty sandbox (whose tool calls still execute on
the Airflow worker), :class:`SandboxToolset` dispatches the model's ``run_code``
into a real microVM/container via the ``apache-airflow-providers-sandbox``
``SandboxProvider`` contract (local / Daytona / E2B / Modal / islo) — so the
code runs off the worker.

Usage::

    from airflow.providers.common.ai.toolsets import SandboxToolset

    AgentOperator(
        task_id="agent",
        prompt="...",
        llm_conn_id="openai",
        toolsets=[SandboxToolset(provider="islo", image="python:3.12-slim")],
    )
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import RunContext, ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from airflow.providers.common.ai.utils.tool_definition import return_schema_kwargs

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())
_RUN_CODE_SCHEMA = {
    "type": "object",
    "properties": {
        "code": {"type": "string", "description": "Python source to run in the sandbox."}
    },
    "required": ["code"],
}


class _SandboxFailed(Exception):
    def __init__(self, stdout: str, stderr: str, exit_code: int | None) -> None:
        super().__init__(f"sandbox exit={exit_code}")
        self.stdout, self.stderr, self.exit_code = stdout, stderr, exit_code


class SandboxToolset(AbstractToolset[Any]):
    """
    Run agent-generated Python in an isolated sandbox via SandboxProvider.

    :param provider: Backend alias (local|daytona|e2b|modal|islo) or ``module:Class``.
    :param image: Provider image/template/snapshot for the sandbox.
    :param env: Environment variables injected into the sandbox (e.g. API keys).
    :param sandbox_timeout: Sandbox lifetime ceiling (seconds).
    :param poll_interval: Seconds between status polls.
    """

    def __init__(
        self,
        *,
        provider: str = "islo",
        image: str | None = None,
        env: dict[str, str] | None = None,
        sandbox_timeout: int = 600,
        poll_interval: float = 1.0,
        python_bin: str = "python3",
    ) -> None:
        self._provider_name = provider
        self._image = image
        self._env = env or {}
        self._timeout = sandbox_timeout
        self._poll = poll_interval
        self._python_bin = python_bin
        self._provider: Any = None
        self._handle: str | None = None

    @property
    def id(self) -> str:
        return f"sandbox-{self._provider_name}"

    async def __aenter__(self) -> SandboxToolset:
        from airflow.providers.sandbox.backends.base import SandboxSpec
        from airflow.providers.sandbox.provider_loader import load_provider

        def _open() -> tuple[Any, str]:
            provider = load_provider(self._provider_name)
            provider.authenticate()
            handle = provider.create_sandbox(
                SandboxSpec(image=self._image, env=self._env, timeout=self._timeout)
            )
            return provider, handle

        self._provider, self._handle = await asyncio.to_thread(_open)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._provider is not None and self._handle is not None:
            await asyncio.to_thread(self._provider.destroy, self._handle)
        self._provider = self._handle = None

    async def for_run(self, ctx: RunContext[Any]) -> SandboxToolset:
        # Fresh sandbox per agent run so concurrent runs never share state.
        return SandboxToolset(
            provider=self._provider_name,
            image=self._image,
            env=self._env,
            sandbox_timeout=self._timeout,
            poll_interval=self._poll,
            python_bin=self._python_bin,
        )

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tool_def = ToolDefinition(
            name="run_code",
            description=(
                "Run Python in an isolated sandbox and return its stdout. "
                "Print what you need back (JSON recommended)."
            ),
            parameters_json_schema=_RUN_CODE_SCHEMA,
            sequential=True,
            **return_schema_kwargs({"type": "string"}),
        )
        return {
            "run_code": ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        }

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name != "run_code":
            raise ValueError(f"Unknown tool: {name!r}")
        if self._provider is None or self._handle is None:
            raise RuntimeError("SandboxToolset used outside its async context")
        code = tool_args["code"]

        def _exec() -> str:
            from airflow.providers.sandbox.backends.base import SandboxState

            ref = self._provider.run(
                self._handle, [self._python_bin, "-c", code], env=self._env, timeout=self._timeout
            )
            deadline = time.monotonic() + self._timeout
            res = self._provider.poll_status(self._handle, ref)
            running = (SandboxState.PENDING, SandboxState.RUNNING, SandboxState.UNKNOWN)
            while res.state in running:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"sandbox run exceeded {self._timeout}s")
                time.sleep(self._poll)
                res = self._provider.poll_status(self._handle, ref)

            # Not every backend fills ExecResult.stdout (only islo does cleanly);
            # local/e2b/modal surface output via fetch_logs — fall back to it.
            out = res.stdout or ""
            if not out:
                try:
                    _msgs, lines = self._provider.fetch_logs(self._handle, ref)
                    out = "\n".join(lines)
                except Exception:
                    out = ""
            if res.state is not SandboxState.SUCCEEDED:
                raise _SandboxFailed(out, res.stderr or "", res.exit_code)
            return out

        try:
            stdout = await asyncio.to_thread(_exec)
        except _SandboxFailed as failed:
            raise ModelRetry(
                f"run_code failed (exit={failed.exit_code}).\n"
                f"stdout:\n{failed.stdout}\nstderr:\n{failed.stderr}\n"
                "Fix the code and try again."
            ) from failed
        return stdout if isinstance(stdout, str) else json.dumps(stdout)
