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
"""SandboxToolset runs agent code in a sandbox via SandboxProvider.

Hermetic: uses the local subprocess backend (no SaaS creds) and pydantic-ai's
FunctionModel (no LLM key), so the full agent -> run_code -> sandbox path is
exercised offline.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("pydantic_ai")
pytest.importorskip("airflow.providers.common.ai.sandbox.base")

from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from airflow.providers.common.ai.toolsets import SandboxToolset


def test_call_tool_runs_code_in_local_sandbox():
    async def _run():
        async with SandboxToolset(provider="local") as live:
            tools = await live.get_tools(None)
            return await live.call_tool(
                "run_code", {"code": "print('HELLO_FROM_SANDBOX')"}, None, tools["run_code"]
            )

    out = asyncio.run(_run())
    assert "HELLO_FROM_SANDBOX" in out


def test_failing_code_raises_model_retry():
    from pydantic_ai.exceptions import ModelRetry

    async def _run():
        async with SandboxToolset(provider="local") as live:
            tools = await live.get_tools(None)
            await live.call_tool(
                "run_code", {"code": "raise SystemExit(3)"}, None, tools["run_code"]
            )

    with pytest.raises(ModelRetry):
        asyncio.run(_run())


def test_agent_executes_run_code_in_sandbox():
    state = {"n": 0}

    def model_fn(messages, info):
        if state["n"] == 0:
            state["n"] += 1
            return ModelResponse(
                parts=[ToolCallPart(tool_name="run_code", args={"code": "print('AGENT_SBX_OK')"})]
            )
        return ModelResponse(parts=[TextPart("done")])

    agent = Agent(FunctionModel(model_fn), toolsets=[SandboxToolset(provider="local")])
    result = agent.run_sync("run code")
    ran = any(
        "AGENT_SBX_OK" in str(getattr(p, "content", ""))
        for m in result.all_messages()
        for p in getattr(m, "parts", [])
    )
    assert ran


def test_toolset_id_reflects_provider():
    assert SandboxToolset(provider="islo").id == "sandbox-islo"
