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
"""Recording-only Dag for the Islo SandboxToolset UI demo."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from airflow.providers.common.compat.sdk import dag as airflow_dag, task

DAG_ID = "common_ai_sandbox_toolset_islo_ui_demo"
MARKER = "islo-microvm-ok"
STATE_PATH = "/tmp/airflow_sandbox_e2e"


@airflow_dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["common.ai", "sandbox", "islo", "ui-demo"],
)
def example_sandbox_toolset_islo_ui():
    @task
    def run_sandbox_agent() -> str:
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import IsloSandboxBackend, SandboxSpec
        from airflow.providers.common.ai.toolsets import SandboxToolset

        log = logging.getLogger(__name__)
        steps = [
            ("write_file", {"path": STATE_PATH, "content": MARKER}),
            (
                "run_command",
                {"command": f"cat {STATE_PATH} && echo SPEC_MARKER=$SPEC_MARKER && echo $((6 * 7))"},
            ),
            ("read_file", {"path": STATE_PATH}),
            ("run_command", {"command": "echo expected-failure >&2; exit 3"}),
            ("list_directory", {"path": "/tmp"}),
        ]
        labels = [
            "write_file → native upload into the Islo microVM",
            "run_command → persistence, environment propagation, arithmetic",
            "read_file → state persists across tool calls",
            "run_command → expected exit-code-3 and stderr path",
            "list_directory → persistent workspace is visible",
        ]

        def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
            returns = [
                part.content
                for message in messages
                for part in message.parts
                if part.part_kind == "tool-return"
            ]
            if returns:
                log.info("ISLO RESULT  %s", str(returns[-1]).strip().replace("\n", " | "))
                time.sleep(0.8)
            if len(returns) < len(steps):
                tool_name, args = steps[len(returns)]
                log.info("ISLO STEP %s/%s  %s", len(returns) + 1, len(steps), labels[len(returns)])
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name=tool_name,
                            args=args,
                            tool_call_id=f"islo-step-{len(returns) + 1}",
                        )
                    ]
                )
            return ModelResponse(parts=[TextPart(content="Islo sandbox boundary passed")])

        agent = Agent(
            FunctionModel(model_function),
            instructions="Run the deterministic Islo sandbox boundary demo.",
            toolsets=[
                SandboxToolset(
                    IsloSandboxBackend(),
                    spec=SandboxSpec(
                        env={"SPEC_MARKER": "forwarded-at-create"},
                        block_network=True,
                    ),
                )
            ],
        )
        result = agent.run_sync("Exercise every Islo-backed sandbox tool.")
        log.info("ISLO SUCCESS  %s; sandbox destroyed", result.output)
        return result.output

    run_sandbox_agent()


dag = example_sandbox_toolset_islo_ui()
