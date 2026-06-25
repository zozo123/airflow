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
"""Example: an agent whose generated code runs in an isolated sandbox.

The agent decides to write Python and calls the ``run_code`` tool from
:class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`, which
executes that code in a real cloud sandbox (here islo) — off the Airflow worker.
Swap ``provider="local"`` to run with no credentials.
"""

from __future__ import annotations

import datetime

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets import SandboxToolset
from airflow.sdk import DAG

with DAG(
    dag_id="example_agent_sandbox",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "ai", "sandbox"],
) as dag:
    # [START howto_agent_sandbox]
    fib_in_sandbox = AgentOperator(
        task_id="fib_in_sandbox",
        prompt=(
            "Compute the 30th Fibonacci number (F(1)=1, F(2)=1) by running Python "
            "with the run_code tool. Reply with only the number."
        ),
        llm_conn_id="openai_default",  # any PydanticAI (conn_type=pydanticai) connection
        toolsets=[
            SandboxToolset(
                provider="islo",  # or local/daytona/e2b/modal, or "module:Class"
                image="python:3.12-slim",
            )
        ],
    )
    # [END howto_agent_sandbox]


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
