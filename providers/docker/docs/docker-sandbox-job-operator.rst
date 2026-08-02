.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Docker Sandbox job operator
===========================

``DockerSandboxJobOperator`` runs one author-declared job through Docker
Sandboxes and the ``sbx`` CLI. It is the task-scoped analogue of
``DockerOperator`` or ``KubernetesPodOperator``: the external sandbox job is
the Airflow task's workload.

It is intentionally different from ``DockerSandboxExecutor``. The operator is
run by an existing Airflow executor, and the sandbox image does not need to
contain Airflow or the Task SDK. A sandbox-backed executor instead places the
complete Airflow ``ExecuteTask`` workload inside the sandbox.

Example
-------

.. code-block:: python

   from airflow.providers.docker.sandbox.operators import DockerSandboxJobOperator

   build = DockerSandboxJobOperator(
       task_id="build",
       template="ubuntu:24.04",
       command=["bash", "-lc", "./build.sh"],
       scratch_root="/var/lib/airflow/docker-sandbox-jobs",
       cpus=8,
       memory="16g",
       timeout_seconds=3600,
       deferrable=True,
   )

Lifecycle
---------

The worker launches the sandbox using a deterministic request ID derived from
the Airflow task attempt. The operator then defers with a serialized stable
sandbox handle. The trigger polls the local Docker Sandboxes daemon and emits
the handle again with the terminal event. This allows ``execute_complete`` to
reconstruct the exact sandbox identity even when the worker process that
launched the job no longer exists.

The operator removes the sandbox after success, failure, timeout or task
cancellation unless ``keep=True`` is explicitly configured.

Local-only positioning
----------------------

Docker Sandboxes is used here as a development and conformance backend. The
operator requires the triggerer and worker to reach the same ``sbx`` daemon and
the same protected scratch root. It does not currently claim production
multi-tenant guarantees, provider-enforced hard TTL, or trusted remote result
attestation.

Initial end-to-end matrix
-------------------------

The implementation is not complete until the following cases run against a
real local Airflow stack and Docker Sandboxes daemon:

.. list-table::
   :header-rows: 1

   * - Scenario
     - Expected result
   * - Successful command
     - Task succeeds, terminal result is returned, sandbox is removed
   * - Non-zero exit
     - Task fails with the external exit code and sandbox is removed
   * - Command timeout
     - Supervisor reports timeout, task fails, sandbox is removed
   * - Task cleared before launch acceptance
     - Launch is fenced or the exact sandbox is terminated
   * - Task cleared while running
     - ``on_kill`` terminates the exact stable sandbox identity
   * - Worker restart after launch
     - Serialized trigger handle remains sufficient to finish and clean up
   * - Triggerer restart
     - Trigger serialization reconnects to the same sandbox
   * - Scheduler restart
     - Deferred task remains resumable without duplicate launch
   * - Duplicate trigger delivery
     - Completion and cleanup remain idempotent
   * - Sandbox disappears
     - Task fails closed with a ``gone`` result
   * - Stable name reused with another sandbox ID
     - Driver rejects the identity mismatch
   * - Cleanup failure
     - Task records the cleanup error and the E2E leak check detects the resource
   * - Parallel mapped tasks
     - Deterministic request IDs do not collide
   * - Repeated test suite
     - Zero active sandbox leaks after the configured grace period

Follow-up scope
---------------

File upload/download, artifact declarations, reconnectable incremental logs,
and a provider-enforced TTL are intentionally follow-up work. Those semantics
should be proven by both this local backend and at least one production-oriented
remote backend before extraction into a stable common sandbox job API.
