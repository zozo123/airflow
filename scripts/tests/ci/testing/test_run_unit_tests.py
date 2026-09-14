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

import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[4] / "scripts" / "ci" / "testing" / "run_unit_tests.sh"

# Deliberately excludes the directory holding breeze, so a regression that lets the script run on
# past the guard cannot start a real test run - it fails to find breeze instead.
MINIMAL_ENVIRONMENT = {"PATH": "/usr/bin:/bin"}


def test_run_unit_tests_aborts_when_the_job_budget_is_missing_in_github_actions():
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "core", "DB"],
        env={**MINIMAL_ENVIRONMENT, "GITHUB_ACTIONS": "true"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "JOB_START_EPOCH and JOB_TIMEOUT_MINUTES must both be set" in result.stdout


@pytest.mark.parametrize("scope", ["DB", "Non-DB", "All"])
@pytest.mark.parametrize("exit_code", [0, 1, 2, 5, 124, 137, 143])
def test_provider_tests_preserve_breeze_exit_status(tmp_path, scope, exit_code):
    breeze = tmp_path / "breeze"
    breeze.write_text('#!/bin/sh\nexit "$BREEZE_EXIT_CODE"\n')
    breeze.chmod(0o755)
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "providers", scope],
        env={"PATH": f"{tmp_path}:/usr/bin:/bin", "BREEZE_EXIT_CODE": str(exit_code)},
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == exit_code, result.stdout + result.stderr
    assert ("Providers tests completed successfully" in result.stdout) == (exit_code == 0)


def test_quarantined_provider_failures_remain_non_blocking(tmp_path):
    breeze = tmp_path / "breeze"
    breeze.write_text('#!/bin/sh\nexit 1\n')
    breeze.chmod(0o755)
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), "providers", "Quarantined"],
        env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
