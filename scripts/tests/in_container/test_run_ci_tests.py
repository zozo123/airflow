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

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "in_container" / "run_ci_tests.sh"


@pytest.fixture
def run_ci_tests(tmp_path):
    runner = tmp_path / "run_ci_tests.sh"
    shutil.copyfile(SCRIPT_PATH, runner)
    # Isolate container initialization while executing the real pytest result handling.
    (tmp_path / "_in_container_script_init.sh").write_text(
        'dump_airflow_logs() { echo "test logs dumped"; }\n'
    )
    pytest_stub = tmp_path / "pytest"
    pytest_stub.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$PYTEST_ARGS_FILE"\nexit "$PYTEST_EXIT_CODE"\n')
    pytest_stub.chmod(0o755)
    args_file = tmp_path / "pytest_args"

    def run(exit_code, group, args):
        result = subprocess.run(
            ["bash", str(runner), *args],
            env={
                "PATH": f"{tmp_path}:/usr/bin:/bin",
                "CI": "true",
                "TEST_GROUP": group,
                "PYTEST_EXIT_CODE": str(exit_code),
                "PYTEST_ARGS_FILE": str(args_file),
                "RESULT_LOG_FILE": str(tmp_path / "missing.xml"),
            },
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if args:
            assert args_file.read_text().splitlines() == args
        return result

    return run


@pytest.mark.parametrize(
    ("group", "args", "expected"),
    [
        pytest.param("providers", ["--run-db-tests-only"], 0, id="empty-provider-db-selection"),
        pytest.param("providers", ["tests/with spaces", "--run-db-tests-only"], 0, id="flag-after-path"),
        pytest.param("providers", [], 5, id="unfiltered-providers"),
        pytest.param("providers", ["--skip-db-tests"], 5, id="non-db-providers"),
        pytest.param("core", ["--run-db-tests-only"], 5, id="core-db-selection"),
        pytest.param("", ["--run-db-tests-only"], 5, id="missing-test-group"),
        pytest.param("providers", ["--", "--run-db-tests-only"], 5, id="flag-after-option-separator"),
        pytest.param("providers", ["test_--run-db-tests-only.py"], 5, id="flag-substring-is-not-option"),
    ],
)
def test_no_tests_is_accepted_only_for_provider_db_selection(run_ci_tests, group, args, expected):
    result = run_ci_tests(5, group, args)
    assert result.returncode == expected, result.stdout + result.stderr
    assert ("No DB tests were collected for providers" in result.stdout) == (expected == 0)
    assert ("test logs dumped" in result.stdout) == (expected != 0)


@pytest.mark.parametrize("exit_code", [0, 1, 2, 3, 4, 124, 137, 139, 143])
def test_provider_db_selection_preserves_other_pytest_results(run_ci_tests, exit_code):
    result = run_ci_tests(exit_code, "providers", ["--run-db-tests-only"])
    assert result.returncode == exit_code, result.stdout + result.stderr
    assert "No DB tests were collected" not in result.stdout
