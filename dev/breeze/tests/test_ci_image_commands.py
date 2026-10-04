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

import json
import os
import subprocess
from unittest import mock

import pytest

from airflow_breeze.commands.ci_image_commands import (
    build_ci_image_if_needed,
    confirm_build_if_sources_changed,
    get_ci_image_sources_hash_label,
    import_mount_cache,
    is_ci_image_built_from_current_sources,
    save,
)
from airflow_breeze.global_constants import CI_IMAGE_SOURCES_HASH_LABEL
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.utils.md5_build_check import calculate_ci_sources_hash

CI_IMAGE = "ghcr.io/apache/airflow/main/ci/python3.10"


@pytest.mark.parametrize(
    ("compress", "failure"), [(True, None), (True, "docker"), (True, "zstd"), (False, None)]
)
@mock.patch("airflow_breeze.commands.ci_image_commands.perform_environment_checks", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.run_command", autospec=True)
def test_save_image(mock_run_command, mock_environment_checks, tmp_path, monkeypatch, compress, failure):
    tools = tmp_path / "bin"
    tools.mkdir()
    docker = tools / "docker"
    docker.write_text(
        '#!/bin/bash\n[[ "$1" == buildx ]] && exit 0\n'
        '[[ "$3" == -o ]] && { printf "image stream" > "$4"; exit 0; }\n'
        'printf "image stream"\n[[ "$FAILURE" == docker ]] && exit 7\nexit 0\n'
    )
    zstd = tools / "zstd"
    zstd.write_text('#!/bin/bash\ncat > "${@: -1}"\n[[ "$FAILURE" == zstd ]] && exit 9\nexit 0\n')
    docker.chmod(0o755)
    zstd.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tools}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAILURE", failure or "")
    mock_run_command.side_effect = lambda command, check=False, **kwargs: subprocess.run(
        command, capture_output=True, text=True, check=check, **kwargs
    )
    # Positional shell arguments must preserve spaces and shell metacharacters literally.
    destination = tmp_path / "image $(false) ' name.tar"
    kwargs = dict(
        python="3.10",
        platform="linux/amd64",
        github_repository="apache/airflow",
        image_file=destination,
        image_file_dir=tmp_path,
        compress=compress,
    )
    if failure:
        with pytest.raises(SystemExit) as exc:
            save.callback(**kwargs)
        assert exc.value.code == (7 if failure == "docker" else 9)
        assert not destination.exists()
    else:
        save.callback(**kwargs)
        assert destination.read_bytes() == b"image stream"


@pytest.mark.parametrize("compress", [False, True])
@mock.patch("airflow_breeze.commands.ci_image_commands.perform_environment_checks", autospec=True)
@mock.patch("airflow_breeze.utils.run_utils.get_dry_run", autospec=True, return_value=True)
def test_save_image_dry_run(mock_dry_run, mock_environment_checks, tmp_path, compress):
    destination = tmp_path / "existing.tar"
    destination.write_bytes(b"existing image")
    save.callback(
        python="3.10",
        platform="linux/amd64",
        github_repository="apache/airflow",
        image_file=destination,
        image_file_dir=tmp_path,
        compress=compress,
    )
    assert destination.read_bytes() == b"existing image"


def test_calculate_ci_sources_hash_is_stable_across_checkouts(tmp_path, monkeypatch):
    watched_files = ["Dockerfile.ci", "scripts/docker/common.sh"]
    for checkout in ("worktree-a", "worktree-b"):
        root = tmp_path / checkout
        (root / "scripts" / "docker").mkdir(parents=True)
        (root / "Dockerfile.ci").write_text("FROM base")
        (root / "scripts" / "docker" / "common.sh").write_text("echo common")
    monkeypatch.setattr("airflow_breeze.utils.md5_build_check.FILES_FOR_REBUILD_CHECK", watched_files)
    monkeypatch.setattr("airflow_breeze.utils.md5_build_check.AIRFLOW_ROOT_PATH", tmp_path / "worktree-a")
    hash_of_first_checkout = calculate_ci_sources_hash()
    monkeypatch.setattr("airflow_breeze.utils.md5_build_check.AIRFLOW_ROOT_PATH", tmp_path / "worktree-b")
    assert calculate_ci_sources_hash() == hash_of_first_checkout
    (tmp_path / "worktree-b" / "Dockerfile.ci").write_text("FROM other")
    assert calculate_ci_sources_hash() != hash_of_first_checkout


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    [
        pytest.param(0, json.dumps({CI_IMAGE_SOURCES_HASH_LABEL: "abc"}), "abc", id="label-present"),
        pytest.param(0, json.dumps({"other-label": "abc"}), None, id="label-absent"),
        pytest.param(0, "null", None, id="no-labels-at-all"),
        pytest.param(0, "", None, id="empty-output"),
        pytest.param(0, "not-json", None, id="invalid-json"),
        pytest.param(1, "", None, id="image-missing"),
    ],
)
@mock.patch("airflow_breeze.commands.ci_image_commands.run_command")
def test_get_ci_image_sources_hash_label(mock_run_command, returncode, stdout, expected):
    mock_run_command.return_value = mock.MagicMock(returncode=returncode, stdout=stdout)
    assert get_ci_image_sources_hash_label(CI_IMAGE) == expected


@pytest.mark.parametrize(
    ("image_hash", "current_hash", "expected"),
    [
        pytest.param("abc", "abc", True, id="match"),
        pytest.param("abc", "def", False, id="mismatch"),
        pytest.param(None, "abc", False, id="no-label"),
    ],
)
@mock.patch("airflow_breeze.commands.ci_image_commands.calculate_ci_sources_hash")
@mock.patch("airflow_breeze.commands.ci_image_commands.get_ci_image_sources_hash_label")
def test_is_ci_image_built_from_current_sources(
    mock_get_ci_image_sources_hash_label,
    mock_calculate_ci_sources_hash,
    image_hash,
    current_hash,
    expected,
):
    mock_get_ci_image_sources_hash_label.return_value = image_hash
    mock_calculate_ci_sources_hash.return_value = current_hash
    assert is_ci_image_built_from_current_sources(BuildCiParams()) is expected


@mock.patch("airflow_breeze.commands.ci_image_commands.mark_image_as_rebuilt")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_confirm_build_if_sources_changed_skips_build_when_image_matches_current_sources(
    mock_is_ci_image_built_from_current_sources, mock_mark_image_as_rebuilt
):
    mock_is_ci_image_built_from_current_sources.return_value = True
    build_ci_params = BuildCiParams()
    assert confirm_build_if_sources_changed(build_ci_params) is False
    mock_mark_image_as_rebuilt.assert_called_once_with(ci_image_params=build_ci_params)


@mock.patch("airflow_breeze.commands.ci_image_commands.md5sum_check_if_build_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.mark_image_as_rebuilt")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_confirm_build_if_sources_changed_falls_back_to_md5_check_when_image_does_not_match(
    mock_is_ci_image_built_from_current_sources,
    mock_mark_image_as_rebuilt,
    mock_md5sum_check_if_build_is_needed,
):
    mock_is_ci_image_built_from_current_sources.return_value = False
    mock_md5sum_check_if_build_is_needed.return_value = False
    assert confirm_build_if_sources_changed(BuildCiParams()) is False
    mock_mark_image_as_rebuilt.assert_not_called()
    mock_md5sum_check_if_build_is_needed.assert_called_once()


@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image")
@mock.patch("airflow_breeze.commands.ci_image_commands.mark_image_as_rebuilt")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_reuses_image_built_in_another_checkout(
    mock_is_ci_image_built_from_current_sources,
    mock_mark_image_as_rebuilt,
    mock_run_build_ci_image,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    mock_is_ci_image_built_from_current_sources.return_value = True
    build_ci_image_if_needed(command_params=BuildCiParams())
    mock_mark_image_as_rebuilt.assert_called_once()
    mock_run_build_ci_image.assert_not_called()


@mock.patch("airflow_breeze.commands.ci_image_commands.check_if_image_building_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_forces_build_when_image_does_not_match_sources(
    mock_is_ci_image_built_from_current_sources,
    mock_run_build_ci_image,
    mock_check_if_image_building_is_needed,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    mock_is_ci_image_built_from_current_sources.return_value = False
    mock_check_if_image_building_is_needed.return_value = True
    mock_run_build_ci_image.return_value = (0, "built")
    build_ci_image_if_needed(command_params=BuildCiParams())
    assert mock_check_if_image_building_is_needed.call_args.kwargs["ci_image_params"].force_build is True
    mock_run_build_ci_image.assert_called_once()


@mock.patch("airflow_breeze.commands.ci_image_commands.check_if_image_building_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_does_not_reuse_image_when_force_build_requested(
    mock_is_ci_image_built_from_current_sources,
    mock_run_build_ci_image,
    mock_check_if_image_building_is_needed,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    mock_check_if_image_building_is_needed.return_value = True
    mock_run_build_ci_image.return_value = (0, "built")
    build_ci_image_if_needed(command_params=BuildCiParams(force_build=True))
    mock_is_ci_image_built_from_current_sources.assert_not_called()
    mock_run_build_ci_image.assert_called_once()


@mock.patch("airflow_breeze.commands.ci_image_commands.check_if_image_building_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_does_not_query_docker_when_marker_present(
    mock_is_ci_image_built_from_current_sources,
    mock_check_if_image_building_is_needed,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    command_params = BuildCiParams()
    marker = tmp_path / command_params.airflow_branch / f".built_{command_params.python}"
    marker.parent.mkdir(parents=True)
    marker.touch()
    mock_check_if_image_building_is_needed.return_value = False
    build_ci_image_if_needed(command_params=command_params)
    mock_is_ci_image_built_from_current_sources.assert_not_called()


@mock.patch("airflow_breeze.commands.ci_image_commands.run_command", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.make_sure_builder_configured", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.perform_environment_checks", autospec=True)
def test_import_mount_cache_does_not_prune_the_imported_cache_mount(
    mock_perform_environment_checks,
    mock_make_sure_builder_configured,
    mock_run_command,
    tmp_path,
):
    cache_file = tmp_path / "ci-cache-mount-save-v3-3.10.tar.gz"
    cache_file.write_bytes(b"")
    import_mount_cache.callback(builder="autodetect", cache_file=cache_file)
    commands = [call.args[0] for call in mock_run_command.call_args_list]
    assert commands[-2:] == [
        ["docker", "rmi", "airflow-import-cache"],
        ["docker", "image", "prune", "-f"],
    ]
