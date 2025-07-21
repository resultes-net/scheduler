import os as _os

import pytest as _pt

import clouds_yaml as _cyaml
import runner_manager as _run


@_pt.mark.asyncio
async def test_create_server() -> None:
    runner_manager = _create_manager()
    ip_address = await runner_manager.create_server_and_get_ip()
    print(ip_address)


def _create_manager():
    os_password = _os.environ["OS_PASSWORD"]
    runner_manager = _run.RunnerManager(os_password, _cyaml.clouds_yaml_file_path)
    return runner_manager


def test_delete_servers() -> None:
    runner_manager = _create_manager()
    runner_manager.delete_servers()
