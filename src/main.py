import asyncio as _asyncio
import logging as _log
import os as _os
import pathlib as _pl
import signal as _sig
import subprocess as _sp

import aiohttp as _ahttp

import scheduler.clouds_yaml as _cyaml
import scheduler.config as _config
import scheduler.log_config as _clog
import scheduler.runner.manager as _run
import scheduler.runner.paths as _rp
import scheduler.server.server_client as _sc
import scheduler.workflow as _wf

if _config.run_debugger():
    import debugpy as _debugpy

_LOGGER = _log.getLogger(__name__)

_looper: _wf.Looper | None = None


def on_sigterm(signal, stack_frame) -> None:
    global _looper
    _LOGGER.info("Received SIGTERM. Shutting down.")
    if _looper:
        _looper.shut_down()


_sig.signal(_sig.SIGTERM, on_sigterm)


async def main(
    server_base_uri: str,
    runner_manager: _run.AbstractRunnerManager,
    polling_period_seconds: int,
    paths: _rp.Paths,
) -> None:
    _delete_stale_resources(runner_manager)

    try:
        async with _ahttp.ClientSession(server_base_uri) as server_session:
            server_client = _sc.ServerClient(server_session)
            async with _wf.Looper(
                server_client,
                runner_manager,
                paths,
            ) as looper:
                await looper.loop(polling_period_seconds)
    finally:
        _delete_stale_resources(runner_manager)


def _delete_stale_resources(runner_manager: _run.AbstractRunnerManager):
    if _config.keep_runners_alive():
        _config.log_keep_runners_alive_explanation()
    else:
        runner_manager.delete_all_servers_except(except_ip_addresses=[])

    runner_manager.delete_stale_disk_images()


def _run_debugger_and_wait_for_client() -> None:
    _config.log_run_debugger_explanation()
    _debugpy.listen(5678)
    _debugpy.wait_for_client()


def _configure_logging(log_level: str) -> None:
    root_logger = _log.getLogger()
    root_logger.setLevel(log_level)

    stream_handler = _log.StreamHandler()
    stream_handler.setLevel(log_level)

    formatter = _clog.Formatter()
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)


def _get_windows_host_ip_address() -> str:
    # Python translation of `bash` snippet found here:
    # https://learn.microsoft.com/en-us/windows/wsl/networking#identify-ip-address

    completed_process = _sp.run(
        "ip route show".split(), capture_output=True, text=True, check=True
    )

    routes = completed_process.stdout.splitlines()

    default_routes = [r for r in routes if "default" in r]
    assert len(default_routes) == 1
    default_route = default_routes[0]

    _, _, ip_address, *_ = default_route.split()
    return ip_address


if __name__ == "__main__":
    log_level = _os.environ.get("LOG_LEVEL", "INFO")
    _configure_logging(log_level)

    _LOGGER.info("Starting scheduler...")

    python_frozen_modules = _os.environ.get("PYTHON_FROZEN_MODULES")
    if not python_frozen_modules:
        _LOGGER.info("PYTHON_FROZEN_MODULES not set.")
    else:
        _LOGGER.info("PYTHON_FROZEN_MODULES = %s.", python_frozen_modules)

    if _config.run_debugger():
        _run_debugger_and_wait_for_client()

    server_host = _os.environ.get("SERVER_HOST", "localhost")
    server_port = int(_os.environ.get("SERVER_PORT", "8000"))
    server_base_uri = f"http://{server_host}:{server_port}/"
    _LOGGER.info("Server base URI: %s", server_base_uri)

    runner_port = int(_os.environ.get("RUNNER_PORT", "3000"))

    polling_period_seconds = int(_os.environ.get("POLLING_PERIOD_SECONDS", "3"))
    _LOGGER.info("Polling period (seconds): %i", polling_period_seconds)

    shall_use_openstack = int(_os.environ.get("USE_OPENSTACK", "0"))
    runner_manager: _run.AbstractRunnerManager
    if shall_use_openstack:
        clouds_yaml_file_path = _cyaml.clouds_yaml_file_path

        _LOGGER.info(
            "Using OpenStack runner manager with config file %s.", clouds_yaml_file_path
        )

        os_password = _os.environ["OS_PASSWORD"]

        runner_manager = _run.RunnerManager(os_password, clouds_yaml_file_path)
    else:
        _LOGGER.info("Using dummy runner manager.")
        host = _get_windows_host_ip_address()
        runner_manager = _run.DummyRunnerManager(host, n_max_jobs_per_runner=512)

    default_trnexe_path = r"create-disk-image\contents\TRNSYS18\Exe\TrnEXE.exe"

    trnexe_path = _pl.PureWindowsPath(_os.environ.get("TRNEXE", default_trnexe_path))

    default_python_exe_path = r"venv\Scripts\python.exe"

    python_exe_path = _pl.PureWindowsPath(
        _os.environ.get("PYTHON_EXE", default_python_exe_path)
    )

    paths = _rp.Paths(trnexe_path, python_exe_path)

    coroutine = main(server_base_uri, runner_manager, polling_period_seconds, paths)

    _asyncio.run(coroutine)
