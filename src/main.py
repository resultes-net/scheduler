import asyncio as _asyncio
import logging as _log
import os as _os
import signal as _sig
import socket as _soc

import aiohttp as _ahttp

import clouds_yaml as _cyaml
import config as _config
import log_config as _clog
import runner_manager as _run
import server_client as _sc
import workflow as _wf

_looper: _wf.Looper | None = None


def on_sigterm(signal, stack_frame) -> None:
    global _looper
    _log.info("Received SIGTERM. Shutting down.")
    if _looper:
        _looper.shut_down()


_sig.signal(_sig.SIGTERM, on_sigterm)


async def main(
    server_base_uri: str,
    runner_manager: _run.AbstractRunnerManager,
    polling_period_seconds: int,
) -> None:
    _delete_stale_servers(runner_manager)

    try:
        async with _ahttp.ClientSession(server_base_uri) as server_session:
            server_client = _sc.ServerClient(server_session)
            async with _wf.Looper(server_client, runner_manager) as looper:
                await looper.loop(polling_period_seconds)
    finally:
        _delete_stale_servers(runner_manager)


def _delete_stale_servers(runner_manager: _run.AbstractRunnerManager):
    if _config.keepRunnersAlive():
        _config.log_explanation()
    else:
        runner_manager.delete_servers()


if __name__ == "__main__":
    log_level = _os.environ.get("LOG_LEVEL", "INFO")
    _log.basicConfig(format=_clog.LOG_FORMAT, level=log_level)
    _log.info("Starting scheduler...")

    server_host = _os.environ.get("SERVER_HOST", "localhost")
    server_port = int(_os.environ.get("SERVER_PORT", "8000"))
    server_base_uri = f"ws://{server_host}:{server_port}/"
    _log.info("Server base URI: %s", server_base_uri)

    runner_port = int(_os.environ.get("RUNNER_PORT", "3000"))

    polling_period_seconds = int(_os.environ.get("POLLING_PERIOD_SECONDS", "3"))
    _log.info("Polling period (seconds): %i", polling_period_seconds)

    shall_use_openstack = int(_os.environ.get("USE_OPENSTACK", "0"))
    runner_manager: _run.AbstractRunnerManager
    if shall_use_openstack:
        clouds_yaml_file_path = _cyaml.clouds_yaml_file_path

        _log.info(
            "Using OpenStack runner manager with config file %s.", clouds_yaml_file_path
        )

        os_password = _os.environ["OS_PASSWORD"]

        runner_manager = _run.RunnerManager(os_password, clouds_yaml_file_path)
    else:
        _log.info("Using dummy runner manager.")
        host = f"{_soc.gethostname()}.local"
        runner_manager = _run.DummyRunnerManager(host, n_max_jobs_per_runner=512)

    coroutine = main(server_base_uri, runner_manager, polling_period_seconds)

    _asyncio.run(coroutine)
