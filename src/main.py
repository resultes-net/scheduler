import asyncio as _asyncio
import datetime as _dt
import logging as _log
import os as _os
import pprint as _pprint
import signal as _sig
import socket as _soc

import aiohttp as _ahttp
import resultes_pydantic_models.simulations.simulation as _psim

import runner_client as _rc
import server_client as _sc

SHUTDOWN_TIMEOUT_SECONDS = 5 * 60
PERIOD_SECONDS = 10.0
PERIOD = _dt.timedelta(seconds=PERIOD_SECONDS)

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"

_is_shutting_down = False


def on_sigterm(signal, stack_frame) -> None:
    global _is_shutting_down
    _log.info("Received SIGTERM. Shutting down.")
    _is_shutting_down = True


_sig.signal(_sig.SIGTERM, on_sigterm)


async def loop(
    server_client: _sc.ServerClient, runner_client: _rc.RunnerClient
) -> None:
    _log.info("Scheduler started.")

    tasks = set[_asyncio.Task[None]]()

    next_wakeup_time = _dt.datetime.now() + PERIOD

    while not _is_shutting_down:
        simulations_by_user_id = (
            await server_client.get_simulations_waiting_for_variations_creation_by_user_id()
        )

        data = _pprint.pformat(simulations_by_user_id, indent=4)

        _log.info(
            "Found the following simulations for which to create variations: %s\n", data
        )

        tasks = {t for t in tasks if not t.done()}

        for _, simulations in simulations_by_user_id.items():
            for simulation in simulations:
                coroutine = create_variations(runner_client, simulation)
                task = _asyncio.create_task(coroutine)
                tasks.add(task)

        await _sleep_until(next_wakeup_time)

        next_wakeup_time += PERIOD

    _log.info("Shutting down: main loop exited. Waiting for tasks to finish...")

    _, incomplete_tasks = await _asyncio.wait(tasks, timeout=SHUTDOWN_TIMEOUT_SECONDS)

    if incomplete_tasks:
        _log.warning(
            "%d task(s) did not finish within %d seconds.",
            len(incomplete_tasks),
            SHUTDOWN_TIMEOUT_SECONDS,
        )

    _log.info("...DONE.")


async def create_variations(
    runner_client: _rc.RunnerClient, simulation: _psim.Simulation
) -> None:
    variation_ids = await runner_client.create_variations(simulation.parameters)

    formatted_variation_ids = ", ".join(variation_ids)
    _log.info(
        "The following variations have been created: %s.",
        formatted_variation_ids,
    )


async def _sleep_until(wakeup_time: _dt.datetime) -> None:
    now = _dt.datetime.now()
    if wakeup_time < now:
        _log.warning(
            "Wake up time %s is in the past (now = %s). Resetting to 10 seconds from now.",
            wakeup_time,
            now,
        )
        wakeup_time = now + PERIOD

    seconds_to_sleep = (wakeup_time - now).seconds

    await _asyncio.sleep(seconds_to_sleep)


async def main() -> None:
    server_host = _os.environ.get("SERVER_HOST", "localhost")
    server_port = int(_os.environ.get("SERVER_PORT", "8000"))
    server_base_uri = f"ws://{server_host}:{server_port}/"

    runner_host_dev = f"{_soc.gethostname()}.local"
    runner_host = _os.environ.get("RUNNER_HOST", runner_host_dev)
    runner_port = int(_os.environ.get("RUNNER_PORT", "3000"))
    runner_base_uri = f"ws://{runner_host}:{runner_port}/"

    async with _ahttp.ClientSession(server_base_uri) as server_session:
        async with _ahttp.ClientSession(runner_base_uri) as runner_session:
            server_client = _sc.ServerClient(server_session)
            runner_client = _rc.RunnerClient(prefix="api", session=runner_session)

            await loop(server_client, runner_client)


if __name__ == "__main__":
    _log.basicConfig(format=LOG_FORMAT, level=_log.INFO)
    _log.info("Starting scheduler...")

    _asyncio.run(main())
