import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import datetime as _dt
import logging as _log
import os as _os
import pathlib as _pl
import pprint as _pprint
import signal as _sig
import socket as _soc
import typing as _tp

import aiohttp as _ahttp
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar

import clouds_yaml as _cyaml
import runner_client as _rc
import runner_manager as _run
import scheduling.runners as _sr
import scheduling.users as _usr
import server_client as _sc

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(taskName)s - %(module)s - %(message)s"

_is_shutting_down = False


def on_sigterm(signal, stack_frame) -> None:
    global _is_shutting_down
    _log.info("Received SIGTERM. Shutting down.")
    _is_shutting_down = True


_sig.signal(_sig.SIGTERM, on_sigterm)


class TerminateTaskGroup(Exception):
    pass


async def terminate_task_group() -> _tp.NoReturn:
    raise TerminateTaskGroup()


class _RunnerClientWrapper:
    def __init__(
        self,
        client_session: _ahttp.ClientSession,
        websocket: _ahttp.ClientWebSocketResponse,
        runner_client: _rc.RunnerClient,
    ) -> None:
        self._session = client_session
        self._websocket = websocket
        self._client: _rc.RunnerClient | None = runner_client

    @staticmethod
    async def create(ip_address: str) -> "_RunnerClientWrapper":
        base_uri = f"http://{ip_address}:3000/"

        session = _ahttp.ClientSession(base_uri)
        try:
            websocket = await session.ws_connect("/")
        except:
            await session.close()
            raise

        client = _rc.RunnerClient(websocket)
        client.start()
        return _RunnerClientWrapper(session, websocket, client)

    @property
    def client(self) -> _rc.RunnerClient:
        if not self._client:
            raise RuntimeError("Client not shut down.")

        return self._client

    async def shut_down(self) -> None:
        self.client.stop()
        await self._websocket.close()
        await self._session.close()


class Looper(_ctx.AbstractAsyncContextManager["Looper"]):
    def __init__(
        self,
        server_client: _sc.ServerClient,
        runner_manager: _run.AbstractRunnerManager,
    ) -> None:
        self._server_client = server_client
        self._runner_manager = runner_manager
        self._runner_client_wrappers_by_ip_address = dict[str, _RunnerClientWrapper]()
        self._runners = _sr.RunnersScheduler()
        self._lock = _asyncio.Lock()

    async def __aenter__(self) -> _tp.Self:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        for (
            runner_client_wrapper
        ) in self._runner_client_wrappers_by_ip_address.values():
            await runner_client_wrapper.shut_down()

        return False

    async def loop(self, polling_period_seconds: int) -> None:
        _log.info("Scheduler started.")

        period = _dt.timedelta(seconds=polling_period_seconds)

        next_wakeup_time = _dt.datetime.now() + period

        try:
            async with _asyncio.TaskGroup() as task_group:
                while not _is_shutting_down:
                    simulations_by_user_id = (
                        await self._server_client.get_simulations_waiting_for_variations_creation_by_user_id()
                    )

                    data = _pprint.pformat(simulations_by_user_id, indent=4)

                    _log.info(
                        "Found the following simulations for which to create variations: %s\n",
                        data,
                    )

                    users_scheduler = self._create_users_scheduler(
                        simulations_by_user_id
                    )

                    while users_scheduler.has_next_simulation():
                        next_simulation = users_scheduler.pop_next_simulation()

                        assert next_simulation.id

                        await self._server_client.set_simulation_state(
                            next_simulation.id,
                            _psim.SimulationState.CREATING_VARIATIONS,
                        )

                        coroutine = self._create_variations(next_simulation)
                        task_group.create_task(coroutine)

                    await self._delete_any_idle_runners()

                    await self._sleep_until(next_wakeup_time, period)

                    next_wakeup_time += period

                _log.info("Exited main loop.")

                task_group.create_task(terminate_task_group())

        except* TerminateTaskGroup:
            pass

    def _create_users_scheduler(
        self,
        simulations_by_user_id: _cabc.Mapping[str, _cabc.Sequence[_psim.Simulation]],
    ) -> _usr.UsersScheduler:
        users = [self._create_user(k, ss) for k, ss in simulations_by_user_id.items()]
        users_scheduler = _usr.UsersScheduler(users)
        return users_scheduler

    def _create_user(
        self, user_id: str, simulations: _cabc.Sequence[_psim.Simulation]
    ) -> _usr.User:
        n_jobs = self._runners.get_n_jobs(user_id)
        return _usr.User(n_jobs, simulations)

    async def _delete_any_idle_runners(self):
        idle_runner_ip_addresses = self._runners.get_idle_runner_ip_addresses()
        for idle_runner_ip_address in idle_runner_ip_addresses:
            _log.info("Deleting server %s.", idle_runner_ip_address)
            self._runners.remove_runner(idle_runner_ip_address)
            wrapper = self._runner_client_wrappers_by_ip_address[idle_runner_ip_address]
            await wrapper.shut_down()
            self._runner_manager.delete_servers(idle_runner_ip_address)

    async def _create_variations(
        self,
        simulation: _psim.Simulation,
    ) -> None:
        simulation_id = simulation.id
        assert simulation_id

        # Ensure no other job creates a new runner at the same time.
        async with self._lock:
            if self._runners.have_all_runners_max_jobs():
                await self._create_new_runner()

        runner_client = self._start_job_and_get_handling_runner_client(
            simulation_id=simulation_id, user_id=simulation.user_id
        )

        relative_deck_file_paths = await runner_client.create_variations(
            simulation_id, simulation.parameters
        )

        if relative_deck_file_paths:
            for relative_deck_file_path in relative_deck_file_paths:
                _log.info(
                    "Creating variation for deck file %s (simulation ID = %s)...",
                    relative_deck_file_path,
                    simulation_id,
                )
                relative_deck_file_pure_path = _pl.PureWindowsPath(
                    relative_deck_file_path
                )

                variation = _pvar.CreateVariation(
                    simulation_id=simulation_id,
                    relative_deck_file_path=relative_deck_file_pure_path,
                )

                await self._server_client.create_variation(simulation_id, variation)

                _log.info("...DONE.")

        else:
            _log.info("Got empty response to request %s.", simulation_id)

        await self._server_client.set_simulation_state(
            simulation_id, _psim.SimulationState.WAITING_FOR_VARIATION_RUNS
        )

        self._runners.remove_completed_job(simulation_id)

    def _start_job_and_get_handling_runner_client(
        self, /, simulation_id: str, user_id: str
    ) -> _rc.RunnerClient:
        runner_ip_address = self._runners.start_job_and_get_handling_runner_ip_address(
            simulation_id, user_id
        )
        runner_client_wrapper = self._runner_client_wrappers_by_ip_address[
            runner_ip_address
        ]
        runner_client = runner_client_wrapper.client

        _log.info("Job %s will be run on runner %s.", simulation_id, runner_ip_address)

        return runner_client

    async def _create_new_runner(self) -> None:
        _log.info("Creating new runner...")
        ip_address = await self._runner_manager.create_server_and_get_ip()
        _log.info("...DONE. New runner with ip address %s created.", ip_address)
        runner_client_wrapper = await self._create_client_wrapper(ip_address)
        self._runner_client_wrappers_by_ip_address[ip_address] = runner_client_wrapper
        self._runners.add_runner(ip_address, self._runner_manager.n_max_jobs_per_runner)

    async def _create_client_wrapper(self, ip_address: str) -> _RunnerClientWrapper:
        _log.info("Trying to connect to runner %s...", ip_address)

        seconds_to_sleep = 5.0
        timeout = 120.0
        try:
            async with _asyncio.timeout(timeout):
                while True:
                    try:
                        client_wrapper = await _RunnerClientWrapper.create(ip_address)
                        _log.info("...DONE trying to connect to runner %s.", ip_address)
                        return client_wrapper
                    except _ahttp.ClientConnectionError:
                        _log.info(
                            "...FAILED. Trying again in %f second(s).", seconds_to_sleep
                        )

                    await _asyncio.sleep(seconds_to_sleep)
        except TimeoutError:
            _log.error(
                "...TIMED OUT trying to connect to runner %s after %f second(s).",
                ip_address,
                timeout,
            )
            raise

    @staticmethod
    async def _sleep_until(
        wakeup_time: _dt.datetime, polling_period: _dt.timedelta
    ) -> None:
        now = _dt.datetime.now()
        if wakeup_time < now:
            _log.warning(
                "Wake up time %s is in the past (now = %s). Resetting to 10 seconds from now.",
                wakeup_time,
                now,
            )
            wakeup_time = now + polling_period

        seconds_to_sleep = (wakeup_time - now).seconds

        await _asyncio.sleep(seconds_to_sleep)


async def main(
    server_base_uri: str,
    runner_manager: _run.AbstractRunnerManager,
    polling_period_seconds: int,
) -> None:
    runner_manager.delete_servers()

    try:
        async with _ahttp.ClientSession(server_base_uri) as server_session:
            server_client = _sc.ServerClient(server_session)
            async with Looper(server_client, runner_manager) as looper:
                await looper.loop(polling_period_seconds)
    finally:
        runner_manager.delete_servers()


if __name__ == "__main__":
    log_level = _os.environ.get("LOG_LEVEL", "INFO")
    _log.basicConfig(format=LOG_FORMAT, level=log_level)
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
