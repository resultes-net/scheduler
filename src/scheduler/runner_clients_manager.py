import asyncio as _asyncio
import logging as _log
import datetime as _dt
import collections.abc as _cabc

import aiohttp as _ahttp

import scheduler.config as _config
import scheduler.runnable_job_base as _jb
import scheduler.runner.client as _rc
import scheduler.runner.client_wrapper as _rcw
import scheduler.runner.manager as _run
import scheduler.runner.paths as _rp
import scheduler.scheduling.runners as _srun

_LOGGER = _log.getLogger(__name__)


class RunnerClientsManager:
    def __init__(
        self,
        runner_manager: _run.AbstractRunnerManager,
        paths: _rp.Paths,
    ) -> None:
        self._runner_manager = runner_manager
        self._paths = paths

        self._runner_client_wrappers_by_ip_address = dict[
            str, _rcw.RunnerClientWrapper
        ]()
        self._runners_scheduler = _srun.RunnersScheduler()

        self._lastest_runner_created_on: _dt.datetime | None = None

    def n_runners(self) -> int:
        return len(self._runner_client_wrappers_by_ip_address)

    def latest_runner_created_on(self) -> _dt.datetime | None:
        return self._lastest_runner_created_on

    def get_n_jobs(self, user_id: str) -> int:
        return self._runners_scheduler.get_n_jobs(user_id)

    def have_all_runners_max_jobs(self) -> bool:
        return self._runners_scheduler.have_all_runners_max_jobs()

    async def create_new_runner(self) -> None:
        _LOGGER.info("Creating new runner...")
        ip_address = await _asyncio.to_thread(
            self._runner_manager.create_server_and_get_ip
        )
        _LOGGER.info("...DONE. New runner with IP address %s created.", ip_address)
        runner_client_wrapper = await self._create_client_wrapper(ip_address)

        self._lastest_runner_created_on = _dt.datetime.now()

        await self._set_options(runner_client_wrapper.client)

        self._runner_client_wrappers_by_ip_address[ip_address] = runner_client_wrapper

        self._runners_scheduler.add_runner(
            ip_address, self._runner_manager.n_max_jobs_per_runner
        )

    async def _set_options(self, runner_client: _rc.RunnerClient) -> None:
        info = _log.getLevelName(_log.INFO)

        runner_log_level = _config.runner_log_level()
        if runner_log_level != info:
            _config.log_runner_log_level_not_info_explanation()

        runner_shall_remove_completed_jobs = (
            _config.runner_shall_remove_completed_jobs()
        )
        if not runner_shall_remove_completed_jobs:
            _config.log_runner_shall_not_remove_completed_jobs_explanation()

        await runner_client.set_options(
            runner_log_level, runner_shall_remove_completed_jobs
        )

    async def _create_client_wrapper(self, ip_address: str) -> _rcw.RunnerClientWrapper:
        _LOGGER.info("Trying to connect to runner %s...", ip_address)

        seconds_to_sleep = 5.0
        timeout = 120.0
        try:
            async with _asyncio.timeout(timeout):
                while True:
                    try:
                        client_wrapper = await _rcw.RunnerClientWrapper.create(
                            ip_address,
                            self._paths,
                        )
                        _LOGGER.info(
                            "...DONE trying to connect to runner %s.", ip_address
                        )
                        return client_wrapper
                    except _ahttp.ClientConnectionError:
                        _LOGGER.info(
                            "...FAILED. Trying again in %f second(s).", seconds_to_sleep
                        )

                    await _asyncio.sleep(seconds_to_sleep)
        except TimeoutError:
            _LOGGER.error(
                "...TIMED OUT trying to connect to runner %s after %f second(s).",
                ip_address,
                timeout,
            )
            raise

    def assign_job_and_get_handling_runner_client(
        self, job: _jb.RunnableJobBase
    ) -> _rc.RunnerClient:
        runner_ip_address = (
            self._runners_scheduler.assign_job_and_get_runner_ip_address(
                job_id=job.id, user_id=job.user_id
            )
        )
        runner_client_wrapper = self._runner_client_wrappers_by_ip_address[
            runner_ip_address
        ]
        runner_client = runner_client_wrapper.client

        _LOGGER.info("Job %s will be run on runner %s.", job.id, runner_ip_address)

        return runner_client

    def remove_completed_job(self, job_id: str) -> None:
        self._runners_scheduler.remove_completed_job(job_id)

    async def remove_any_uneeded_runners(self, shall_keep_one_free_job: bool) -> None:
        if _config.keep_runners_alive():
            _config.log_keep_runners_alive_explanation()
            return

        ip_addresses_of_idle_runners_to_remove = (
            self._get_ip_addresses_of_idle_runners_to_remove(shall_keep_one_free_job)
        )

        for (
            ip_address_of_idle_runner_to_remove
        ) in ip_addresses_of_idle_runners_to_remove:
            _LOGGER.info("Removing runner %s.", ip_address_of_idle_runner_to_remove)
            self._runners_scheduler.remove_runner(ip_address_of_idle_runner_to_remove)
            wrapper = self._runner_client_wrappers_by_ip_address.pop(
                ip_address_of_idle_runner_to_remove
            )
            await wrapper.shut_down()
            self._runner_manager.delete_servers(ip_address_of_idle_runner_to_remove)

    def _get_ip_addresses_of_idle_runners_to_remove(
        self, shall_keep_one_free_job: bool
    ) -> _cabc.Sequence[str]:
        runners = self._runners_scheduler.get_runners()
        idle_runners = [r for r in runners if r.is_idle()]

        if not idle_runners:
            return []

        n_free_jobs_on_non_idle_runners = sum(
            r.n_free_jobs() for r in runners if not r.is_idle()
        )

        shall_keep_one_idle_runner = (
            shall_keep_one_free_job and n_free_jobs_on_non_idle_runners == 0
        )

        if shall_keep_one_idle_runner:
            idle_runner_to_keep = idle_runners[0]

            _LOGGER.info(
                "Not removing runner with IP %s because we were asked to keep on free job.",
                idle_runner_to_keep.ip_address,
            )

        idle_runners_to_remove = (
            idle_runners[1:] if shall_keep_one_idle_runner else idle_runners
        )

        ip_addresses = [r.ip_address for r in idle_runners_to_remove]

        return ip_addresses

    async def shut_down(self) -> None:
        for (
            runner_client_wrapper
        ) in self._runner_client_wrappers_by_ip_address.values():
            await runner_client_wrapper.shut_down()
