import asyncio as _asyncio
import logging as _log

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

    def n_runners(self) -> int:
        return len(self._runner_client_wrappers_by_ip_address)

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
        self._runner_client_wrappers_by_ip_address[ip_address] = runner_client_wrapper

        self._runners_scheduler.add_runner(
            ip_address, self._runner_manager.n_max_jobs_per_runner
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

    async def delete_any_idle_runners(self) -> None:
        if _config.keepRunnersAlive():
            _config.log_keep_runners_alive_explanation()
            return

        idle_runner_ip_addresses = (
            self._runners_scheduler.get_idle_runner_ip_addresses()
        )
        for idle_runner_ip_address in idle_runner_ip_addresses:
            _LOGGER.info("Deleting server %s.", idle_runner_ip_address)
            self._runners_scheduler.remove_runner(idle_runner_ip_address)
            wrapper = self._runner_client_wrappers_by_ip_address[idle_runner_ip_address]
            await wrapper.shut_down()
            self._runner_manager.delete_servers(idle_runner_ip_address)

    async def shut_down(self) -> None:
        for (
            runner_client_wrapper
        ) in self._runner_client_wrappers_by_ip_address.values():
            await runner_client_wrapper.shut_down()
