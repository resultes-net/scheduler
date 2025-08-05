import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import datetime as _dt
import itertools as _it
import logging as _log
import pprint as _pprint
import typing as _tp

import scheduler.jobs.create_variations as _sjcv
import scheduler.runnable_job_base as _jb
import scheduler.runner.client as _rc
import scheduler.runner.manager as _run
import scheduler.runner_clients_manager as _rcm
import scheduler.scheduling.jobs as _susr
import scheduler.server.server_client as _sc

_LOGGER = _log.getLogger(__name__)


class TerminateTaskGroup(Exception):
    pass


async def terminate_task_group() -> _tp.NoReturn:
    raise TerminateTaskGroup()


class Looper(_ctx.AbstractAsyncContextManager["Looper"]):
    def __init__(
        self,
        server_client: _sc.ServerClient,
        runner_manager: _run.AbstractRunnerManager,
    ) -> None:
        self._server_client = server_client
        self._runner_clients_manager = _rcm.RunnerClientsManager(runner_manager)

        self._is_shutting_down = False

    async def __aenter__(self) -> _tp.Self:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        await self._runner_clients_manager.shut_down()
        return False

    def shut_down(self) -> None:
        if self._is_shutting_down:
            raise RuntimeError("Already shutting down.")

        self._is_shutting_down = True

    async def loop(self, polling_period_seconds: int) -> None:
        if self._is_shutting_down:
            raise RuntimeError("Shut down.")

        _LOGGER.info("Scheduler started.")

        period = _dt.timedelta(seconds=polling_period_seconds)

        next_wakeup_time = _dt.datetime.now() + period

        try:
            async with _asyncio.TaskGroup() as task_group:
                while not self._is_shutting_down:
                    waiting_simulations = (
                        await self._server_client.get_simulations_waiting_for_variations_creation()
                    )

                    if waiting_simulations:
                        data = _pprint.pformat(waiting_simulations, indent=4)

                        _LOGGER.info(
                            "Found the following simulations for which to create variations: %s\n",
                            data,
                        )

                    runnable_jobs = [
                        _sjcv.CreateVariationsJob(s, self._server_client)
                        for s in waiting_simulations
                    ]

                    await self._process_jobs(task_group, runnable_jobs)

                    await self._runner_clients_manager.delete_any_idle_runners()

                    next_wakeup_time = (
                        await self._adjust_wakeup_time_if_needed_and_sleep_until(
                            next_wakeup_time, period
                        )
                    )

                    next_wakeup_time += period

                _LOGGER.info("Exited main loop.")

                task_group.create_task(terminate_task_group())

        except* TerminateTaskGroup:
            pass
        except* BaseException as exception:
            _LOGGER.error("Exception occurred: %s. Terminating.", exception)
            raise

    async def _process_jobs(
        self,
        task_group: _asyncio.TaskGroup,
        runnable_jobs: _cabc.Sequence[_jb.RunnableJobBase],
    ):
        users_scheduler = self._create_users_scheduler(runnable_jobs)

        while users_scheduler.has_next_runnable_job():
            if self._runner_clients_manager.have_all_runners_max_jobs():
                await self._runner_clients_manager.create_new_runner()

            next_runnable_job = users_scheduler.pop_next_runnable_job()

            await next_runnable_job.set_started()

            runner_client = (
                self._runner_clients_manager.assign_job_and_get_handling_runner_client(
                    next_runnable_job
                )
            )

            coroutine = self._run_job_and_remove_once_completed(
                next_runnable_job, runner_client
            )

            task_name = f"Task-Job-{next_runnable_job.id}"
            task_group.create_task(coroutine, name=task_name)

    async def _run_job_and_remove_once_completed(
        self,
        runnable_job: _jb.RunnableJobBase,
        runner_client: _rc.RunnerClient,
    ) -> None:
        try:
            await runnable_job.run(runner_client)
        finally:
            self._runner_clients_manager.remove_completed_job(runnable_job.id)

    def _create_users_scheduler(
        self,
        runnable_jobs: _cabc.Sequence[_jb.RunnableJobBase],
    ) -> _susr.JobsScheduler:
        def get_user_id(runnable_job: _jb.RunnableJobBase) -> str:
            return runnable_job.user_id

        jobs_by_user_id = _it.groupby(runnable_jobs, key=get_user_id)

        user_jobs = [self._create_user_jobs(k, list(vs)) for k, vs in jobs_by_user_id]
        users_scheduler = _susr.JobsScheduler(user_jobs)
        return users_scheduler

    def _create_user_jobs(
        self, user_id: str, runnable_jobs: _cabc.Sequence[_jb.RunnableJobBase]
    ) -> _susr.UserJobs:
        n_jobs = self._runner_clients_manager.get_n_jobs(user_id)
        return _susr.UserJobs(n_jobs, runnable_jobs)

    @staticmethod
    async def _adjust_wakeup_time_if_needed_and_sleep_until(
        wakeup_time: _dt.datetime, polling_period: _dt.timedelta
    ) -> _dt.datetime:
        now = _dt.datetime.now()
        if wakeup_time < now:
            _LOGGER.warning(
                "Wake up time %s is in the past (now = %s). Resetting to 10 seconds from now.",
                wakeup_time,
                now,
            )
            wakeup_time = now + polling_period

        seconds_to_sleep = (wakeup_time - now).seconds

        await _asyncio.sleep(seconds_to_sleep)

        return wakeup_time
