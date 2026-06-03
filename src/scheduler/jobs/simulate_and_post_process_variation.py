import datetime as _dt
import logging as _log
import typing as _tp

import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar
import resultes_pydantic_models.runner as _prun

import scheduler.runnable_job_base as _jb
import scheduler.runner.client as _rc
import scheduler.server.server_client as _sc

_LOGGER = _log.getLogger(__name__)


class SimulateAndPostProcessVariation(_jb.RunnableJobBase):
    def __init__(
        self, variation: _pvar.Variation, user_id: str, server_client: _sc.ServerClient
    ) -> None:
        if variation.state != _pvar.VariationState.WAITING:
            raise ValueError(
                f"Variation not in state {_pvar.VariationState.WAITING.name}.",
                variation.state,
            )

        super().__init__()

        self._variation = variation
        self._user_id = user_id
        self._server_client = server_client

    @property
    @_tp.override
    def id(self) -> str:
        return self._variation.id

    @property
    @_tp.override
    def user_id(self) -> str:
        return self._user_id

    @property
    @_tp.override
    def waiting_to_run_since(self) -> _dt.datetime:
        return self._variation.state_changed_on

    @_tp.override
    async def set_started(self) -> None:
        await self._server_client.set_simulation_state(
            self._variation.simulation_id, _psim.SimulationState.RUNNING_VARIATIONS
        )

        await self._server_client.set_variation_state(
            self._variation.id, _pvar.VariationState.RUNNING
        )

    @_tp.override
    async def run(self, runner_client: _rc.RunnerClient) -> None:
        simulation = await self._server_client.get_simulation(
            self._variation.simulation_id
        )

        n_total_time_steps = simulation.parameters.values.time.n_steps

        async for payload in runner_client.simulate_and_post_process_variation(
            self._variation, n_total_time_steps
        ):
            match payload:
                case _prun.JobProgress(progress=progress):
                    await self._update_progress(progress)
                case _prun.JobSuccess():
                    await self._update_state_on_success()
                case _prun.JobError() as job_error:
                    await self._update_state_on_error(job_error)

    async def _update_progress(self, progress: int) -> None:
        _LOGGER.info("Variation %s progress: %i.", self._variation.id, progress)

        await self._server_client.set_variation_progress(self._variation.id, progress)

        variations = await self._server_client.get_variations(
            self._variation.simulation_id
        )

        # This only works if variations take equally long, but I can't think of a better
        # way for now.
        simulation_progress = round(
            sum(v.progress for v in variations) / len(variations)
        )

        await self._server_client.set_simulation_progress(
            self._variation.simulation_id, simulation_progress
        )

    async def _update_state_on_success(self) -> None:
        _LOGGER.info("%s - Variation done.", self.id)

        await self._server_client.set_variation_state(
            self._variation.id, _pvar.VariationState.DONE
        )

        variations = await self._server_client.get_variations(
            self._variation.simulation_id
        )

        all_variations_done = all(
            v.state == _pvar.VariationState.DONE for v in variations
        )

        if all_variations_done:
            await self._server_client.set_simulation_state(
                self._variation.simulation_id, _psim.SimulationState.DONE
            )

    async def _update_state_on_error(self, job_error: _prun.JobError) -> None:
        _LOGGER.error(
            "%s - Error running job command %s: %s.",
            self.id,
            job_error.command_number,
            job_error.message,
        )

        await self._server_client.set_variation_state(
            self._variation.id, _pvar.VariationState.ERROR
        )

        await self._server_client.set_simulation_state(
            self._variation.simulation_id, _psim.SimulationState.ERROR
        )
