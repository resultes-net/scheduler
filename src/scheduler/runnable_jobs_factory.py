import asyncio as _asyncio
import collections.abc as _cabc
import logging as _log
import pprint as _pprint

import resultes_pydantic_models.simulations.simulation as _psim

import scheduler.jobs.create_variations as _cv
import scheduler.jobs.simulate_and_post_process_variation as _sppvj
import scheduler.runnable_job_base as _jb
import scheduler.server.server_client as _sc

_LOGGER = _log.getLogger(__name__)


class RunnableJobsFactory:
    def __init__(self, server_client: _sc.ServerClient) -> None:
        self._server_client = server_client

    async def create_runnable_jobs(self) -> _cabc.Sequence[_jb.RunnableJobBase]:
        create_variations_jobs = await self._create_create_variations_jobs()

        simulate_variation_jobs = (
            await self._create_simulate_and_post_process_variation_jobs()
        )

        runnable_jobs: _cabc.Sequence[_jb.RunnableJobBase] = [
            *create_variations_jobs,
            *simulate_variation_jobs,
        ]

        return runnable_jobs

    async def _create_create_variations_jobs(
        self,
    ) -> _cabc.Sequence[_cv.CreateVariationsJob]:
        waiting_get_simulations = (
            await self._server_client.get_simulations_waiting_for_variations_creation()
        )

        if waiting_get_simulations:
            data = _pprint.pformat(waiting_get_simulations, indent=4)

            _LOGGER.info(
                "Found the following simulations for which to create variations: %s\n",
                data,
            )

        async def get_params_and_create_sim(
            get_simulation: _psim.GetSimulation,
        ) -> _psim.SimulationWithParams:
            parameters = await self._server_client.get_simulation_parameters(
                get_simulation.id
            )
            simulation = _psim.SimulationWithParams(
                **get_simulation.model_dump(), parameters=parameters
            )
            return simulation

        waiting_simulations = await _asyncio.gather(
            *[get_params_and_create_sim(gs) for gs in waiting_get_simulations]
        )

        create_variations_jobs = [
            _cv.CreateVariationsJob(s, self._server_client) for s in waiting_simulations
        ]

        return create_variations_jobs

    async def _create_simulate_and_post_process_variation_jobs(
        self,
    ) -> _cabc.Sequence[_sppvj.SimulateAndPostProcessVariation]:
        waiting_variations = await self._server_client.get_waiting_variations()

        if waiting_variations.waiting_variations:
            data = _pprint.pformat(waiting_variations, indent=4)

            _LOGGER.info(
                "Found the following variations waiting to be simulated: %s\n",
                data,
            )

        user_ids = {s.id: s.user_id for s in waiting_variations.associated_simulations}

        simulate_variation_jobs = [
            _sppvj.SimulateAndPostProcessVariation(
                v, user_ids[v.simulation_id], self._server_client
            )
            for v in waiting_variations.waiting_variations
        ]

        return simulate_variation_jobs
