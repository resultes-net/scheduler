import logging as _log
import pathlib as _pl
import typing as _tp

import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar
import resultes_pydantic_models.runner as _prun

import scheduler.runner.client as _rc
import scheduler.server.server_client as _sc

from . import _simulation_job_base as _sj

_LOGGER = _log.getLogger(__name__)


class CreateVariationsJob(_sj.SimulationJobBase):
    def __init__(
        self, simulation: _psim.Simulation, server_client: _sc.ServerClient
    ) -> None:
        if simulation.state != _psim.SimulationState.WAITING_FOR_VARIATIONS_CREATION:
            raise ValueError("Simulation not waiting for creations of variations.")

        super().__init__(simulation, server_client)

    @_tp.override
    async def set_started(self) -> None:
        await self._server_client.set_simulation_state(
            self._simulation.id, _psim.SimulationState.CREATING_VARIATIONS
        )

    @_tp.override
    async def run(self, runner_client: _rc.RunnerClient) -> None:
        async for payload in runner_client.create_variations(self._simulation):
            match payload:
                case _prun.JobError() as job_error:
                    _LOGGER.error(
                        "%s - Error running job command %s: %s.",
                        self.id,
                        job_error.command_number,
                        job_error.message,
                    )
                    await self._server_client.set_simulation_state(
                        self._simulation.id, _psim.SimulationState.ERROR
                    )
                    return
                case _:
                    pass

        relative_deck_file_paths = payload.result

        if relative_deck_file_paths:
            for relative_deck_file_path in relative_deck_file_paths:
                _LOGGER.info(
                    "Creating variation for deck file %s (simulation ID = %s)...",
                    relative_deck_file_path,
                    self._simulation.id,
                )
                relative_deck_file_pure_path = _pl.PureWindowsPath(
                    relative_deck_file_path
                )

                variation = _pvar.CreateVariation(
                    relative_deck_file_containing_dir_path=relative_deck_file_pure_path,
                )

                await self._server_client.create_variation(
                    self._simulation.id, variation
                )

                _LOGGER.info("...DONE.")

        else:
            _LOGGER.info("Got empty response to request %s.", self._simulation.id)

        await self._server_client.set_simulation_state(
            self._simulation.id, _psim.SimulationState.WAITING_FOR_VARIATION_RUNS
        )
