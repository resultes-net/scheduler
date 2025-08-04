import datetime as _dt
import logging as _log
import pathlib as _pl

import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar

import scheduler.runnable_job_base as _jb
import scheduler.runner.client as _rc
import scheduler.server.server_client as _sc

_LOGGER = _log.getLogger(__name__)


class CreateVariationsJob(_jb.RunnableJobBase):
    def __init__(
        self, simulation: _psim.Simulation, server_client: _sc.ServerClient
    ) -> None:
        if simulation.state != _psim.SimulationState.WAITING_FOR_VARIATIONS_CREATION:
            raise ValueError("Simulation not waiting for creations of variations.")

        super().__init__()

        self._simulation = simulation
        self._server_client = server_client

    @property
    def user_id(self) -> str:
        return self._simulation.user_id

    @property
    def waiting_to_run_since(self) -> _dt.datetime:
        return self._simulation.state_changed_on

    async def set_started(self) -> None:
        await self._server_client.set_simulation_state(
            self._simulation.id, _psim.SimulationState.CREATING_VARIATIONS
        )

    async def run(self, runner_client: _rc.RunnerClient) -> None:
        relative_deck_file_paths = await runner_client.create_variations(
            self.id, self._simulation.parameters
        )

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
                    relative_deck_file_path=relative_deck_file_pure_path,
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
