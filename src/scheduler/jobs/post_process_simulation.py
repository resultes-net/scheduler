import logging as _log

import resultes_pydantic_models.simulations.simulation as _psim

import scheduler.server.server_client as _sc

from . import _simulation_job_base as _sj

_LOGGER = _log.getLogger(__name__)


class PostProcessSimulation(_sj.SimulationJobBase):
    def __init__(
        self, simulation: _psim.Simulation, server_client: _sc.ServerClient
    ) -> None:
        if (
            simulation.state
            != _psim.SimulationState.WAITING_FOR_CROSS_VARIATION_PROCESSING
        ):
            raise ValueError(
                "Simulation not waiting for cross processing of variations."
            )

        super().__init__(simulation, server_client)
