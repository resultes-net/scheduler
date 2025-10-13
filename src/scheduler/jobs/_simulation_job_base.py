import datetime as _dt
import logging as _log
import typing as _tp

import resultes_pydantic_models.simulations.simulation as _psim

import scheduler.runnable_job_base as _jb
import scheduler.server.server_client as _sc

_LOGGER = _log.getLogger(__name__)


class SimulationJobBase(_jb.RunnableJobBase):
    def __init__(
        self, simulation: _psim.Simulation, server_client: _sc.ServerClient
    ) -> None:
        super().__init__()

        self._simulation = simulation
        self._server_client = server_client

    @property
    @_tp.override
    def id(self) -> str:
        return self._simulation.id

    @property
    @_tp.override
    def user_id(self) -> str:
        return self._simulation.user_id

    @property
    @_tp.override
    def waiting_to_run_since(self) -> _dt.datetime:
        return self._simulation.state_changed_on
