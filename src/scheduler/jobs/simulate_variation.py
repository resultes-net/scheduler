import datetime as _dt
import logging as _log
import pathlib as _pl

import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar

import scheduler.runnable_job_base as _jb
import scheduler.runner.client as _rc
import scheduler.server.server_client as _sc

_LOGGER = _log.getLogger(__name__)


class SimulateVariation(_jb.RunnableJobBase):
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
    def user_id(self) -> str:
        return self._user_id

    @property
    def waiting_to_run_since(self) -> _dt.datetime:
        return self._variation.state_changed_on

    async def set_started(self) -> None:
        await self._server_client.set_variation_state(
            self._variation.id, _pvar.VariationState.RUNNING
        )

    async def run(self, runner_client: _rc.RunnerClient) -> None:
        await self._server_client.set_variation_state(
            self._variation.id, _pvar.VariationState.DONE
        )
