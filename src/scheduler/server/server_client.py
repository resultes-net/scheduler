import collections.abc as _cabc
import enum as _enum

import aiohttp as _ahttp
import resultes_pydantic_models.server as _psrv
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar


class ServerClient:
    def __init__(self, session: _ahttp.ClientSession) -> None:
        self._session = session

    async def get_simulations_waiting_for_variations_creation(
        self,
    ) -> _cabc.Sequence[_psim.Simulation]:
        params = {"state": "waiting-for-variations-creation"}
        async with self._session.get("simulations", json="", params=params) as response:
            response.raise_for_status()
            json = await response.json()

        simulations = [_psim.Simulation(**s) for s in json]

        return simulations

    async def get_waiting_variations(self) -> _psrv.WaitingVariations:
        async with self._session.get("waiting-variations") as response:
            response.raise_for_status()
            json = await response.json()

        return _psrv.WaitingVariations(**json)

    async def set_simulation_state(
        self, simulation_id: str, new_state: _psim.SimulationState
    ) -> None:
        await self._set_state("simulations", simulation_id, new_state)

    async def create_variation(
        self, simulation_id: str, variation: _pvar.CreateVariation
    ) -> _pvar.Variation:
        async with self._session.post(
            f"simulations/{simulation_id}/variations",
            json=variation.model_dump(),
        ) as response:
            response.raise_for_status()
            response_json = await response.json()
            return _pvar.Variation(**response_json)

    async def set_variation_state(
        self, variation_id: str, new_state: _pvar.VariationState
    ) -> None:
        await self._set_state("variations", variation_id, new_state)

    async def _set_state(self, collection: str, id: str, new_state: _enum.Enum) -> None:
        params = {"new_state": new_state.value}
        async with self._session.put(
            f"{collection}/{id}/state", params=params
        ) as response:
            response.raise_for_status()
            _ = await response.json()
