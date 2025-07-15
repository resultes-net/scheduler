import collections.abc as _cabc

import aiohttp as _ahttp
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar


class ServerClient:
    def __init__(self, session: _ahttp.ClientSession) -> None:
        self._session = session

    async def get_simulations_waiting_for_variations_creation_by_user_id(
        self,
    ) -> _cabc.Mapping[str, _cabc.Sequence[_psim.Simulation]]:
        params = {"state": "waiting-for-variations-creation"}
        async with self._session.get("simulations", json="", params=params) as response:
            response.raise_for_status()

            json = await response.json()
            result = {
                user_id: [_psim.Simulation(**s) for s in simulations]
                for user_id, simulations in json.items()
            }
            return result

    async def set_simulation_state(
        self, simulation_id: str, new_state: _psim.SimulationState
    ) -> None:
        async with self._session.put(
            f"simulations/{simulation_id}/state", json=new_state.value
        ) as response:
            response.raise_for_status()
            _ = await response.json()

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
