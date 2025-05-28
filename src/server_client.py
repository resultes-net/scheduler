import collections.abc as _cabc

import aiohttp as _ahttp
import resultes_pydantic_models.simulations.simulation as _psim


class ServerClient:
    def __init__(self, session: _ahttp.ClientSession) -> None:
        self._session = session

    async def get_simulations_waiting_for_variations_creation_by_user_id(
        self,
    ) -> _cabc.Mapping[str, _cabc.Sequence[_psim.Simulation]]:
        params = {"state": "waiting-for-variations-creation"}
        async with self._session.get("simulations", json="", params=params) as response:
            json = await response.json()
            result = {
                user_id: [_psim.Simulation(**s) for s in simulations]
                for user_id, simulations in json.items()
            }
            return result


