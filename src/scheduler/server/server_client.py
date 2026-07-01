import collections.abc as _cabc
import enum as _enum
import typing as _tp

import aiohttp as _ahttp
import resultes_pydantic_models.common as _pcom
import resultes_pydantic_models.server as _psrv
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar


class ServerClient:
    def __init__(self, session: _ahttp.ClientSession) -> None:
        self._session = session

    async def get_latest_login_on(self) -> _pcom.AwarePastDatetime:
        async with self._session.get("latest-login") as response:
            response.raise_for_status()
            json = await response.json()

        latest_login = _psrv.LatestLogin(**json)

        return latest_login.on

    async def get_simulations_waiting_for_variations_creation(
        self,
    ) -> _cabc.Sequence[_psim.Simulation]:
        params = {"state": "waiting-for-variations-creation"}
        async with self._session.get("simulations", json="", params=params) as response:
            response.raise_for_status()
            json = await response.json()

        simulations = [_psim.Simulation(**s) for s in json]

        return simulations

    async def get_running_simulations(self) -> _cabc.Sequence[_psim.Simulation]:
        params = {"state": "running"}
        async with self._session.get("simulations", json="", params=params) as response:
            response.raise_for_status()
            json = await response.json()

            simulations = [_psim.Simulation(**s) for s in json]
            return simulations

    async def get_simulation(self, simulation_id: str) -> _psim.Simulation:
        async with self._session.get(f"simulation/{simulation_id}") as response:
            response.raise_for_status()
            json = await response.json()

            return _psim.Simulation(**json)

    async def get_waiting_variations(self) -> _psrv.WaitingVariations:
        async with self._session.get("waiting-variations") as response:
            response.raise_for_status()
            json = await response.json()

        return _psrv.WaitingVariations(**json)

    async def set_simulation_state(
        self, simulation_id: str, new_state: _psim.SimulationState
    ) -> None:
        await self._set_state("simulations", simulation_id, new_state)

    async def set_simulation_progress(
        self, simulation_id: str, new_progress: int
    ) -> None:
        await self._set_progress("simulations", simulation_id, new_progress)

    async def create_variation(
        self, simulation_id: str, variation: _pvar.CreateVariation
    ) -> _pvar.Variation:
        async with self._session.post(
            f"simulations/{simulation_id}/variations",
            json=variation.model_dump(mode="json"),
        ) as response:
            response.raise_for_status()
            response_json = await response.json()
            return _pvar.Variation(**response_json)

    async def get_variations(
        self,
        simulation_id: str,
    ) -> _cabc.Sequence[_pvar.Variation]:
        async with self._session.get(
            f"simulations/{simulation_id}/variations",
        ) as response:
            response.raise_for_status()
            response_json = await response.json()
            variations = [_pvar.Variation(**v) for v in response_json]
            return variations

    async def set_variation_state(
        self, variation_id: str, new_state: _pvar.VariationState
    ) -> None:
        await self._set_state("variations", variation_id, new_state)

    async def set_variation_progress(
        self, variation_id: str, new_progress: int
    ) -> None:
        await self._set_progress("variations", variation_id, new_progress)

    async def _set_state(self, collection: str, id: str, new_state: _enum.Enum) -> None:
        params = {"new_state": new_state.value}
        async with self._session.put(
            f"{collection}/{id}/state", params=params
        ) as response:
            response.raise_for_status()
            _ = await response.json()

    async def _set_progress(
        self,
        collection: _tp.Literal["simulations", "variations"],
        id: str,
        new_progress: int,
    ) -> None:
        params = {"new_progress": new_progress}
        async with self._session.put(
            f"{collection}/{id}/progress", params=params
        ) as response:
            response.raise_for_status()
            _ = await response.json()
