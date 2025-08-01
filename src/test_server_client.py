import aiohttp as _ahttp
import pytest as _pt

from server_client import ServerClient


class TestServerClient:
    @_pt.mark.asyncio
    async def test_get_simulations_waiting_for_variations_creation_by_user_id(
        self,
    ) -> None:
        async with _ahttp.ClientSession("http://localhost:8000") as session:
            client = ServerClient(session)
            simulations = (
                await client.get_simulations_waiting_for_variations_creation_by_user_id()
            )
            print(simulations)

    @_pt.mark.asyncio
    async def test_get_waiting_variations(
        self,
    ) -> None:
        async with _ahttp.ClientSession("http://localhost:8000") as session:
            client = ServerClient(session)
            waiting_variations = await client.get_waiting_variations()
            print(waiting_variations)
