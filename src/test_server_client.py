from server_client import ServerClient


import aiohttp as _ahttp
import pytest as _pt


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