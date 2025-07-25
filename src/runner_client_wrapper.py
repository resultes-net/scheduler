import aiohttp as _ahttp

import runner_client as _rc


class RunnerClientWrapper:
    def __init__(
        self,
        session: _ahttp.ClientSession,
        requests_websocket: _ahttp.ClientWebSocketResponse,
        logging_websocket: _ahttp.ClientWebSocketResponse,
        runner_client: _rc.RunnerClient,
    ) -> None:
        self._session = session
        self._requests_websocket = requests_websocket
        self._logging_websocket = logging_websocket
        self._client: _rc.RunnerClient | None = runner_client

    @staticmethod
    async def create(ip_address: str) -> "RunnerClientWrapper":
        base_uri = f"http://{ip_address}:3000/"

        session = _ahttp.ClientSession(base_uri)

        requests_websocket = None
        logging_websocket = None
        try:
            requests_websocket = await session.ws_connect("/requests")
            logging_websocket = await session.ws_connect("/logging")
        except:
            if requests_websocket:
                await requests_websocket.close()
            await session.close()
            raise

        client = _rc.RunnerClient(requests_websocket, logging_websocket)
        await client.start()

        return RunnerClientWrapper(
            session, requests_websocket, logging_websocket, client
        )

    @property
    def client(self) -> _rc.RunnerClient:
        if not self._client:
            raise RuntimeError("Client shut down.")

        return self._client

    async def shut_down(self) -> None:
        self.client.stop()
        await self._requests_websocket.close()
        await self._logging_websocket.close()
        await self._session.close()
