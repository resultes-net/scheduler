import aiohttp as _ahttp

import scheduler.runner.client as _rc
import scheduler.runner.paths as _rp


class RunnerClientWrapper:
    def __init__(
        self,
        session: _ahttp.ClientSession,
        websocket: _ahttp.ClientWebSocketResponse,
        runner_client: _rc.RunnerClient,
    ) -> None:
        self._session = session
        self._websocket = websocket
        self._client: _rc.RunnerClient | None = runner_client

    @staticmethod
    async def create(
        ip_address: str,
        paths: _rp.Paths,
    ) -> "RunnerClientWrapper":
        base_uri = f"http://{ip_address}:3000"

        session = _ahttp.ClientSession(base_uri)

        try:
            requests_websocket = await session.ws_connect("/jsonrpc")
        except:
            await session.close()
            raise

        client = _rc.RunnerClient(
            requests_websocket,
            ip_address,
            paths,
        )
        client.start()

        return RunnerClientWrapper(session, requests_websocket, client)

    @property
    def client(self) -> _rc.RunnerClient:
        if not self._client:
            raise RuntimeError("Client shut down.")

        return self._client

    async def shut_down(self) -> None:
        await self._websocket.close()

        await self.client.join()
        self._client = None

        await self._session.close()
