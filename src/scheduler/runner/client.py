import collections.abc as _cabc
import logging as _log

import aiohttp as _ahttp
import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjrpct
import resultes_jsonrpc.websockets.client as _rjwc
import resultes_pydantic_models.runner as _mrun
import resultes_pydantic_models.simulations.parameters.ttes as _pttes

import scheduler.jrpc_methods as _jrpcm

_jrpcm.configure()

_LOGGER = _log.getLogger(__name__)


class RunnerClient:
    def __init__(
        self,
        requests_websocket: _ahttp.ClientWebSocketResponse,
        logging_websocket: _ahttp.ClientWebSocketResponse,
    ) -> None:
        self._jsonrpc_client = _rjjc.JsonRpcClient(requests_websocket)
        self._requests_websocket_client = _rjwc.WebsocketClient(
            requests_websocket, self._jsonrpc_client
        )

        dispatcher = _rjjs.SyncDispatcher()
        self._jsonrpc_server = _rjjs.JsonRpcServer(logging_websocket, dispatcher)
        self._logging_websocket_client = _rjwc.WebsocketClient(
            logging_websocket, self._jsonrpc_server
        )

        self._started = False

    def start(self) -> None:
        if self._started:
            raise RuntimeError("Already started.")

        self._started = True

        _LOGGER.info("Starting.")

        self._requests_websocket_client.start()
        self._logging_websocket_client.start()

    async def join(self) -> None:
        if not self._started:
            raise RuntimeError("Not started")

        _LOGGER.info("Joining.")

        await self._requests_websocket_client.join()
        await self._logging_websocket_client.join()

    async def create_variations(
        self, simulation_id: str, parameters: _pttes.TtesParameters
    ) -> _cabc.Sequence[str]:
        # params = {"parameters": parameters.model_dump()}

        runner_job = _mrun.RunnerJob(
            id=simulation_id,
            object_storage_path=_mrun.ObjectStorageZipPath(
                container="resultes-static",
                path="pytrnsys-systems/systems-main.zip",
            ),
            script_to_run="systems-main/TTES/run.pytrnsys",
            results_glob_pattern="systems-main/TTES/results/*/",
        )

        params: _rjrpct.JsonStructured = {"runner_job": runner_job.model_dump()}

        return await self._jsonrpc_client.send_request_and_check_and_get_response(
            "run_python_script_in_pytrnsys_venv", params
        )
