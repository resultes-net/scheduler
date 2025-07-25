import asyncio as _asyncio
import collections.abc as _cabc
import logging as _log
import typing as _tp

import aiohttp as _ahttp
import jsonrpcserver as _jrpcs
import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjrpct
import resultes_pydantic_models.pytrnsys as _mpytrnsys
import resultes_pydantic_models.simulations.parameters.ttes as _pttes

_LOGGER = _log.getLogger(__name__)


@_jrpcs.method()
async def post_log_message(_: _tp.Any, level: int, message: str) -> None:
    _LOGGER.log(level, message)


class RunnerClient:
    def __init__(
        self,
        requests_websocket: _ahttp.ClientWebSocketResponse,
        logging_websocket: _ahttp.ClientWebSocketResponse,
    ) -> None:
        self._jsonrpc_client = _rjjc.JsonRpcClient(requests_websocket)
        self._jsonrpc_server = _rjjs.JsonRpcServer(logging_websocket)

        self._jsonrpc_client_response_reader_task: _asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._jsonrpc_client_response_reader_task:
            raise RuntimeError("Already started.")

        coroutine = self._jsonrpc_client.start()
        self._jsonrpc_client_response_reader_task = _asyncio.Task(coroutine)

        await self._jsonrpc_server.start()

    def stop(self) -> None:
        if not self._jsonrpc_client_response_reader_task:
            raise RuntimeError("Not started.")

        self._jsonrpc_server.stop()
        self._jsonrpc_client.stop()

    async def create_variations(
        self, simulation_id: str, parameters: _pttes.TtesParameters
    ) -> _cabc.Sequence[str]:
        # params = {"parameters": parameters.model_dump()}

        runner_job = _mpytrnsys.RunnerJob(
            id=simulation_id,
            object_storage_path=_mpytrnsys.ObjectStorageZipPath(
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
