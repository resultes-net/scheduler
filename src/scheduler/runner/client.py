import collections.abc as _cabc
import logging as _log
import pathlib as _pl

import aiohttp as _ahttp
import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjrpct
import resultes_jsonrpc.jsonrpc.types as _tps
import resultes_jsonrpc.websockets.client as _rjwc
import resultes_pydantic_models.runner as _mrun
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar

import scheduler.jrpc_methods as _jrpcm

_jrpcm.configure()

_LOGGER = _log.getLogger(__name__)


class RunnerClient:
    def __init__(
        self,
        requests_websocket: _ahttp.ClientWebSocketResponse,
        logging_websocket: _ahttp.ClientWebSocketResponse,
        ip_address: str,
    ) -> None:
        self._jsonrpc_client = _rjjc.JsonRpcClient(requests_websocket)
        self._requests_websocket_client = _rjwc.WebsocketClient(
            requests_websocket, self._jsonrpc_client
        )

        dispatcher = _rjjs.SyncDispatcher()
        context = _jrpcm.Context(ip_address)
        self._jsonrpc_server = _rjjs.JsonRpcServer(
            logging_websocket, dispatcher, context
        )
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
        self,
        simulation: _psim.Simulation,
    ) -> _cabc.Sequence[str]:
        runner_job = _mrun.RunnerJob(
            id=simulation.id,
            object_storage_path=_mrun.ObjectStorageZipPath(
                container="resultes-static",
                path="pytrnsys-systems/systems-main.zip",
            ),
            program=_pl.PureWindowsPath(
                r"C:\Users\Administrator\resultes\venv3.13\Scripts\python.exe"
            ),
            args=[_pl.PureWindowsPath(r"run.pytrnsys")],
            working_dir=_pl.PureWindowsPath(r"systems-main\TTES"),
            results_glob_pattern="systems-main/TTES/results/*/",
        )

        return await self._run_job_on_client(runner_job)

    async def simulate_variation(
        self,
        variation: _pvar.Variation,
    ) -> None:
        input_object_storage_zip_path = f"{variation.simulation_id}.zip"

        runner_job = _mrun.RunnerJob(
            id=variation.id,
            object_storage_path=_mrun.ObjectStorageZipPath(
                container="resultes-results",
                path=input_object_storage_zip_path,
            ),
            program=_pl.PureWindowsPath(r"C:\TRNSYS18\Exe\TrnEXE.exe"),
            args=[variation.relative_deck_file_containing_dir_path],
            working_dir=variation.relative_deck_file_containing_dir_path.parent,
        )

        await self._run_job_on_client(runner_job)

    async def _run_job_on_client(self, runner_job: _mrun.RunnerJob) -> _tps.Json:
        params: _rjrpct.JsonStructured = {"runner_job": runner_job.model_dump()}

        return await self._jsonrpc_client.send_request_and_check_and_get_response(
            "run_job", params
        )
