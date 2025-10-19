import collections.abc as _cabc
import logging as _log
import pathlib as _pl
import typing as _tp

import aiohttp as _ahttp
import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjrpct
import resultes_jsonrpc.jsonrpc.types as _tps
import resultes_jsonrpc.websockets.client as _rjwc
import resultes_pydantic_models.runner as _mrun
import resultes_pydantic_models.simulations.parameters.ptes as _pptes
import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar

import scheduler.jrpc_methods as _jrpcm
import scheduler.runner.paths as _rp

_jrpcm.configure()

_LOGGER = _log.getLogger(__name__)


class RunnerClient:
    def __init__(
        self,
        requests_websocket: _ahttp.ClientWebSocketResponse,
        logging_websocket: _ahttp.ClientWebSocketResponse,
        ip_address: str,
        paths: _rp.Paths,
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

        self._paths = paths

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

    async def set_options(
        self,
        log_level: str,
        shall_remove_completed_jobs: bool,
    ) -> None:
        runner_options = _mrun.RunnerOptions(
            log_level=log_level, shall_remove_completed_jobs=shall_remove_completed_jobs
        )
        params: _rjrpct.JsonStructured = {"runner_options": runner_options.model_dump()}

        await self._jsonrpc_client.send_request_and_check_and_get_response(
            "set_options", params
        )

    async def create_variations(
        self,
        simulation: _psim.Simulation,
    ) -> _cabc.Sequence[str]:
        match simulation.parameters:
            case _pttes.TtesParameters():
                system_name = "TTES"
            case _pptes.PtesParameters():
                system_name = "PTES"
            case _:
                _tp.assert_never(simulation.parameters)

        runner_job = self._create_create_variations_runner_job(simulation, system_name)

        return await self._run_job_on_client(runner_job)

    def _create_create_variations_runner_job(
        self, simulation: _psim.Simulation, system_name: _tp.Literal["TTES", "PTES"]
    ) -> _mrun.RunnerJob:
        command = _mrun.Command(
            program=self._paths.python_exe,
            args=["run.pytrnsys"],
            working_dir=_pl.PureWindowsPath("systems-main") / system_name,
        )

        object_storage_output_path = _mrun.ObjectStorageOutputZipFilePath(
            container="resultes-results", path=f"results/{simulation.id}.zip"
        )
        result = _mrun.MultipleFilesResult(
            glob_patterns=["**"],
            object_storage_output_file_path=object_storage_output_path,
        )

        runner_job = _mrun.RunnerJob(
            id=simulation.id,
            object_storage_input_path=_mrun.ObjectStorageInputZipFilePath(
                container="resultes-static",
                path="pytrnsys-systems/systems-main.zip",
            ),
            commands=[command],
            results=[result],
            return_paths_glob_pattern=f"systems-main/{system_name}/results/*/",
        )

        return runner_job

    async def simulate_and_post_process_variation(
        self,
        variation: _pvar.Variation,
    ) -> None:
        object_storage_input_zip_path = f"results/{variation.simulation_id}.zip"

        object_storage_input_path = _mrun.ObjectStorageInputZipFilePath(
            container="resultes-results",
            path=object_storage_input_zip_path,
        )

        relative_deck_file_containing_dir_path = (
            variation.relative_deck_file_containing_dir_path
        )

        deck_file_name = f"{relative_deck_file_containing_dir_path.name}.dck"

        relative_log_file_path = (
            relative_deck_file_containing_dir_path
            / f"{relative_deck_file_containing_dir_path.name}.log"
        )

        simulate_command = _mrun.Command(
            program=self._paths.trnexe_exe,
            args=[deck_file_name, "/N"],
            working_dir=relative_deck_file_containing_dir_path,
            relative_log_file_path=relative_log_file_path,
        )

        post_process_command = _mrun.Command(
            program=self._paths.python_exe,
            args=["process.pytrnsys", "results"],
            working_dir=_pl.PureWindowsPath("systems-main") / "PTES",
        )

        object_storage_output_path = _mrun.ObjectStorageOutputZipFilePath(
            container="resultes-results", path=f"results/{variation.id}.zip"
        )

        all_files_result = _mrun.MultipleFilesResult(
            glob_patterns=["**"],
            object_storage_output_file_path=object_storage_output_path,
        )

        plot_results = self._create_plot_results(variation)

        runner_job = _mrun.RunnerJob(
            id=variation.id,
            object_storage_input_path=object_storage_input_path,
            commands=[simulate_command, post_process_command],
            results=[all_files_result, *plot_results],
        )

        await self._run_job_on_client(runner_job)

    @staticmethod
    def _create_plot_results(
        variation: _pvar.Variation,
    ) -> _cabc.Sequence[_mrun.SingleFileResult]:
        plot_result_paths = map(
            _pl.PureWindowsPath,
            [
                r"balance\balance-monthly-A4.png",
                r"boiler\boiler-hourly-A4.png",
                r"hp\balance-monthly-A4.png",
                r"hp\q_t-A4.png",
                r"hx\efficiency-hourly-A4.png",
                r"hx\LMTD-hourly-A4.png",
                r"ptes\balance-monthly-A4.png",
                r"ptes\soc-hourly-A4.png",
                r"ptes\t-ptes-hourly-A4.png",
                r"sink\sink-hourly-A4.png",
                r"solar\q_t-A4.png",
                r"solar\solar-hourly-A4.png",
                r"solar\solar-monthly-A4.png",
                r"source\source-hourly-A4.png",
            ],
        )

        variation_dir_path = variation.relative_deck_file_containing_dir_path
        plot_results = [
            _mrun.SingleFileResult(
                file_path=variation_dir_path / p,
                object_storage_output_file_path=_mrun.ObjectStorageOutputFilePath(
                    container="resultes-results",
                    path=f"results/{variation.id}/{p.as_posix()}",
                ),
            )
            for p in plot_result_paths
        ]

        return plot_results

    async def _run_job_on_client(self, runner_job: _mrun.RunnerJob) -> _tps.Json:
        params: _rjrpct.JsonStructured = {"runner_job": runner_job.model_dump()}

        return await self._jsonrpc_client.send_request_and_check_and_get_response(
            "run_job", params
        )
