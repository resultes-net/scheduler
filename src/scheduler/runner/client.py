import collections.abc as _cabc
import logging as _log
import pathlib as _pl
import typing as _tp

import aiohttp as _ahttp
import resultes_jsonrpc.jsonrpc.connection as _rjjc
import resultes_jsonrpc.jsonrpc.types as _rjrpct
import resultes_jsonrpc.websockets.client as _rjwc
import resultes_pydantic_models.runner as _mrun
import resultes_pydantic_models.simulations.parameters as _params
import resultes_pydantic_models.simulations.parameters.ptes as _pptes
import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import resultes_pydantic_models.simulations.simulation as _psim
import resultes_pydantic_models.simulations.variation as _pvar

import scheduler.job_payload as _jp
import scheduler.jrpc_methods as _jrpcm
import scheduler.runner.paths as _rp

_jrpcm.configure()

_LOGGER = _log.getLogger(__name__)


class RunnerClient:
    def __init__(
        self,
        websocket: _ahttp.ClientWebSocketResponse,
        ip_address: str,
        paths: _rp.Paths,
    ) -> None:
        self._context = _jrpcm.Context(ip_address)
        dispatcher = _rjjc.AsyncDispatcher()

        self._jsonrpc_connection = _rjjc.Connection(
            dispatcher, self._context, websocket
        )

        self._websocket_client = _rjwc.Client(websocket, self._jsonrpc_connection)

        self._paths = paths

        self._started = False

    def start(self) -> None:
        if self._started:
            raise RuntimeError("Already started.")

        self._started = True

        _LOGGER.info("Starting.")

        self._websocket_client.start()

    async def join(self) -> None:
        if not self._started:
            raise RuntimeError("Not started")

        _LOGGER.info("Joining.")

        await self._websocket_client.join()

    async def set_options(
        self,
        log_level: str,
        shall_remove_completed_jobs: bool,
    ) -> None:
        runner_options = _mrun.RunnerOptions(
            log_level=log_level, shall_remove_completed_jobs=shall_remove_completed_jobs
        )
        params: _rjrpct.JsonStructured = {"value": runner_options.model_dump()}

        await self._jsonrpc_connection.send_request_and_check_and_get_response(
            "set_options", params
        )

    async def create_variations(
        self,
        simulation: _psim.Simulation,
    ) -> _cabc.AsyncIterable[_jp.JobPayload]:
        runner_job = self._create_create_variations_runner_job(simulation)

        async for notification in self._run_job_on_client(runner_job):
            yield notification

    def _create_create_variations_runner_job(
        self, simulation: _psim.Simulation
    ) -> _mrun.RunnerJob:
        parameters = simulation.parameters

        system_name = self._get_system_name(parameters)

        commands = list[_mrun.GeneralCommand]()

        if system_name == "PTES":
            create_parameters_ddck_file_command = _mrun.GeneralCommand(
                program=self._paths.python_exe,
                args=[
                    r"PTES\create_parameters_ddck_file.py",
                    "parameters.json",
                ],
                working_dir=_pl.PureWindowsPath("."),
            )

            commands.append(create_parameters_ddck_file_command)

        create_variations_command = _mrun.GeneralCommand(
            program=self._paths.python_exe,
            args=["run.pytrnsys"],
            working_dir=_pl.PureWindowsPath(system_name),
        )

        commands.append(create_variations_command)

        object_storage_output_path = _mrun.ObjectStorageOutputZipFilePath(
            container="resultes-results", path=f"results/{simulation.id}.zip"
        )
        result = _mrun.MultipleFilesResult(
            glob_patterns=_mrun.GlobPatterns(include=["**"]),
            object_storage_output_file_path=object_storage_output_path,
        )

        serialized_parameters = parameters.model_dump()

        runner_job = _mrun.RunnerJob(
            id=simulation.id,
            parameters=serialized_parameters,
            object_storage_input_path=_mrun.ObjectStorageInputZipFilePath(
                container="resultes-static",
                path="pytrnsys-systems/systems-main.zip",
            ),
            commands=commands,
            results=[result],
            return_paths_glob_pattern=f"{system_name}/results/*/",
        )

        return runner_job

    @staticmethod
    def _get_system_name(parameters: _params.Parameters) -> str:
        values = parameters.values

        match values:
            case _pttes.TtesParameters():
                return "TTES"
            case _pptes.PtesParameters():
                return "PTES"
            case _:
                _tp.assert_never(values)

    async def simulate_and_post_process_variation(
        self,
        variation: _pvar.Variation,
        n_total_time_steps: int,
    ) -> _cabc.AsyncIterable[_jp.JobPayload]:
        runner_job = self._create_simulate_and_post_process_variation_runner_job(
            variation, n_total_time_steps
        )

        async for payload in self._run_job_on_client(runner_job):
            yield payload

    def _create_simulate_and_post_process_variation_runner_job(
        self,
        variation: _pvar.Variation,
        n_total_time_steps: int,
    ) -> _mrun.RunnerJob:
        object_storage_input_zip_path = f"results/{variation.simulation_id}.zip"

        object_storage_input_path = _mrun.ObjectStorageInputZipFilePath(
            container="resultes-results",
            path=object_storage_input_zip_path,
        )

        relative_deck_file_containing_dir_path = (
            variation.relative_deck_file_containing_dir_path
        )

        deck_file_name = f"{relative_deck_file_containing_dir_path.name}.dck"

        relative_deck_file_path = (
            relative_deck_file_containing_dir_path / deck_file_name
        )

        relative_tempereratures_step_prt_file_path = (
            relative_deck_file_containing_dir_path / "PTES_T.prt"
        )

        simulate_command = _mrun.RunTrnsysCommand(
            trnsys_exe_path=self._paths.trnexe_exe,
            relative_deck_file_path=relative_deck_file_path,
            n_total_timesteps=n_total_time_steps,
            relative_temperatures_step_prt_file_path=relative_tempereratures_step_prt_file_path,
        )

        post_process_command = _mrun.GeneralCommand(
            program=self._paths.python_exe,
            args=["process.pytrnsys", "results"],
            working_dir=_pl.PureWindowsPath("PTES"),
        )

        object_storage_output_path = _mrun.ObjectStorageOutputZipFilePath(
            container="resultes-results", path=f"results/{variation.id}.zip"
        )

        all_files_result = _mrun.MultipleFilesResult(
            glob_patterns=_mrun.GlobPatterns(include=["**"], exclude=["**/*.lst"]),
            object_storage_output_file_path=object_storage_output_path,
        )

        plot_results = self._create_plot_results(variation)

        runner_job = _mrun.RunnerJob(
            id=variation.id,
            object_storage_input_path=object_storage_input_path,
            commands=[simulate_command, post_process_command],
            results=[all_files_result, *plot_results],
        )

        return runner_job

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
                r"hx\effectiveness-hourly-A4.png",
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

    async def _run_job_on_client(
        self, runner_job: _mrun.RunnerJob, timeout_seconds: float | None = 10.0
    ) -> _cabc.AsyncIterable[_jp.JobPayload]:
        with self._context.add_job(runner_job.id):
            params = {"value": runner_job.model_dump()}

            try:
                await self._jsonrpc_connection.send_request_and_check_and_get_response(
                    "run_job", params, timeout_seconds
                )
            except TimeoutError:
                _LOGGER.error("Sending run job command timed out.")
                raise

            async for payload in self._context.read_payloads(runner_job.id):
                yield payload
