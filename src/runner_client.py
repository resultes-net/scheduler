import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import logging as _log
import typing as _tp

import aiohttp as _ahttp
import jsonrpcclient as _jrpcl
import resultes_pydantic_models.pytrnsys as _mpytrnsys
import resultes_pydantic_models.simulations.parameters.ttes as _pttes

_LOGGER = _log.getLogger(__file__)


class RunnerClient(_ctx.AbstractAsyncContextManager["RunnerClient"]):
    _WAKEUP_PERIOD = 2.5
    _SHUTDOWN_TIMEOUT_SECONDS = 2 * _WAKEUP_PERIOD

    def __init__(self, websocket: _ahttp.ClientWebSocketResponse) -> None:
        self._websocket = websocket
        self._task: _asyncio.Task[None] | None = None
        self._new_responses_received_event = _asyncio.Event()
        self._parsed_response_futures_by_request_id = dict[
            int, _asyncio.Future[_jrpcl.responses.Response]
        ]()

    def start(self) -> None:
        self._task = _asyncio.create_task(self._response_reader())

    def stop(self) -> None:
        if not self._task:
            raise RuntimeError("Client not running.")

        self._task.cancel()

    async def __aenter__(self) -> _tp.Self:
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> bool:
        self.stop()

        # Don't ignore exception that caused this method to be called
        return False

    async def _response_reader(self) -> None:

        _LOGGER.debug("Start reading responses.")

        try:
            while True:
                try:
                    response = await self._websocket.receive_json(
                        timeout=self._WAKEUP_PERIOD
                    )
                except TimeoutError:
                    continue

                _LOGGER.debug("Received response %s.", response)

                parsed = _jrpcl.parse(response)

                responses = (
                    [parsed]
                    if isinstance(parsed, (_jrpcl.Ok, _jrpcl.Error))
                    else list(parsed)
                )

                for response in responses:
                    future = self._parsed_response_futures_by_request_id[response.id]
                    future.set_result(response)

        except Exception as exception:
            _LOGGER.error("An exception occurred: %s", exception)
            raise
        finally:
            _LOGGER.info("Exiting main loop.")

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

        params = {"runner_job": runner_job.model_dump()}
        json = _jrpcl.request("run_python_script_in_pytrnsys_venv", params)

        return await self._send_request_and_check_response(json)

    async def set_loki_ip_address(self, loki_ip_address: str) -> None:
        params = {"loki_ip_address": loki_ip_address}
        json = _jrpcl.request("set_loki_ip_address", params)

        await self._send_request_and_check_response(json)

    async def _send_request_and_check_response(self, json: _tp.Any) -> _tp.Any:
        _LOGGER.debug("Sending request %s.", json)
        await self._websocket.send_json(json)

        response = await self._get_response(json["id"])
        _LOGGER.debug("Got response %s.", response)

        match response:
            case _jrpcl.Ok(result):
                return result
            case _jrpcl.Error() as error:
                raise RuntimeError(error)
            case _:
                _tp.assert_never(_)

    async def _get_response(self, request_id: int) -> _jrpcl.responses.Response:
        async with self._registered_future(request_id) as future:
            response = await future
            return response

    @_ctx.asynccontextmanager
    async def _registered_future(
        self, request_id: int
    ) -> _cabc.AsyncIterator[_asyncio.Future[_jrpcl.responses.Response]]:
        future = _asyncio.Future[_jrpcl.responses.Response]()
        self._parsed_response_futures_by_request_id[request_id] = future

        yield future

        del self._parsed_response_futures_by_request_id[request_id]
