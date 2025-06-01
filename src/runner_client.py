import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import logging as _log
import typing as _tp

import aiohttp as _ahttp
import jsonrpcclient as _jrpcl
import resultes_pydantic_models.simulations.parameters.ttes as _pttes

_LOGGER = _log.getLogger(__file__)


class RunnerClient(_ctx.AbstractAsyncContextManager):
    _WAKEUP_PERIOD = 2.5
    _SHUTDOWN_TIMEOUT_SECONDS = 2 * _WAKEUP_PERIOD

    def __init__(self, websocket: _ahttp.ClientWebSocketResponse) -> None:
        self._websocket = websocket
        self._task: _asyncio.Task[None] | None = None
        self._new_responses_received_event = _asyncio.Event()
        self._parsed_responses_by_request_id = dict[int, _jrpcl.responses.Response]()

    async def __aenter__(self) -> _tp.Self:
        self._task = _asyncio.create_task(self._response_reader())
        return self

    async def __aexit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> bool:
        if not self._task:
            raise RuntimeError("Client not running.")

        self._task.cancel()

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
                    self._parsed_responses_by_request_id[response.id] = response

                _LOGGER.debug("Notifying requests.")
                self._new_responses_received_event.set()
                self._new_responses_received_event.clear()
        except Exception as exception:
            _LOGGER.error("An exception occurred: %s", exception)
            raise
        finally:
            _LOGGER.info("Exiting main loop.")

    async def create_variations(
        self, parameters: _pttes.TtesParameters
    ) -> _cabc.Sequence[str]:
        params = {"parameters": parameters.model_dump()}
        json = _jrpcl.request("create_variations", params)

        _LOGGER.debug("Sending request %s.", json)
        await self._websocket.send_json(json)

        response = await self._get_response(json["id"])
        _LOGGER.debug("Got response %s.", response)

        _jrpcl.parse

        match response:
            case _jrpcl.Ok(result):
                return result
            case _jrpcl.Error() as error:
                raise RuntimeError(error)
            case _:
                _tp.assert_never(_)

    async def _get_response(self, request_id: int) -> _jrpcl.responses.Response:
        while True:
            await self._new_responses_received_event.wait()

            _LOGGER.debug("Hoping for request id %d...", request_id)

            response = self._parsed_responses_by_request_id.get(request_id)

            if response:
                _LOGGER.debug("...success!.")
                return response

            _LOGGER.debug("...maybe next time.")
