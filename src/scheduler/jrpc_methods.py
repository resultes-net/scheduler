import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import logging as _log
import traceback as _tb

import jsonrpcserver as _jrpcs
import jsonrpcserver.codes as _jrpcsc
import pydantic as _pyd
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.jsonrpc.types as _rjjt
import resultes_pydantic_models.runner as _rpmr

_LOGGER = _log.getLogger(__name__)


def configure() -> None:
    # This is a dummy method making sure that the import of this module is not flagged as "unused"
    pass


class Context:
    def __init__(self, ip_address: str) -> None:
        super().__init__()
        self.ip_address = ip_address
        self._notification_queues = dict[str, _asyncio.Queue[_rpmr.JobNotification]]()

    @_ctx.contextmanager
    def add_job(self, job_id: str) -> _cabc.Iterator[None]:
        self._notification_queues[job_id] = _asyncio.Queue[_rpmr.JobNotification]()
        yield
        del self._notification_queues[job_id]

    async def read_notifications(
        self, job_id: str
    ) -> _cabc.AsyncIterable[_rpmr.JobNotification]:
        queue = self._get_queue(job_id)

        self._notification_queues[job_id] = queue

        try:
            while notification := await queue.get():
                yield notification

                payload = notification.payload

                match payload:
                    case _rpmr.JobError() | _rpmr.JobSuccess():
                        return
                    case _:
                        pass
        finally:
            del self._notification_queues[job_id]

    async def send_notification(self, job_notification: _rpmr.JobNotification) -> None:
        job_id = job_notification.job_id

        queue = self._get_queue(job_id)

        if not queue:
            raise ValueError("Uknown job ID.", job_id)

        await queue.put(job_notification)

    def _get_queue(self, job_id: str) -> _asyncio.Queue[_rpmr.JobNotification]:
        queue = self._notification_queues.get(job_id)

        if not queue:
            raise ValueError("Uknown job ID.", job_id)

        return queue


def cancellable_async_validated_jrpcs_method[T: _pyd.BaseModel, C](
    clazz: type[T],
) -> _cabc.Callable[
    [_rjjs.AsyncJsonRpcMethod[[C, T]]], _rjjs.AsyncJsonRpcMethod[[C, _rjjt.JsonObject]]
]:
    def create_validating_method(
        validated_method: _rjjs.AsyncJsonRpcMethod[[C, T]],
    ) -> _rjjs.AsyncJsonRpcMethod[[C, _rjjt.JsonObject]]:
        @_jrpcs.method()
        @_ft.wraps(validated_method)
        async def validating_method(
            data: _rjjt.JsonObject, context: C
        ) -> _jrpcs.Result:
            try:
                instance = clazz(**data)
            except _pyd.ValidationError as validation_error:
                errors = validation_error.errors()
                return _jrpcs.InvalidParams(errors)

            try:
                return await validated_method(context, instance)
            except _asyncio.CancelledError:
                _LOGGER.warning("The request was cancelled on the server.")
                return _jrpcs.Error(
                    _jrpcsc.ERROR_SERVER_ERROR,
                    "The request was cancelled on the server.",
                )
            except Exception as exception:
                _LOGGER.error("Exception occurred: %s", exc_info=exception)
                traceback = "\n".join(_tb.format_exception(exception))
                return _jrpcs.Error(
                    _jrpcsc.ERROR_SERVER_ERROR, str(exception), traceback
                )

        return validating_method

    return create_validating_method


@cancellable_async_validated_jrpcs_method(_rpmr.LogMessage)
async def post_log_message(
    context: Context, log_message: _rpmr.LogMessage
) -> _jrpcs.Result:
    extra = {"remote_ip": context.ip_address}

    # Make sure we see any message coming in from the runner (which messages the runner sends
    # can be configured on the runner), i.e. ensure that we never log below our root logger's
    # level. However, if the message coming in is at a higher level, we want to log at that level,
    # to better convey the urgency.
    root_logger = _log.getLogger()
    root_logger_level = root_logger.getEffectiveLevel()
    log_message.level = max(log_message.level, root_logger_level)

    _LOGGER.log(log_message.level, "%s", log_message.message, extra=extra)

    return _jrpcs.Success()


@cancellable_async_validated_jrpcs_method(_rpmr.JobNotification)
async def job_notification(
    context: Context, job_notification: _rpmr.JobNotification
) -> _jrpcs.Result:
    await context.send_notification(job_notification)

    return _jrpcs.Success()
