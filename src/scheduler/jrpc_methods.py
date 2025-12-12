import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import logging as _log

import jsonrpcserver as _jrpcs
import resultes_jsonrpc.jsonrpc.connection as _rjjc
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
        _LOGGER.info("Adding notification queue for job %s.", job_id)
        self._notification_queues[job_id] = _asyncio.Queue[_rpmr.JobNotification]()
        yield
        del self._notification_queues[job_id]
        _LOGGER.info("Notification queue for job %s removed.", job_id)

    async def read_notifications(
        self, job_id: str
    ) -> _cabc.AsyncIterable[_rpmr.JobNotification]:
        queue = self._get_queue(job_id)

        self._notification_queues[job_id] = queue

        _LOGGER.info("Start waiting for notifications for job %s.", job_id)

        while notification := await queue.get():
            _LOGGER.debug("Job %s got notification %s.", job_id, notification)

            yield notification

            payload = notification.payload

            match payload:
                case _rpmr.JobError() | _rpmr.JobSuccess():
                    break
                case _:
                    pass

        _LOGGER.info("Stop waiting for notifications for job %s.", job_id)

    async def send_notification(self, job_notification: _rpmr.JobNotification) -> None:
        _LOGGER.debug(
            "Sending notification for job %s: %s.",
            job_notification.job_id,
            job_notification,
        )

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


@_rjjc.cancellable_async_validated_jrpcs_method(_rpmr.LogMessage)
async def post_log_message(context: Context, value: _rpmr.LogMessage) -> _jrpcs.Result:
    extra = {"remote_ip": context.ip_address}

    # Make sure we see any message coming in from the runner (which messages the runner sends
    # can be configured on the runner), i.e. ensure that we never log below our root logger's
    # level. However, if the message coming in is at a higher level, we want to log at that level,
    # to better convey the urgency.
    root_logger = _log.getLogger()
    root_logger_level = root_logger.getEffectiveLevel()
    value.level = max(value.level, root_logger_level)

    _LOGGER.log(value.level, "%s", value.message, extra=extra)

    return _jrpcs.Success()


@_rjjc.cancellable_async_validated_jrpcs_method(_rpmr.JobNotification)
async def job_notification(
    context: Context, value: _rpmr.JobNotification
) -> _jrpcs.Result:
    _LOGGER.info("Got notification %s.", value)

    await context.send_notification(value)

    return _jrpcs.Success()
