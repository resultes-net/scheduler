import logging as _log

import jsonrpcserver as _jrpcs
import resultes_jsonrpc.jsonrpc.server as _rjjs

_LOGGER = _log.getLogger(__name__)


def configure() -> None:
    # This is a dummy method making sure that the import of this module is not flagged as "unused"
    pass


class Context(_rjjs.ContextBase):
    def __init__(self, ip_address: str) -> None:
        super().__init__()
        self.ip_address = ip_address


@_jrpcs.method()
def post_log_message(context: Context, level: int, message: str) -> _jrpcs.Result:
    extra = {"remote_ip": context.ip_address}

    # Make sure we see any message coming in from the runner (which messages the runner sends
    # can be configured on the runner), i.e. ensure that we never log below our root logger's
    # level. However, if the message coming in is at a higher level, we want to log at that level,
    # to better convey the urgency.
    root_logger = _log.getLogger()
    root_logger_level = root_logger.getEffectiveLevel()
    scheduler_level = max(level, root_logger_level)

    _LOGGER.log(scheduler_level, "%s", message, extra=extra)
    return _jrpcs.Success()
