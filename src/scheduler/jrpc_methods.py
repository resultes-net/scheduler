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
    _LOGGER.log(level, "%s - %s", context.ip_address, message)
    return _jrpcs.Success()
