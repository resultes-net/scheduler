import logging as _log
import typing as _tp

import jsonrpcserver as _jrpcs

_LOGGER = _log.getLogger(__name__)


def configure() -> None:
    # This is a dummy method making sure that the import of this module is not flagged as "unused"
    pass


@_jrpcs.method()
def post_log_message(_: _tp.Any, level: int, message: str) -> _jrpcs.Result:
    _LOGGER.log(level, message)
    return _jrpcs.Success()
