import logging as _log
import typing as _tp

LOCAL_LOG_FORMAT = "%(process)d/%(thread)d/%(taskName)s: %(asctime)s - %(levelname)s - %(module)s - %(message)s"
REMOTE_LOG_FORMAT = "%(remote_ip)s: %(message)s"


class Formatter(_log.Formatter):
    def __init__(self) -> None:
        super().__init__()

        self._local_formatter = _log.Formatter(LOCAL_LOG_FORMAT)
        self._remote_formatter = _log.Formatter(REMOTE_LOG_FORMAT)

    @_tp.override
    def format(self, record: _log.LogRecord) -> str:
        if hasattr(record, "remote_ip"):
            return self._remote_formatter.format(record)

        return self._local_formatter.format(record)
