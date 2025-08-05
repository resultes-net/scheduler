import logging as _log
import pathlib as _pl

_MOUNT_DIR_PATH = _pl.Path(__file__).parents[2] / "config-cm"

_LOGGER = _log.getLogger(__name__)


def keepRunnersAlive() -> bool:
    return _read_bool_value("keepRunnersAlive")


def log_keep_runners_alive_explanation() -> None:
    _LOGGER.warning(
        "Not deleting runners because we were told to keep them alive in the scheduler/config-cm configMap."
    )


def run_debugger() -> bool:
    return _read_bool_value("runDebugger")


def log_run_debugger_explanation() -> None:
    _LOGGER.warning(
        "Running debugger because we were told to do so in the scheduler/config-cm configMap."
    )


def _read_bool_value(file_name: str) -> bool:
    file_path = _MOUNT_DIR_PATH / file_name

    value = file_path.read_text()

    if value == "true":
        return True
    elif value == "false":
        return False
    else:
        raise ValueError(
            f"Expected {file_path} to contain 'true' or 'false' but found {value}."
        )
