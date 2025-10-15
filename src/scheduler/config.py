import logging as _log
import pathlib as _pl

_MOUNT_DIR_PATH = _pl.Path(__file__).parents[2] / "config-cm"

_LOGGER = _log.getLogger(__name__)

_BECAUSE = "because we were told to do so in the scheduler/config-cm configMap"


def keep_runners_alive() -> bool:
    return _read_bool_value("keepRunnersAlive")


def log_keep_runners_alive_explanation() -> None:
    _LOGGER.warning("Not deleting runners %s.", _BECAUSE)


def run_debugger() -> bool:
    return _read_bool_value("runDebugger")


def log_run_debugger_explanation() -> None:
    _LOGGER.warning("Running debugger %s.", _BECAUSE)


def runner_shall_remove_completed_jobs() -> bool:
    return _read_bool_value("runnerShallRemoveCompletedJobs")


def log_runner_shall_not_remove_completed_jobs_explanation() -> None:
    _LOGGER.warning("Not removing completed jobs on runners %s.", _BECAUSE)


def runner_log_level() -> str:
    return _read_str_value("runnerLogLevel")


def log_runner_log_level_not_info_explanation() -> None:
    _LOGGER.warning("Runner log level not INFO %s.", _BECAUSE)


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


def _read_str_value(file_name: str) -> str:
    file_path = _MOUNT_DIR_PATH / file_name

    return file_path.read_text()
