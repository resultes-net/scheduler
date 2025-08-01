import logging as _log
import pathlib as _pl

_MOUNT_DIR_PATH = _pl.Path(__file__).parents[2] / "config-cm"


def keepRunnersAlive() -> bool:
    file_name = "keepRunnersAlive"

    path = _MOUNT_DIR_PATH / file_name
    value = path.read_text()

    if value == "true":
        return True
    elif value == "false":
        return False
    else:
        raise ValueError(
            f"Expected {file_name} to contain 'true' or 'false' but found {value}."
        )


def log_explanation() -> None:
    _log.warning(
        "Not deleting runners because we were told to keep them alive in the scheduler/config-cm configMap."
    )
