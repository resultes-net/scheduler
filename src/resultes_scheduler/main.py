import datetime as _dt
import json as _json
import logging as _log
import os as _os
import pprint as _pprint
import signal as _sig
import time as _time

import generated_client as _client

PERIOD = _dt.timedelta(seconds=10)

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"

_is_shutting_down = False


def on_sigterm(signal, stack_frame) -> None:
    _shutting_down = True


_sig.signal(_sig.SIGTERM, on_sigterm)


def _sleep_until(wakeup_time: _dt.datetime) -> None:
    now = _dt.datetime.now()
    if wakeup_time < now:
        _log.warning(
            "Wake up time %s is in the past (now = %s). Resetting to 10 seconds from now.",
            wakeup_time,
            now,
        )
        wakeup_time = now + PERIOD

    seconds_to_sleep = (wakeup_time - now).seconds

    _time.sleep(seconds_to_sleep)


def main(host: str, port: int) -> None:
    uri = f"http://{host}:{port}"

    config = _client.Configuration(host=uri)

    _log.info("...scheduler started.")

    with _client.ApiClient(config) as client:
        api = _client.DefaultApi(client)

        _log.info("Client created. Entering main loop.")

        next_wakeup_time = _dt.datetime.now() + PERIOD

        while not _is_shutting_down:
            simulations_by_user_id = api.get_simulations_waiting_for_variations_creation_by_user_id_simulations_get(
                state="waiting-for-variations-creation"
            )

            data = _pprint.pformat(simulations_by_user_id, indent=4)

            _log.info("Found the following simulations: %s\n", data)

            _sleep_until(next_wakeup_time)

            next_wakeup_time += PERIOD


if __name__ == "__main__":
    _log.basicConfig(format=LOG_FORMAT, level=_log.INFO)
    _log.info("Starting scheduler...")
    host = _os.environ.get("HOST", "localhost")
    port = int(_os.environ.get("PORT", "8000"))
    main(host, port)
