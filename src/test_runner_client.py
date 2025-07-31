import logging as _log
import secrets as _sec
import socket as _soc

import pytest as _pt

import jrpc_methods as _jrpcm
import log_config as _clog
import runner_client_wrapper as _rcw


@_pt.mark.asyncio
async def test() -> None:
    _log.basicConfig(level=_log.INFO, format=_clog.LOG_FORMAT)
    _jrpcm.configure()

    _log.info("Logging is working!")

    host = f"{_soc.gethostname()}.local"
    wrapper = await _rcw.RunnerClientWrapper.create(host)

    client = wrapper.client
    try:
        simulation_id = _sec.token_hex(nbytes=5)

        variations_created = await client.create_variations(simulation_id, None)

        print(variations_created)
    finally:
        await wrapper.shut_down()
