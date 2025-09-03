import logging as _log
import secrets as _sec
import socket as _soc

import pytest as _pt

import scheduler.jrpc_methods as _jrpcm
import scheduler.log_config as _clog
import scheduler.runner.client_wrapper as _rcw


@_pt.mark.asyncio
async def test() -> None:
    _log.basicConfig(level=_log.INFO, format=_clog.LOCAL_LOG_FORMAT)
    _jrpcm.configure()

    _log.info("Logging is working!")

    host = f"{_soc.gethostname()}.local"
    wrapper = await _rcw.RunnerClientWrapper.create(host)

    client = wrapper.client
    try:
        job_id = _sec.token_hex(nbytes=5)

        variations_created = await client.create_variations(job_id, None)

        print(variations_created)
    finally:
        await wrapper.shut_down()
