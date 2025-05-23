import collections.abc as _cabc

import aiohttp as _ahttp
import jsonrpcclient as _jrpcl
import pydantic as _pyd
import resultes_pydantic_models.simulations.parameters.ttes as _pttes


class RunnerClient:
    def __init__(self, prefix: str, session: _ahttp.ClientSession) -> None:
        self._prefix = prefix
        self._session = session

    async def create_variations(
        self, parameters: _pttes.TtesParameters
    ) -> _cabc.Sequence[str]:
        params = {"parameters": parameters.model_dump()}
        json = _jrpcl.request("create_variations", params)

        async with self._session.post(self._prefix, json=json) as response:
            parsed = _jrpcl.parse(await response.json())

            match parsed:
                case _jrpcl.Ok(result):
                    return result
                case _jrpcl.Error() as error:
                    raise RuntimeError(error)
                case _:
                    raise NotImplementedError()
