from __future__ import annotations as _

import abc as _abc
import datetime as _dt
import typing as _tp
import secrets as _secs

if _tp.TYPE_CHECKING:
    import scheduler.runner.client as _rc


class RunnableJobBase(_abc.ABC):
    def __init__(self) -> None:
        self.id = _secs.token_hex(nbytes=5)

    @property
    @_abc.abstractmethod
    def user_id(self) -> str:
        raise NotImplementedError()

    @property
    @_abc.abstractmethod
    def waiting_to_run_since(self) -> _dt.datetime:
        raise NotImplementedError()

    @_abc.abstractmethod
    async def set_started(self) -> None:
        raise NotImplementedError()

    @_abc.abstractmethod
    async def run(self, runner_client: _rc.RunnerClient) -> None:
        raise NotImplementedError()
