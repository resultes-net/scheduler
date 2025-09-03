from __future__ import annotations as _

import abc as _abc
import datetime as _dt
import typing as _tp

if _tp.TYPE_CHECKING:
    import scheduler.runner.client as _rc


class RunnableJobBase(_abc.ABC):   
    @property
    @_abc.abstractmethod
    def id(self) -> str:
        raise NotImplementedError()

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
