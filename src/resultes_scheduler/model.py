import datetime as _dt
import ipaddress as _ip
import pathlib as _pl
import typing as _tp
import uuid as _uuid

import pydantic as _pyd
import sqlalchemy as _sqla
import sqlmodel as _sqlm

_T = _tp.TypeVar("_T")
_TDeco = _tp.TypeVar("_TDeco", bound=_sqla.TypeDecorator)


def _create_type_decorator(clazz: _tp.Type[_T], length: int = 2048, key: str | None = None) -> _tp.Type[
    _sqla.TypeDecorator]:
    class TypeDecorator(_sqla.TypeDecorator):
        impl = _sqla.String(length)
        python_type = clazz

        def process_bind_param(self, value, dialect) -> str:
            return str(value)

        def process_result_value(self, value, dialect) -> _T:
            if key:
                return clazz(key=value)

            return clazz(value)

        def process_literal_param(self, value, dialect) -> str:
            return str(value)

    return TypeDecorator


def _create_typed_field(clazz: _tp.Type[_T], length: int = 2048, key: str | None = None) -> _sqlm.Field:
    decorator = _create_type_decorator(clazz, length, key)
    return _sqlm.Field(sa_type=decorator)


class IPv4AddressType(_sqla.TypeDecorator):
    impl = _sqla.String(2048)
    python_type = _ip.IPv4Address


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.UTC)


class RunBase(_sqlm.SQLModel):
    variation_uuid: _uuid.UUID
    relative_deck_file_path: _pl.PureWindowsPath = _create_typed_field(_pl.PureWindowsPath)
    simulation_files: _pyd.HttpUrl = _create_typed_field(_pyd.HttpUrl)
    running_on: _ip.IPv4Address = _create_typed_field(_ip.IPv4Address)


class Run(RunBase, table=True):
    id: int | None = _sqlm.Field(default=None, primary_key=True)
    simulation_started_at: _dt.datetime = _sqlm.Field(default_factory=_utc_now)
