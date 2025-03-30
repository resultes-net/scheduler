import datetime as _dt
import ipaddress as _ip
import pathlib as _pl
import uuid as _uuid

import pydantic as _pyd
import sqlmodel as _sqlm

from .database_utils import helpers as _dbh


class RunBase(_sqlm.SQLModel):
    variation_uuid: _uuid.UUID
    relative_deck_file_path: _pl.PureWindowsPath = _dbh.create_typed_field(_pl.PureWindowsPath)
    simulation_files: _pyd.HttpUrl = _dbh.create_typed_field(_pyd.HttpUrl)
    running_on: _ip.IPv4Address = _dbh.create_typed_field(_ip.IPv4Address)


class Run(RunBase, table=True):
    id: int | None = _sqlm.Field(default=None, primary_key=True)
    simulation_started_at: _dt.datetime = _sqlm.Field(default_factory=_dbh.utc_now)
