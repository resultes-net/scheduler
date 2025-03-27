import contextlib as _ctx
import typing as _tp

import fastapi as _fapi
import sqlmodel as _sqlm
import uvicorn as _uvc
import pydantic as _pyd

import resultes_scheduler.model as _model

sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}
engine = _sqlm.create_engine(sqlite_url, connect_args=connect_args)


def create_db_and_tables() -> None:
    _sqlm.SQLModel.metadata.create_all(engine)


def get_session() -> _tp.Iterable[_sqlm.Session]:
    with _sqlm.Session(engine) as session:
        yield session


SessionDep = _tp.Annotated[_sqlm.Session, _fapi.Depends(get_session)]


@_ctx.asynccontextmanager
async def lifespan(_: _fapi.FastAPI) -> _tp.AsyncIterable[None]:
    create_db_and_tables()
    yield


app = _fapi.FastAPI(lifespan=lifespan)


@app.post("/runs/")
def create_run(run_base: _model.RunBase, session: SessionDep) -> _model.Run:
    run = _model.Run.model_validate(run_base)
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


@app.get("/runs/")
def read_runs(
        session: SessionDep,
        offset: int = 0,
        limit: _tp.Annotated[int, _fapi.Query(le=100)] = 100,
) -> _tp.Sequence[_model.Run]:
    runs = session.exec(_sqlm.select(_model.Run).offset(offset).limit(limit)).all()
    return runs


@app.get("/runs/{run_id}")
def read_run(run_id: int, session: SessionDep) -> _model.Run:
    run = session.get(_model.Run, run_id)
    if not run:
        raise _fapi.HTTPException(status_code=404, detail="run not found")
    return run


@app.delete("/runs/{run_id}")
def delete_run(run_id: int, session: SessionDep) -> _pyd.JsonValue:
    run = session.get(_model.Run, run_id)
    if not run:
        raise _fapi.HTTPException(status_code=404, detail="run not found")
    session.delete(run)
    session.commit()
    return {"ok": True}


if __name__ == "__main__":
    _uvc.run(app, host="localhost", port=8000)
