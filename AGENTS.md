# Guidance for agents working on `scheduler`

For the overall ResulTES architecture, dependency management and deployment, read the org-wide guide first:
https://github.com/resultes-net/issues/blob/main/AGENTS.md. This file only adds what's specific to this repo.

## Environment
1. Python 3.12, venv in `venv/` (separate from `server`'s venv; e.g. `aiohttp` is only installed here).
1. Entry point: `src/main.py`. Configured via env vars, e.g. `SERVER_HOST`/`SERVER_PORT` (internal server, default
   `localhost:8000`), `RUNNER_PORT`, `USE_OPENSTACK`, `POLLING_PERIOD_SECONDS`, `LOG_LEVEL`.

## Submodules and `pydantic-models`
1. Git submodules: `pydantic-models`, `openstack-utils`, `jsonrpc`, `dev-utils`, `docker-utils`.
1. The scheduler parses the internal server's responses (e.g. `resultes_pydantic_models.simulations.simulation.Simulation`)
   with its *own* `pydantic-models` checkout. When `server` changes those models incompatibly (e.g. adds a required field),
   this repo's `pydantic-models` must be moved to the same commit, or the scheduler fails validating every response. New
   fields it doesn't know are dropped silently instead (see the org-wide guide, which also describes how to get an unpushed
   `pydantic-models` commit from another checkout).

## Runner jobs
1. `scheduler.runner.client.RunnerClient` builds the runner jobs. Inputs are processed in order, so later inputs
   overwrite files of earlier ones.
1. The simulate-and-post-process jobs get their whole working directory from the create-variations job's results zip
   (glob `**`). Files that the simulation needs (e.g. weather data) therefore only need to be inputs of the
   create-variations job.

## Restarts and crashes
1. On startup, the scheduler sets all simulations and variations still running to `ERROR` (`_set_error_state_on_unknown_jobs`
   in `src/main.py`). A deployment or a crash therefore fails every running job.
1. An unhandled exception in any job ends the `TaskGroup` in `workflow.py` and crashes the scheduler
   (https://github.com/resultes-net/issues/issues/52). In Loki (see the org-wide guide), crashes show up as "Exception
   occurred: unhandled errors in a TaskGroup (1 sub-exception). Terminating.", and the jobs set to `ERROR` as "Found
   unknown/leftover simulation …" or "… variation …".
1. Deploying `server` or `client` doesn't fail running jobs in practice: the scheduler has kept running through every
   internal-server rollout so far, since a rollout starts the new pod before stopping the old one. There's a latent risk,
   though: `server_client.py` doesn't retry, so a single failed request during a rollout would crash the scheduler (see
   https://github.com/resultes-net/issues/issues/58). Only a scheduler restart or crash fails running jobs (see 1 and 2).

## Tests
1. `pytest.ini` sets `python_files = *.py`: tests live next to the code in regular modules.
1. The run image doesn't install pytest, so modules the scheduler imports must not import `pytest`. Tests that need it
   (`@pytest.mark.asyncio`, `pytest.raises`, fixtures) go in a separate `test_*.py` module next to the code.
1. Several tests need external services:
   1. `src/scheduler/runner/test_manager.py` needs OpenStack credentials (`OS_PASSWORD`, ...).
   1. `src/scheduler/server/test_server_client.py` needs the internal server on `localhost:8000`. It can be run locally
      against any database from `server/src`: `DB_PORT=<port> ../venv/bin/uvicorn internal_server:app --port 8000`
      (see `server/alembic/AGENTS.md` for a throwaway database).
