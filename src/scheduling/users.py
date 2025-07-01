import collections.abc as _cabc
import datetime as _dt
import heapq as _heap

import resultes_pydantic_models.simulations.simulation as _psim


class User:
    def __init__(
        self, n_running_jobs: int, waiting_simulations: _cabc.Sequence[_psim.Simulation]
    ) -> None:
        if not waiting_simulations:
            raise ValueError("Waiting simulations mustn't be empty.")

        self.n_running_jobs = n_running_jobs

        def get_state_changed_on(simulation: _psim.Simulation) -> _dt.datetime:
            return simulation.state_changed_on

        self._waiting_simulations = sorted(
            waiting_simulations, key=get_state_changed_on
        )

    def get_oldest_waiting_simulation(self) -> _psim.Simulation:
        return self._waiting_simulations[0]

    def has_only_one_waiting_simulation(self) -> bool:
        return len(self._waiting_simulations) == 1

    def remove_oldest_waiting_simulation(self) -> None:
        self._waiting_simulations.pop(0)

    @property
    def _oldest_simulation_state_changed_on(self) -> _dt.datetime:
        return self._waiting_simulations[0].state_changed_on

    def __lt__(self, other: "User") -> bool:
        if self.n_running_jobs != other.n_running_jobs:
            return self.n_running_jobs < other.n_running_jobs

        return (
            self._oldest_simulation_state_changed_on
            < other._oldest_simulation_state_changed_on
        )


class UsersScheduler:
    def __init__(self, users: _cabc.Iterable[User]) -> None:
        self._users = list(users)
        _heap.heapify(self._users)

    def has_next_simulation(self) -> bool:
        return bool(self._users)

    def pop_next_simulation(self) -> _psim.Simulation:
        if not self.has_next_simulation():
            raise RuntimeError("No next simulation.")

        next_user = _heap.heappop(self._users)

        next_simulation = next_user.get_oldest_waiting_simulation()

        if not next_user.has_only_one_waiting_simulation():
            next_user.remove_oldest_waiting_simulation()
            _heap.heappush(self._users, next_user)

        return next_simulation
