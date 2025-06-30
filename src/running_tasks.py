import collections.abc as _cabc
import dataclasses as _dc
import datetime as _dt

import resultes_pydantic_models.common as _rpmc


@_dc.dataclass
class Task:
    id: str
    user_id: str
    created_on: _dt.datetime = _dc.field(default_factory=_rpmc.utc_now)

    def __post_init__(self) -> None:
        if not _rpmc.is_timezone_aware_in_past(self.created_on):
            raise ValueError(
                "Created on datetime must be in past and have explicit time zone information."
            )


@_dc.dataclass
class _RunningTask:
    task: Task
    runner: "_Runner"


@_dc.dataclass
class _Runner:
    ip_address: str
    n_max_tasks: int
    assigned_tasks: list[_RunningTask] = []

    def n_tasks(self) -> int:
        return len(self.assigned_tasks)

    def have_max_tasks(self) -> bool:
        return self.n_tasks() == self.n_max_tasks

    def is_idle(self) -> bool:
        return self.n_tasks() == 0


class RunningTasks:
    def __init__(self) -> None:
        self._runners = list[_Runner]()

    def have_all_runners_max_tasks(self) -> bool:
        return all(r.have_max_tasks() for r in self._runners)

    def add_runner(self, ip_address: str, n_max_tasks: int) -> None:
        runner = _Runner(ip_address, n_max_tasks)
        self._runners.append(runner)

    def add_task_and_get_handling_runner_ip_address(
        self, task_id: str, user_id: str
    ) -> str:
        if self.have_all_runners_max_tasks():
            raise ValueError("All runners are assigned the maximum amount of tasks.")

        runner_with_most_fewest_free_task_slots = (
            self._get_runner_with_fewest_free_task_slots()
        )

        task = Task(task_id, user_id)
        self._assign_task(task, runner_with_most_fewest_free_task_slots)

        return runner_with_most_fewest_free_task_slots.ip_address

    def _get_runner_with_fewest_free_task_slots(self):
        runners_with_free_task_slot = [
            r for r in self._runners if not r.have_max_tasks()
        ]

        sorted_runners_with_free_task_slot = sorted(
            runners_with_free_task_slot, key=_Runner.n_tasks, reverse=True
        )

        runner_with_fewest_free_task_slots = sorted_runners_with_free_task_slot[0]

        assert not runner_with_fewest_free_task_slots.have_max_tasks

        return runner_with_fewest_free_task_slots

    def _assign_task(self, task: Task, runner: _Runner) -> None:
        assigned_task = _RunningTask(task, runner)
        runner.assigned_tasks.append(assigned_task)

    def remove_completed_task(self, task_id: str) -> None:
        task = _get_single(
            a for r in self._runners for a in r.assigned_tasks if a.task.id == task_id
        )
        if not task:
            raise ValueError("No task with given id.", task_id)

        self._remove_task(task)

    def _remove_task(self, task: _RunningTask) -> None:
        runner = task.runner
        runner.assigned_tasks.remove(task)

    def get_idle_runner_ip_addresses(self) -> _cabc.Sequence[str]:
        return [r.ip_address for r in self._runners if r.is_idle()]

    def remove_runner(self, ip_address: str) -> None:
        runner = _get_single(r for r in self._runners if r.ip_address == ip_address)
        if not runner:
            raise ValueError("No runner with given IP.", ip_address)

        self._runners.remove(runner)


def _get_single[T](iterable: _cabc.Iterable[T]) -> T:
    ts = list(iterable)
    n = len(ts)

    if n == 0:
        raise ValueError("No value found.")
    elif n > 1:
        raise ValueError("More than one value found.")

    return ts[0]
