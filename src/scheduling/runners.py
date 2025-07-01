import collections.abc as _cabc
import dataclasses as _dc

from . import common as _com


@_dc.dataclass
class _RunningJob:
    job: _com.Job
    runner: "_Runner"


@_dc.dataclass
class _Runner:
    ip_address: str
    n_max_jobs: int
    assigned_jobs: list[_RunningJob] = _dc.field(default_factory=lambda: [])

    def n_jobs(self) -> int:
        return len(self.assigned_jobs)

    def have_max_jobs(self) -> bool:
        return self.n_jobs() == self.n_max_jobs

    def is_idle(self) -> bool:
        return self.n_jobs() == 0


class Runners:
    def __init__(self) -> None:
        self._runners = list[_Runner]()

    def have_all_runners_max_jobs(self) -> bool:
        if not self._runners:
            return True
        
        return all(r.have_max_jobs() for r in self._runners)

    def add_runner(self, ip_address: str, n_max_jobs: int) -> None:
        runner = _Runner(ip_address, n_max_jobs)
        self._runners.append(runner)

    def start_job_and_get_handling_runner_ip_address(
        self, job_id: str, user_id: str
    ) -> str:
        if self.have_all_runners_max_jobs():
            raise ValueError("All runners are assigned the maximum amount of jobs.")

        runner_with_most_fewest_free_job_slots = (
            self._get_runner_with_fewest_free_job_slots()
        )

        job = _com.Job(job_id, user_id)
        self._assign_job(job, runner_with_most_fewest_free_job_slots)

        return runner_with_most_fewest_free_job_slots.ip_address

    def _get_runner_with_fewest_free_job_slots(self):
        runners_with_free_job_slot = [r for r in self._runners if not r.have_max_jobs()]

        sorted_runners_with_free_job_slot = sorted(
            runners_with_free_job_slot, key=_Runner.n_jobs, reverse=True
        )

        runner_with_fewest_free_job_slots = sorted_runners_with_free_job_slot[0]

        assert not runner_with_fewest_free_job_slots.have_max_jobs()

        return runner_with_fewest_free_job_slots

    def _assign_job(self, job: _com.Job, runner: _Runner) -> None:
        assigned_job = _RunningJob(job, runner)
        runner.assigned_jobs.append(assigned_job)

    def remove_completed_job(self, job_id: str) -> None:
        job = _get_single(
            a for r in self._runners for a in r.assigned_jobs if a.job.id == job_id
        )
        if not job:
            raise ValueError("No job with given id.", job_id)

        self._remove_job(job)

    def _remove_job(self, job: _RunningJob) -> None:
        runner = job.runner
        runner.assigned_jobs.remove(job)

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
