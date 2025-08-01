import collections.abc as _cabc
import dataclasses as _dc
import datetime as _dt

import resultes_pydantic_models.common as _rpmc


@_dc.dataclass
class _RunningJob:
    id: str
    user_id: str
    runner: "_Runner"
    started_on: _dt.datetime = _dc.field(default_factory=_rpmc.utc_now, init=False)


@_dc.dataclass
class _Runner:
    def __init__(self, ip_address: str, n_max_jobs: int) -> None:
        self.ip_address = ip_address
        self.n_max_jobs = n_max_jobs
        self._assigned_jobs = list[_RunningJob]()

    def has_assigned_job(self, job_id: str) -> bool:
        job_ids = [j.id for j in self._assigned_jobs]
        return job_id in job_ids

    def assign_job(self, *, job_id: str, user_id: str) -> None:
        job_ids = [j.id for j in self._assigned_jobs]
        if job_id in job_ids:
            raise ValueError("Job already assigned.")

        running_job = _RunningJob(job_id, user_id, self)

        self._assigned_jobs.append(running_job)

    def remove_completed_job(self, job_id: str) -> None:
        job = _get_single(j for j in self._assigned_jobs if j.id == job_id)
        self._assigned_jobs.remove(job)

    def get_n_jobs(self, user_id: str | None = None) -> int:
        relevant_jobs = (
            self._assigned_jobs
            if not user_id
            else [j for j in self._assigned_jobs if j.user_id == user_id]
        )

        return len(relevant_jobs)

    def have_max_jobs(self) -> bool:
        return self.get_n_jobs() == self.n_max_jobs

    def is_idle(self) -> bool:
        return self.get_n_jobs() == 0


class RunnersScheduler:
    def __init__(self) -> None:
        self._runners = list[_Runner]()

    def get_n_jobs(self, user_id: str) -> int:
        n_jobs = sum(r.get_n_jobs(user_id) for r in self._runners)
        return n_jobs

    def have_all_runners_max_jobs(self) -> bool:
        if not self._runners:
            return True

        return all(r.have_max_jobs() for r in self._runners)

    def add_runner(self, ip_address: str, n_max_jobs: int) -> None:
        runner = _Runner(ip_address, n_max_jobs)
        self._runners.append(runner)

    def assign_job_and_get_runner_ip_address(self, *, job_id: str, user_id: str) -> str:
        if self.have_all_runners_max_jobs():
            raise ValueError("All runners are assigned the maximum amount of jobs.")

        runner_with_most_fewest_free_job_slots = (
            self._get_runner_with_fewest_free_job_slots()
        )

        runner_with_most_fewest_free_job_slots.assign_job(
            job_id=job_id, user_id=user_id
        )

        return runner_with_most_fewest_free_job_slots.ip_address

    def _get_runner_with_fewest_free_job_slots(self):
        runners_with_free_job_slot = [r for r in self._runners if not r.have_max_jobs()]

        sorted_runners_with_free_job_slot = sorted(
            runners_with_free_job_slot, key=_Runner.get_n_jobs, reverse=True
        )

        runner_with_fewest_free_job_slots = sorted_runners_with_free_job_slot[0]

        assert not runner_with_fewest_free_job_slots.have_max_jobs()

        return runner_with_fewest_free_job_slots

    def remove_completed_job(self, job_id: str) -> None:
        for runner in self._runners:
            if runner.has_assigned_job(job_id):
                runner.remove_completed_job(job_id)
                return

        raise ValueError("Unknown job.", job_id)

    def _remove_job(self, job: _RunningJob) -> None:
        runner = job.runner
        runner.remove_completed_job(job.id)

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
