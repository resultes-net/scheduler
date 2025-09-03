import collections.abc as _cabc
import datetime as _dt
import heapq as _heap

import scheduler.runnable_job_base as _jb


class UserJobs:
    def __init__(
        self, n_running_jobs: int, waiting_jobs: _cabc.Sequence[_jb.RunnableJobBase]
    ) -> None:
        if not waiting_jobs:
            raise ValueError("Waiting jobs mustn't be empty.")

        self.n_running_jobs = n_running_jobs

        def get_waiting_to_run_since(job: _jb.RunnableJobBase) -> _dt.datetime:
            return job.waiting_to_run_since

        self._waiting_jobs = sorted(waiting_jobs, key=get_waiting_to_run_since)

    @property
    def n_waiting_jobs(self) -> int:
        return len(self._waiting_jobs)

    def get_oldest_waiting_job(self) -> _jb.RunnableJobBase:
        return self._waiting_jobs[0]

    def has_only_one_waiting_job(self) -> bool:
        return len(self._waiting_jobs) == 1

    def remove_oldest_waiting_job(self) -> None:
        self._waiting_jobs.pop(0)

    @property
    def _oldest_job_waiting_since(self) -> _dt.datetime:
        oldest_job = self._waiting_jobs[0]
        return oldest_job.waiting_to_run_since

    def __lt__(self, other: "UserJobs") -> bool:
        if self.n_running_jobs != other.n_running_jobs:
            return self.n_running_jobs < other.n_running_jobs

        return self._oldest_job_waiting_since < other._oldest_job_waiting_since


class JobsScheduler:
    def __init__(self, all_user_jobs: _cabc.Iterable[UserJobs]) -> None:
        self._all_user_jobs = list(all_user_jobs)
        _heap.heapify(self._all_user_jobs)

    @property
    def n_runnable_jobs(self) -> int:
        return sum(u.n_waiting_jobs for u in self._all_user_jobs)

    def has_next_runnable_job(self) -> bool:
        return bool(self._all_user_jobs)

    def pop_next_runnable_job(self) -> _jb.RunnableJobBase:
        if not self.has_next_runnable_job():
            raise RuntimeError("No next job.")

        next_user_jobs = _heap.heappop(self._all_user_jobs)

        next_job = next_user_jobs.get_oldest_waiting_job()

        if not next_user_jobs.has_only_one_waiting_job():
            next_user_jobs.remove_oldest_waiting_job()
            _heap.heappush(self._all_user_jobs, next_user_jobs)

        return next_job
