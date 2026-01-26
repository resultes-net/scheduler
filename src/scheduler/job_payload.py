import resultes_pydantic_models.runner as _rpmr

type JobPayload = _rpmr.JobProgress | _rpmr.JobSuccess | _rpmr.JobError
