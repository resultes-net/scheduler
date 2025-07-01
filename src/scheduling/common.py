import dataclasses as _dc
import datetime as _dt

import resultes_pydantic_models.common as _rpmc


@_dc.dataclass
class Job:
    id: str
    user_id: str
    started_on: _dt.datetime = _dc.field(default_factory=_rpmc.utc_now)

    def __post_init__(self) -> None:
        if not _rpmc.is_timezone_aware_in_past(self.started_on):
            raise ValueError(
                "Created on datetime must be in past and have explicit time zone information."
            )
