import dataclasses as _dc
import pathlib as _pl


@_dc.dataclass
class Paths:
    trnexe_exe: _pl.PureWindowsPath
    python_exe: _pl.PureWindowsPath
