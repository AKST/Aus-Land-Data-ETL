from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass, field
import psutil
from typing import AsyncIterator, Protocol, Self

@dataclass
class DaemonConnection:
    reader: StreamReader = field(repr=False)
    writer: StreamWriter = field(repr=False)
    pid: int
    port: int

@dataclass
class DaemonConnectionCfg:
    mod_name: str
    proc_tag: str
    timeout: float

@dataclass
class DaemonCandidatePid:
    cfg: DaemonConnectionCfg = field(repr=False)
    pid: int
    proc: psutil.Process = field(repr=False)

class DaemonService(Protocol):
    def find_daemon_candidates(self: Self, cfg: DaemonConnectionCfg) -> AsyncIterator[DaemonCandidatePid]:
        ...

    def find_connection_candidate(self: Self, candidate: DaemonCandidatePid) -> AsyncIterator[DaemonConnection]:
        ...

    async def start_daemon(self: Self, cfg: DaemonConnectionCfg) -> DaemonCandidatePid:
        ...

class DaemonServiceErrors:
    class T(Exception):
        pass

    class ProcNotFound(T):
        def __init__(self: Self, cfg: DaemonConnectionCfg, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.cfg = cfg

