import asyncio
import logging
import psutil
import os
import subprocess
from typing import AsyncIterator, Self

from lib.service.clock import ClockService
from lib.utility.process import AsyncProcessIter

from ._constants import EVAR_PROC_NAME
from .types import (
    DaemonCandidatePid,
    DaemonConnection,
    DaemonConnectionCfg,
    DaemonService,
    DaemonServiceErrors,
)

async def is_daemon(proc: psutil.Process, proc_tag: str) -> bool:
    if proc.status() == psutil.STATUS_ZOMBIE:
        return False
    try:
        # TODO make async
        env = proc.environ()
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return False
    return EVAR_PROC_NAME in env and env[EVAR_PROC_NAME] == proc_tag

class DaemonServiceImpl(DaemonService):
    _logger = logging.getLogger(__name__)

    def __init__(
        self: Self,
        clock: ClockService,
        host: str,
    ):
        self._host = host
        self._clock = clock

    async def find_daemon_candidates(self: Self, cfg: DaemonConnectionCfg) -> AsyncIterator[DaemonCandidatePid]:
        async for p in await AsyncProcessIter.create(['pid', 'environ', 'status']):
            if not await is_daemon(p, cfg.proc_tag):
                continue
            yield DaemonCandidatePid(cfg, p.pid, p)
            self._logger.warn(f"closing {p.pid}")
            p.kill()
        else:
            raise DaemonServiceErrors.ProcNotFound(cfg)

    async def find_connection_candidate(self: Self, candidate: DaemonCandidatePid) -> AsyncIterator[DaemonConnection]:
        process = psutil.Process(candidate.pid)
        start_time = self._clock.time()
        while self._clock.time() - start_time < candidate.cfg.timeout:
            # TODO make async
            for conn in process.net_connections(kind="inet"):
                if conn.status != "LISTEN":
                    continue

                try:
                    port_candidate = conn.laddr.port
                    reader, writer = await asyncio.open_connection("localhost", port_candidate)
                    yield DaemonConnection(reader, writer, candidate.pid, port_candidate)
                except ConnectionRefusedError as e:
                    self._logger.warn("while conecting", e)
                    continue
                except OSError as e:
                    self._logger.warn("while conecting", e)
                    continue

    async def start_daemon(self: Self, cfg: DaemonConnectionCfg) -> DaemonCandidatePid:
        metadata = {}
        metadata.update(os.environ.copy())
        metadata.update({EVAR_PROC_NAME: cfg.proc_tag})
        sub_process = subprocess.Popen(
            ["python", "-m", cfg.mod_name],
            env=metadata,
            preexec_fn=os.setsid,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        pid = sub_process.pid
        return DaemonCandidatePid(cfg, pid, psutil.Process(pid))


