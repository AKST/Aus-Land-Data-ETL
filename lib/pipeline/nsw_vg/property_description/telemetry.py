import asyncio
from dataclasses import dataclass, field
from functools import reduce
from logging import getLogger
from multiprocessing import Queue as MpQueue
from typing import Self, Optional
import queue

from lib.service.clock import AbstractClockService
from lib.utility.format import fmt_time_elapsed

from .type import ParentMessage

@dataclass
class WorkerState:
    queued: int
    processed: int

    def __add__(self: Self, other: 'WorkerState') -> 'WorkerState':
        return WorkerState(
            queued=self.queued + other.queued,
            processed=self.processed + other.processed,
        )

    @staticmethod
    def sum(workers: list['WorkerState']) -> 'WorkerState':
        return sum(workers, start=WorkerState(0, 0))

    @property
    def status(self: Self) -> tuple[int, int, float]:
        p = float(self.processed) / float(self.queued)
        return self.queued, self.processed, p * 100

@dataclass
class Process:
    workers: dict[int, WorkerState]

    def summary(self: Self) -> WorkerState:
        return WorkerState.sum(list(self.workers.values()))

class Telemetry:
    _logger = getLogger(__name__)

    def __init__(self: Self,
                 start_time: float,
                 clock: AbstractClockService,
                 processes: dict[int, Process]) -> None:
        self.start_time = start_time
        self._clock = clock
        self._processes = processes

    @staticmethod
    def create(clock: AbstractClockService, workers: int, subworkers: int) -> 'Telemetry':
        procs = {
            j: Process({
                i: WorkerState(0, 0)
                for i in range(subworkers) })
            for j in range(workers) }
        return Telemetry(clock.time(), clock, procs)

    @property
    def summary(self: Self) -> WorkerState:
        return WorkerState.sum([p.summary() for p in self._processes.values()])

    def queued(self: Self, process: int, worker: int, amount: int):
        self._processes[process].workers[worker].queued += amount
        self._log()

    def completed(self: Self, process: int, worker: int, amount: int):
        self._processes[process].workers[worker].processed += amount
        self._log()

    def _log(self: Self):
        queued, processed, percent = self.summary.status
        tstr = fmt_time_elapsed(self.start_time, self._clock.time(), 'hms')
        self._logger.info(f"({tstr}) Processed {percent:.2f}%")

class TelemetryListener:
    _logger = getLogger(f'{__name__}.listener')
    _task: Optional[asyncio.Task] = None

    def __init__(self: Self,
                 queue: MpQueue,
                 telemetry: Telemetry) -> None:
        self._recv_q = queue
        self._telemetry = telemetry

    @staticmethod
    def create(telemetry: Telemetry) -> tuple[MpQueue, 'TelemetryListener']:
        queue: MpQueue = MpQueue()
        return queue, TelemetryListener(queue, telemetry)

    def listen(self: Self):
        if not self._task:
            self._task = asyncio.create_task(self._start_listening())

    def stop(self: Self):
        if self._task:
            self._task.cancel()
            self._task = None

    async def _start_listening(self: Self):
        async def next_message():
            """
            It is important to add this timeout otherwise we can
            block the parent thread from exiting.
            """
            try:
                return await asyncio.to_thread(self._recv_q.get, timeout=0.1)
            except queue.Empty:
                return None

        while True:
            message = await next_message()
            self._logger.debug(f"message received {message}")
            match message:
                case None:
                    continue
                case ParentMessage.Queued(p, w, amount):
                    self._telemetry.queued(p, w, amount)
                case ParentMessage.Processed(p, w, amount):
                    self._telemetry.completed(p, w, amount)
                case other:
                    self._logger.warn(f'unknown message {other}')
