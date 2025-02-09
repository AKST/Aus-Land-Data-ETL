from dataclasses import dataclass, field
from functools import reduce
from logging import getLogger
from typing import Self, Tuple

from lib.service.clock import ClockService
from lib.utility.format import fmt_time_elapsed

@dataclass
class WorkerStatistics:
    allocated: int = field(default=0)
    completed: int = field(default=0)

    def __add__(self: Self, other: 'WorkerStatistics') -> 'WorkerStatistics':
        return WorkerStatistics(
            allocated=self.allocated + other.allocated,
            completed=self.completed + other.completed,
        )

    def __str__(self: Self) -> str:
        p = self.completed / self.allocated
        return f"Done {p:.2f}% In {self.allocated} Out {self.completed}"

@dataclass
class FileStatistics:
    rows_parsed: int
    rows_ingested: int
    size: int

    def __add__(self: Self, other: 'FileStatistics') -> 'FileStatistics':
        return FileStatistics(
            rows_parsed=self.rows_parsed + other.rows_parsed,
            rows_ingested=self.rows_ingested + other.rows_ingested,
            size=self.size+other.size,
        )

    def __str__(self: Self) -> str:
        p = self.rows_ingested / self.rows_parsed if self.rows_parsed else 0
        return f"{p * 100.0:.2f}% Read {self.rows_parsed} Saved {self.rows_ingested}"

class NswVgLvTelemetry:
    _logger = getLogger(__name__)
    _files: dict[str, FileStatistics]
    _workers: dict[int, WorkerStatistics]

    def __init__(self: Self, clock: ClockService, start_time: float):
        self._workers = {}
        self._files = {}
        self._clock = clock
        self._start_time = start_time

    @staticmethod
    def create(clock: ClockService) -> 'NswVgLvTelemetry':
        start_time: float = clock.time()
        return NswVgLvTelemetry(clock, start_time)

    def establish_worker(self: Self, id: int):
        self._workers[id] = WorkerStatistics()

    def record_work_allocation(self: Self, worker_id: int, size: int):
        self._workers[worker_id].allocated += size
        self._log_status("ALLOCATE")

    def record_work_completition(self: Self, worker_id: int, size: int):
        self._workers[worker_id].completed += size
        self._log_status("COMPLETE")

    def record_file_queue(self: Self, file: str, size: int):
        self._files[file] = FileStatistics(0, 0, size)
        self._log_status("QUEUE FILE")

    def record_file_parse(self: Self, file: str, rows: int):
        self._files[file].rows_parsed += rows
        self._log_status("PARSE FILE")

    def record_file_saved(self: Self, file: str, rows: int):
        self._files[file].rows_ingested += rows
        self._log_status("SAVED FILE")

    def get_total(self) -> Tuple[FileStatistics, WorkerStatistics]:
        return (
            reduce(lambda a, b: a + b, self._files.values()),
            reduce(lambda a, b: a + b, self._workers.values()),
        )


    def _log_status(self: Self, event: str):
        total, workers = self.get_total()
        t = fmt_time_elapsed(self._start_time, self._clock.time(), 'hms')
        self.total_state = total
        self._logger.info(f"{event.rjust(10)} ({t}) {str(total)}")
