import asyncio
import csv
from dataclasses import dataclass
from io import StringIO
from logging import getLogger
from multiprocessing import Queue as MpQueue
import queue
from typing import AsyncIterator, Self, Any

from lib.service.database import DatabaseService
from lib.service.io import IoService
from lib.service.uuid import UuidService

from .config import (
    NswVgLvTaskDesc,
    NswVgLvParentMsg,
    NswVgLvChildMsg,
    RawLandValueRow,
)

class NswVgLvWorker:
    _close_requested: bool = False
    _stopped: bool = False

    _logger = getLogger(__name__)
    _parse_q: asyncio.Queue[NswVgLvTaskDesc.Parse]
    _load_q: asyncio.Queue[NswVgLvTaskDesc.Load]

    def __init__(self: Self,
                 id: int,
                 ingestion: 'NswVgLvIngestion',
                 coordinator: 'NswVgLvCoordinatorClient',
                 load_q: asyncio.Queue[NswVgLvTaskDesc.Load]):
        self.id = id
        self._ingestion = ingestion
        self._coordinator = coordinator
        self._parse_q = asyncio.Queue()
        self._load_q = load_q

    @staticmethod
    def create(id: int,
               ingestion: 'NswVgLvIngestion',
               coordinator: 'NswVgLvCoordinatorClient',
               back_pressure: int):
        return NswVgLvWorker(id, ingestion, coordinator, asyncio.Queue(maxsize=back_pressure))

    async def start(self: Self, size: int):
        async def read_parse_messages() -> None:
            while self._keep_running():
                try:
                    t_desc = await asyncio.wait_for(self._parse_q.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                self._logger.debug(f'Running task {t_desc}')
                async for load_desc in self._ingestion.parse(t_desc):
                    m = NswVgLvParentMsg.FileRowsParsed(self.id, load_desc.file, len(load_desc.rows))
                    self._coordinator.send_msg(m)
                    await self._load_q.put(load_desc)

        async def read_load_messages() -> None:
            while self._keep_running():
                try:
                    t_desc = await asyncio.wait_for(self._load_q.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                self._logger.debug(f'Running task {t_desc}')
                await self._ingestion.load(t_desc)
                m = NswVgLvParentMsg.FileRowsSaved(self.id, t_desc.file, len(t_desc.rows))
                self._coordinator.send_msg(m)

        try:
            self._logger.debug(f'starting loop')
            await asyncio.gather(
                self._start_recv(),
                *[read_parse_messages() for i in range(0, size)],
                *[read_load_messages() for i in range(0, size)],
            )
        except Exception as e:
            self._stopped = True
            raise e

    async def _start_recv(self: Self):
        while not self._close_requested:
            message = await self._coordinator.recv_msg()
            self._logger.debug(f'received message, {message}')
            match message:
                case NswVgLvChildMsg.RequestClose():
                    self._close_requested = True
                case NswVgLvChildMsg.Ingest(task):
                    await self._parse_q.put(task)
                case None:
                    continue
                case other:
                    self._logger.warn(f'unknown message {other}')

    def _keep_running(self: Self):
        if self._stopped:
            return False
        if self._close_requested:
            return not self._parse_q.empty() \
                or not self._load_q.empty()
        return True

@dataclass
class NswVgLvCoordinatorClient:
    recv_q: MpQueue
    send_q: MpQueue

    def send_msg(self: Self, msg: NswVgLvParentMsg.Base):
        self.send_q.put(msg)

    async def recv_msg(self: Self):
        """
        It is important to add this timeout otherwise we can
        block the parent thread from exiting.
        """
        try:
            return await asyncio.to_thread(self.recv_q.get, timeout=0.1)
        except queue.Empty:
            return None

class NswVgLvIngestion:
    _logger = getLogger(f'{__name__}.NswVgLvIngestion')

    def __init__(self: Self,
                 chunk_size: int,
                 uuid: UuidService,
                 io: IoService,
                 db: DatabaseService):
        self.chunk_size = chunk_size
        self._uuid = uuid
        self._io = io
        self._db = db

    async def parse(self: Self, task: NswVgLvTaskDesc.Parse) -> AsyncIterator[NswVgLvTaskDesc.Load]:
        quasi_file = StringIO(await self._get_data(task.file))
        offset = 0
        batch = []
        for index, row_raw in enumerate(csv.DictReader(quasi_file)):
            batch.append(RawLandValueRow.from_row(
                row_raw,
                self._uuid.get_uuid4_hex(),
                index + 1,
                task.file,
                task.target.datetime,
            ))

            if len(batch) >= self.chunk_size:
                yield NswVgLvTaskDesc.Load(task.file, offset, batch)
                await asyncio.sleep(0)
                offset += self.chunk_size
                batch = []

        if batch:
            yield NswVgLvTaskDesc.Load(task.file, offset, batch)

    async def load(self: Self, task: NswVgLvTaskDesc.Load):
        column_str, values_str, values = get_load_values(task)

        try:
            async with self._db.async_connect() as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany(f"""
                        INSERT INTO nsw_vg_raw.land_value_row ( {column_str} )
                        VALUES ( {values_str} )
                    """, values)
        except Exception as e:
            self._logger.error(f'failed to ingest {task.file}')
            raise e

    async def _get_data(self: Self, f: str) -> str:
        try:
            return await self._io.f_read(f, encoding='utf-8')
        except UnicodeDecodeError:
            return await self._io.f_read(f, encoding='ISO-8859-1')

def get_load_values(task: NswVgLvTaskDesc.Load) -> tuple[str, str, list[list[Any]]]:
    columns = task.rows[0].db_columns()
    column_str = ', '.join(columns)
    values_str = ', '.join(['%s'] * len(columns))
    values = [[getattr(row, name) for name in columns] for row in task.rows]
    return column_str, values_str, values
