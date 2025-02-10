import abc
import asyncio
from dataclasses import dataclass
from logging import getLogger
from multiprocessing import Process, Queue as MpQueue
from multiprocessing.synchronize import Semaphore as MpSemaphore
import time
from typing import Dict, List, Optional, Self, Callable, Tuple
import uuid

from lib.service.database import DatabaseService
from lib.pipeline.nsw_lrs.property_description.parse import parse_property_description_data
from lib.pipeline.nsw_lrs.property_description import PropertyDescription
from .type import ParentMessage, PartitionSlice
from .work_partitioner import WorkPartitioner

@dataclass
class WorkerProcessConfig:
    worker_no: int
    quantiles: List[PartitionSlice]

@dataclass
class _WorkerClient:
    proc: Process

    async def join(self: Self) -> None:
        await asyncio.to_thread(self.proc.join)

SpawnWorkerFn = Callable[[WorkerProcessConfig], Process]

class PropDescIngestionWorkerPool:
    _logger = getLogger(f'{__name__}.Pool')
    _pool: Dict[int, _WorkerClient]
    _semaphore: MpSemaphore
    _spawn_worker_fn: SpawnWorkerFn

    def __init__(self: Self,
                 semaphore: MpSemaphore,
                 spawn_worker_fn: SpawnWorkerFn) -> None:
        self._pool = {}
        self._semaphore = semaphore
        self._spawn_worker_fn = spawn_worker_fn

    def spawn(self: Self, worker_no: int, quantiles: List[PartitionSlice]) -> None:
        worker_conf = WorkerProcessConfig(worker_no=worker_no, quantiles=quantiles)
        process = self._spawn_worker_fn(worker_conf)
        self._pool[worker_no] = _WorkerClient(process)
        process.start()

    async def join_all(self: Self) -> None:
        async with asyncio.TaskGroup() as tg:
            await asyncio.gather(*[
                tg.create_task(process.join())
                for process in self._pool.values()
            ])

class PropDescIngestionSupervisor:
    _logger = getLogger(f'{__name__}.Supervisor')
    _db: DatabaseService
    _worker_pool: PropDescIngestionWorkerPool

    def __init__(self: Self,
                 db: DatabaseService,
                 pool: PropDescIngestionWorkerPool,
                 partitioner: WorkPartitioner) -> None:
        self._db = db
        self._worker_pool = pool
        self._partitioner = partitioner

    async def ingest(self: Self, workers: int, sub_workers: int) -> None:
        no_of_quantiles = workers * sub_workers
        slices = await self._partitioner.find_partitions()

        for q_id, partition_slice in slices.items():
            self._logger.debug(f"spawning {q_id}")
            self._worker_pool.spawn(q_id, partition_slice)

        self._logger.debug(f"Awaiting workers")
        await self._worker_pool.join_all()
        self._logger.debug(f"Done")


class PropDescIngestionWorker:
    _logger = getLogger(f'{__name__}.Worker')

    def __init__(self: Self,
                 process_id: int,
                 queue: MpQueue,
                 semaphore: MpSemaphore,
                 db: DatabaseService) -> None:
        self.process_id = process_id
        self._queue = queue
        self._semaphore = semaphore
        self._db = db

    async def ingest(self: Self, partitions: List[PartitionSlice]) -> None:
        self._logger.debug("Starting sub workers")
        tasks = [asyncio.create_task(self.worker(i, q)) for i, q in enumerate(partitions)]
        await asyncio.gather(*tasks)
        self._logger.debug("Finished ingesting")

    async def worker(self: Self, worker_id: int, partition: PartitionSlice) -> None:
        limit = 100
        temp_table_name = f"q_{uuid.uuid4().hex[:8]}"

        def on_ingest_page(amount: int):
            pid, wid = self.process_id, worker_id
            self._queue.put(ParentMessage.Processed(pid, wid, amount))

        def on_ingest_queued(amount: int):
            pid, wid = self.process_id, worker_id
            self._queue.put(ParentMessage.Queued(pid, wid, amount))

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            self._logger.debug(f'creating temp table {temp_table_name}')
            await self.create_temp_table(partition, temp_table_name, cursor)

            await cursor.execute(f"SELECT count(*) FROM pg_temp.{temp_table_name}")
            count = (await cursor.fetchone())[0]
            on_ingest_queued(count)

            for offset in range(0, count, limit):
                await self.ingest_page(conn, cursor, temp_table_name, offset, limit)
                on_ingest_page(min(limit, count - offset))


            self._logger.debug(f"{temp_table_name}: DONE")
            await cursor.execute(f"""
                DROP TABLE pg_temp.{temp_table_name};
                SET session_replication_role = 'origin';
            """)

    async def ingest_page(self: Self,
                          conn,
                          cursor,
                          table_name: str,
                          offset: int,
                          limit: int) -> None:
        try:
            await cursor.execute(f"""
                SELECT source_id,
                       legal_description,
                       legal_description_id,
                       property_id,
                       effective_date
                  FROM pg_temp.{table_name}
                  LIMIT {limit} OFFSET {offset}
            """)
        except Exception as e:
            self._logger.error(e)
            raise e

        rows: List[Tuple[str, PropertyDescription, str, str, str, str]] = [
            (r[0], *parse_property_description_data(r[1]), r[2], r[3], r[4])
            for r in await cursor.fetchall()
        ]

        remains: List[Tuple[str, str]] = [
            (remains, legal_description_id)
            for _, _, remains, legal_description_id, _, _ in rows
            if remains
        ]
        row_data: List[Tuple[str, PropertyDescription, str, str]]  = [
            (source, property_desc, property, effective_date)
            for source, property_desc, _, _, property, effective_date in rows
        ]

        try:
            await cursor.executemany("""
                INSERT INTO nsw_lrs.base_parcel (base_parcel_id, base_parcel_kind)
                VALUES (nsw_lrs.get_base_parcel_id(%s),
                        nsw_lrs.get_base_parcel_kind(%s))
                ON CONFLICT (base_parcel_id) DO NOTHING;
            """, [
                (folio.id, folio.id)
                for source, property_desc, property, effective_date in row_data
                for folio in property_desc.folios.all
            ])
            await conn.commit()

            await cursor.executemany("""
                INSERT INTO nsw_lrs.folio (
                    folio_id,
                    folio_plan,
                    folio_section,
                    folio_lot,
                    base_parcel_id)
                VALUES (%s, %s, %s, %s, nsw_lrs.get_base_parcel_id(%s))
                ON CONFLICT (folio_id) DO NOTHING;
            """, [
                (folio.id, folio.plan, folio.section, folio.lot, folio.id)
                for source, property_desc, property, effective_date in row_data
                for folio in property_desc.folios.all
            ])
            await conn.commit()

            await cursor.executemany("""
                INSERT INTO nsw_lrs.property_folio(
                    source_id,
                    effective_date,
                    property_id,
                    folio_id,
                    base_parcel_id,
                    partial)
                VALUES (%s, %s, %s, %s, nsw_lrs.get_base_parcel_id(%s), %s)
                ON CONFLICT DO NOTHING
            """, [
                (source, effective_date, property, folio_id, folio_id, partial)
                for source, property_desc, property, effective_date in row_data
                for folio_id, partial in [
                    *((p.id, True) for p in property_desc.folios.partial),
                    *((p.id, False) for p in property_desc.folios.complete),
                ]
            ])
            if remains:
                await cursor.executemany("""
                    INSERT INTO nsw_lrs.legal_description_remains(
                        legal_description_remains,
                        legal_description_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, remains)
        except Exception as e:
            self._logger.error(e)
            raise e
        await conn.commit()


    async def create_temp_table(self: Self,
                                p: PartitionSlice,
                                temp_table_name: str,
                                cursor) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._semaphore.acquire)
        await cursor.execute(f"""
            SET session_replication_role = 'replica';

            CREATE TEMP TABLE pg_temp.{temp_table_name} AS
            SELECT source_id,
                   legal_description,
                   legal_description_id,
                   property_id,
                   effective_date
              FROM {p.src_table_name}
              LEFT JOIN meta.source_byte_position USING (source_id)
              LEFT JOIN meta.file_source USING (file_source_id)
             WHERE legal_description_kind = '> 2004-08-17'
               {f"AND property_id >= {p.start}" if p.start else ''}
               {f"AND property_id < {p.end}" if p.end else ''}
               AND strata_lot_number IS NULL;
        """)
        self._semaphore.release()
        time.sleep(0.01)

