import abc
import asyncio
from dataclasses import dataclass
from logging import getLogger
from multiprocessing import Process
from multiprocessing.synchronize import Semaphore as MpSemaphore
import time
from typing import Dict, List, Optional, Self, Callable
import uuid

from lib.service.database import DatabaseService
from lib.pipeline.nsw_lrs.property_description.parse import parse_property_description_data

@dataclass
class QuantileRange:
    start: Optional[int]
    end: Optional[int]

@dataclass
class WorkerProcessConfig:
    worker_no: int
    quantiles: List[QuantileRange]

@dataclass
class _WorkerClient:
    proc: Process

    async def join(self: Self) -> None:
        await asyncio.to_thread(self.proc.join)

SpawnWorkerFn = Callable[[WorkerProcessConfig], Process]

class PropDescIngestionWorkerPool:
    _logger = getLogger(f'{__name__}.PropDescIngestionWorkerPool')
    _pool: Dict[int, _WorkerClient]
    _semaphore: MpSemaphore
    _spawn_worker_fn: SpawnWorkerFn

    def __init__(self: Self,
                 semaphore: MpSemaphore,
                 spawn_worker_fn: SpawnWorkerFn) -> None:
        self._pool = {}
        self._semaphore = semaphore
        self._spawn_worker_fn = spawn_worker_fn

    def spawn(self: Self, worker_no: int, quantiles: List[QuantileRange]) -> None:
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
    _logger = getLogger(f'{__name__}.PropDescIngestionSupervisor')
    _db: DatabaseService
    _worker_pool: PropDescIngestionWorkerPool

    def __init__(self: Self, db: DatabaseService, pool: PropDescIngestionWorkerPool) -> None:
        self._db = db
        self._worker_pool = pool

    async def ingest(self: Self, workers: int, sub_workers: int) -> None:
        no_of_quantiles = workers * sub_workers
        quantiles = await self._find_table_quantiles(workers, sub_workers)

        for q_id, quantile in quantiles.items():
            self._logger.debug(f"spawning {q_id}")
            self._worker_pool.spawn(q_id, quantile)

        self._logger.debug(f"Awaiting workers")
        await self._worker_pool.join_all()
        self._logger.debug(f"Done")

    async def _find_table_quantiles(self: Self, workers: int, sub_workers: int) -> Dict[int, List[QuantileRange]]:

        async with self._db.async_connect() as c, c.cursor() as cursor:
            no_of_quantiles = workers * sub_workers
            self._logger.info(f"Finding quantiles (count {no_of_quantiles})")
            await cursor.execute(f"""
                SELECT segment, MIN(source_id), MAX(source_id)
                FROM (SELECT source_id, NTILE({no_of_quantiles}) OVER (ORDER BY source_id) AS segment
                        FROM nsw_lrs.legal_description
                       WHERE legal_description_kind = '> 2004-08-17'
                         AND strata_lot_number IS NULL) t
                GROUP BY segment
                ORDER BY segment
            """)
            items = await cursor.fetchall()
            q_start = [None, *(row[1] for row in items[1:])]
            q_end = [*(row[2] for row in items[:-1]), None]

            qs = [
                QuantileRange(start=q_start[i], end=q_end[i])
                for i, row in enumerate(items)
            ]

            return {
                i: qs[i * sub_workers:(i + 1) * sub_workers]
                for i in range(workers)
            }

class PropDescIngestionWorker:
    _logger = getLogger(f'{__name__}.PropDescIngestionWorker')
    _semaphore: MpSemaphore
    _db: DatabaseService

    def __init__(self: Self, semaphore: MpSemaphore, db: DatabaseService) -> None:
        self._semaphore = semaphore
        self._db = db

    async def ingest(self: Self, quantiles: List[QuantileRange]) -> None:
        self._logger.info("Starting sub workers")
        tasks = [asyncio.create_task(self.worker(q)) for q in quantiles]
        await asyncio.gather(*tasks)
        self._logger.info("Finished ingesting")

    async def worker(self: Self, quantile: QuantileRange) -> None:
        limit = 100
        temp_table_name = f"q_{uuid.uuid4().hex[:8]}"

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            self._logger.info(f'creating temp table {temp_table_name}')
            await self.create_temp_table(quantile, temp_table_name, cursor)

            await cursor.execute(f"SELECT count(*) FROM pg_temp.{temp_table_name}")
            count = (await cursor.fetchone())[0]

            for offset in range(0, count, limit):
                self._logger.info(f"{temp_table_name}: {offset}/{count}")
                await self.ingest_page(conn, cursor, temp_table_name, offset, limit)

            self._logger.info(f"{temp_table_name}: DONE")
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

        rows = [
            (r[0], *parse_property_description_data(r[1]), r[2], r[3], r[4])
            for r in await cursor.fetchall()
        ]

        remains = [
            (remains, legal_description_id)
            for _, _, remains, legal_description_id, _, _ in rows
            if remains
        ]
        row_data = [
            (source, property_desc, property, effective_date)
            for source, property_desc, _, _, property, effective_date in rows
        ]

        try:
            await cursor.executemany("""
                INSERT INTO nsw_lrs.parcel (
                    parcel_id,
                    parcel_plan,
                    parcel_section,
                    parcel_lot)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (parcel_id) DO NOTHING;
            """, [
                (parcel.id, parcel.plan, parcel.section, parcel.lot)
                for source, property_desc, property, effective_date in row_data
                for parcel in property_desc.parcels.all
            ])
            await conn.commit()

            await cursor.executemany("""
                INSERT INTO nsw_lrs.property_parcel_assoc(
                    source_id,
                    effective_date,
                    property_id,
                    parcel_id,
                    partial)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, [
                (source, effective_date, property, parcel_id, partial)
                for source, property_desc, property, effective_date in row_data
                for parcel_id, partial in [
                    *((p.id, True) for p in property_desc.parcels.partial),
                    *((p.id, False) for p in property_desc.parcels.complete),
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
                                q: QuantileRange,
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
              FROM nsw_lrs.legal_description
              LEFT JOIN meta.source_byte_position USING (source_id)
              LEFT JOIN meta.file_source USING (file_source_id)
             WHERE legal_description_kind = '> 2004-08-17'
               {f"AND source_id >= {q.start}" if q.start else ''}
               {f"AND source_id < {q.end}" if q.end else ''}
               AND strata_lot_number IS NULL;
        """)
        self._semaphore.release()
        time.sleep(0.01)

