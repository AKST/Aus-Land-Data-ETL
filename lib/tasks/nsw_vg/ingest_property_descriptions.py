import asyncio
from dataclasses import dataclass, field
import logging
from multiprocessing import Process, Semaphore as MpSemaphore, Queue as MpQueue
from multiprocessing.synchronize import Semaphore as SemaphoreT
from typing import Callable

from lib.pipeline.nsw_vg.property_description import (
    PropDescIngestionSupervisor,
    PropDescIngestionWorker,
    PropDescIngestionWorkerPool,
    ProcDescTelemetry,
    ProcDescTelemetryListener,
    PropDescWorkPartitioner,
    ProcUpdateMessage,
    WorkerProcessConfig,
)
from lib.service.clock import *
from lib.service.database import *
from lib.service.uuid import *
from lib.tasks.nsw_vg.config import NswVgTaskConfig
from lib.utility.logging import config_vendor_logging, config_logging

_logger = logging.getLogger(__name__)
_TARGET_TABLES = [
    f'nsw_lrs.{t}'
    for t in ['base_parcel', 'folio', 'property_folio', 'legal_description_remains']
]

async def cli_main(config: NswVgTaskConfig.PropDescIngest) -> None:
    db_service = DatabaseServiceImpl.create(config.db_config, config.workers)
    uuid = UuidServiceImpl()
    clock = ClockService()
    await ingest_property_description(db_service, uuid, clock, config)

async def ingest_property_description(
        db: DatabaseService,
        uuid: UuidService,
        clock: AbstractClockService,
        config: NswVgTaskConfig.PropDescIngest) -> None:
    semaphore = MpSemaphore(1)
    telemetry = ProcDescTelemetry.create(clock, config.workers, config.sub_workers)
    queue, telemetry_listener = ProcDescTelemetryListener.create(telemetry)
    partitioner = PropDescWorkPartitioner(db, config.workers, config.sub_workers)

    try:
        if config.truncate_earlier:
            _logger.info("Truncating earlier results")
            async with db.async_connect() as conn:
                for table in _TARGET_TABLES:
                    query = f'TRUNCATE TABLE {table} CASCADE;'
                    _logger.info(query)
                    await conn.execute(query)

        spawn_worker_with_worker_config = \
            lambda w_config: Process(target=spawn_worker, args=(
                queue, w_config, semaphore,
                config.worker_debug, config.db_config))

        telemetry_listener.listen()
        pool = PropDescIngestionWorkerPool(semaphore, spawn_worker_with_worker_config)
        parent = PropDescIngestionSupervisor(db, pool, partitioner)
        await parent.ingest(config.workers, config.sub_workers)
        telemetry_listener.stop()
    except Exception as e:
        _logger.error("Crashed")
        _logger.exception(e)
        raise e

def spawn_worker(
    queue: MpQueue,
    config: WorkerProcessConfig,
    semaphore: SemaphoreT,
    worker_debug: bool,
    db_config: DatabaseConfig,
):
    async def worker_runtime(config: WorkerProcessConfig, semaphore: SemaphoreT, db_config: DatabaseConfig):
        config_vendor_logging({'sqlglot', 'psycopg.pool'})
        config_logging(config.worker_no, worker_debug)
        db = DatabaseServiceImpl.create(db_config, len(config.quantiles))
        worker = PropDescIngestionWorker(config.worker_no, queue, semaphore, db)
        await worker.ingest(config.quantiles)
    asyncio.run(worker_runtime(config, semaphore, db_config))

if __name__ == '__main__':
    import argparse
    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--debug-worker", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--sub-workers", type=int, required=True)
    parser.add_argument("--truncate-earlier", action='store_true', default=False)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(None, args.debug)

    asyncio.run(cli_main(
        NswVgTaskConfig.PropDescIngest(
            truncate_earlier=args.truncate_earlier,
            worker_debug=args.debug_worker,
            workers=args.workers,
            sub_workers=args.sub_workers,
            db_config=INSTANCE_CFG[args.instance].database,
        ),
    ))

