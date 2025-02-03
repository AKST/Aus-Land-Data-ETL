import asyncio
from dataclasses import dataclass, field
from multiprocessing import Process, Semaphore
from multiprocessing.synchronize import Semaphore as SemaphoreT
from typing import Callable

from lib.pipeline.nsw_vg.property_description import *
from lib.service.database import *
from lib.tasks.nsw_vg.config import NswVgTaskConfig
from lib.utility.logging import config_vendor_logging, config_logging

async def cli_main(config: NswVgTaskConfig.PropDescIngest) -> None:
    db_service = DatabaseServiceImpl.create(config.db_config, config.workers)
    await ingest_property_description(db_service, config)

async def ingest_property_description(
        db: DatabaseService,
        config: NswVgTaskConfig.PropDescIngest) -> None:
    semaphore = Semaphore(1)
    spawn_worker_with_worker_config = lambda w_config: \
        Process(target=spawn_worker,
                args=(w_config, semaphore, config.worker_debug, config.db_config))
    pool = PropDescIngestionWorkerPool(semaphore, spawn_worker_with_worker_config)
    parent = PropDescIngestionSupervisor(db, pool)
    await parent.ingest(config.workers, config.sub_workers)

def spawn_worker(config: WorkerProcessConfig, semaphore: SemaphoreT, worker_debug: bool, db_config: DatabaseConfig):
    async def worker_runtime(config: WorkerProcessConfig, semaphore: SemaphoreT, db_config: DatabaseConfig):
        config_vendor_logging({'sqlglot', 'psycopg.pool'})
        config_logging(config.worker_no, worker_debug)
        db = DatabaseServiceImpl.create(db_config, len(config.quantiles))
        worker = PropDescIngestionWorker(semaphore, db)
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

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(None, args.debug)

    asyncio.run(cli_main(
        NswVgTaskConfig.PropDescIngest(
            worker_debug=args.debug_worker,
            workers=args.workers,
            sub_workers=args.sub_workers,
            db_config=INSTANCE_CFG[args.instance].database,
        ),
    ))

