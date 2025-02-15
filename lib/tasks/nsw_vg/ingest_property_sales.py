import asyncio
from datetime import datetime, date
from dataclasses import dataclass
from functools import reduce
import logging
from multiprocessing import Process, Queue as IpcQueue
from time import time
from typing import List, Optional, Self, Tuple
from pathlib import Path
from pprint import pprint
import resource
import os

from lib.pipeline.nsw_vg.property_sales.data import *
from lib.pipeline.nsw_vg.property_sales.file_format import PropertySalesRowParserFactory, BufferedFileReaderTextSource
from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG, PropertySalesIngestion
from lib.pipeline.nsw_vg.property_sales.orchestration import *
from lib.service.clock import ClockService
from lib.service.io import IoService, IoServiceImpl
from lib.service.database import *
from lib.service.uuid import *
from lib.utility.sampling import Sampler, SamplingConfig

from .config import NswVgTaskConfig
from ..fetch_static_files import Environment

ZIP_DIR = './_out_zip'

async def ingest_property_sales_rows(
    environment: Environment,
    clock: ClockService,
    io: IoService,
    config: NswVgTaskConfig.PsiIngest,
):
    worker_count = config.worker_count
    worker_config = config.worker_config
    parent_config = config.parent_config
    logger = logging.getLogger(f'{__name__}.ingest_property_sales_rows')
    sampler = Sampler[IngestionSample].create(
        clock,
        SamplingConfig(min_sample_delta=0.1),
        logging.getLogger(f'{__name__}.progress'),
        IngestionSample(),
    )

    q_recv: IpcQueue = IpcQueue()
    p_children: List[NswVgPsChildClient] = []
    try:
        for idx in range(0, worker_count):
            q_send: IpcQueue = IpcQueue()
            worker_args = (idx, worker_config, q_send, q_recv)
            p_child = Process(target=_child_proc_entry, args=worker_args)
            p_children.append(NswVgPsChildClient(q_send, p_child))
            p_child.start()
    except Exception as e:
        for p in p_children:
            p.terminate()
        raise e

    async with asyncio.TaskGroup() as tg:
        orchestrator = NswVgPsIngestionCoordinator(
            parent_config,
            sampler,
            q_recv,
            p_children,
            tg,
            io,
        )
        await orchestrator.process([
            *environment.sale_price_annual.links,
            *environment.sale_price_weekly.links,
        ])
    for p in p_children:
        p.kill()
    logger.debug('ingestion ended')

def _child_proc_entry(
    idx: int,
    worker_config: NswVgPsiWorkerConfig,
    recv_msgs: IpcQueue,
    send_msgs: IpcQueue,
) -> None:
    if worker_config.log_config:
        logging.basicConfig(
            level=logging.DEBUG if worker_config.log_config.debug_logs else logging.INFO,
            format=f'[{idx}]{worker_config.log_config.format}',
            datefmt=worker_config.log_config.datefmt)

    logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
    logging.debug(f'initalising child process #{idx}')

    asyncio.run(_child_main(worker_config, recv_msgs, send_msgs))

async def _child_main(
    config: NswVgPsiWorkerConfig,
    recv_msgs: IpcQueue,
    send_msgs: IpcQueue,
) -> None:
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)

    if config.file_limit:
        soft_limit = min(config.file_limit, hard_limit)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft_limit, hard_limit))

    file_limit = int(soft_limit * 0.8)
    io = IoServiceImpl.create(file_limit - config.db_pool_size)
    uuid = UuidServiceImpl()
    clock = ClockService()
    db = DatabaseServiceImpl.create(config.db_config, config.db_pool_size)

    try:
        await db.open()
        async with asyncio.TaskGroup() as tg:
            factory = PropertySalesRowParserFactory(
                io,
                uuid,
                BufferedFileReaderTextSource,
                config.parser_chunk_size,
            )
            ingestion = PropertySalesIngestion.create(
                db,
                tg,
                config.ingestion_config,
                config.db_batch_size,
            )
            server = NswVgPsChildServer(
                tg,
                ingestion,
                factory,
                ParentClient(
                    os.getpid(),
                    send_msgs,
                    threshold=1000,
                ),
            )

            server.start_ingestion()

            while not server.closing:
                message = await asyncio.to_thread(recv_msgs.get)
                await tg.create_task(server.on_message(message))
            await tg.create_task(server.flush())
        logging.info('this child has finished')
    except Exception as e:
        logging.error("this child has died")
        logging.exception(e)
        raise e
    finally:
        await db.close()

async def _cli_main(
    config: NswVgTaskConfig.PsiIngest,
    db_config: DatabaseConfig,
    file_limit: int,
    truncate: bool
) -> None:
    from lib.tooling.schema import create_schema_controller, SchemaCommand
    from ..fetch_static_files import get_session, initialise

    clock = ClockService()
    uuid = UuidServiceImpl()
    io = IoServiceImpl.create(file_limit)
    db = DatabaseServiceImpl.create(db_config, 1)

    async with get_session(io, 'psi') as session:
        environment = await initialise(io, session)

    if truncate:
        controller = create_schema_controller(io, db, uuid)
        await controller.command(SchemaCommand.truncate(
            ns='nsw_vg',
            ns_range=range(3, 4),
            cascade=True,
        ))

    await ingest_property_sales_rows(
        environment,
        clock,
        io,
        config,
    )

if __name__ == '__main__':
    import argparse
    import resource

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="Ingest NSW VG PSI")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--publish-min", type=int, default=None)
    parser.add_argument("--publish-max", type=int, default=None)
    parser.add_argument("--download-min", type=date.fromisoformat, default=None)
    parser.add_argument("--download-max", type=date.fromisoformat, default=None)
    parser.add_argument("--truncate-earlier", action='store_true', default=False)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--worker-debug", type=int, default=False)
    parser.add_argument("--worker-file-limit", type=int, default=None)
    parser.add_argument("--worker-db-pool-size", type=int, default=16)
    parser.add_argument("--worker-db-batch-size", type=int, default=1000)
    parser.add_argument("--worker-parser-chunk-size", type=int, default=8 * 2 ** 10)

    args = parser.parse_args()
    config_logging(worker=None, debug=args.debug)
    config_vendor_logging({'sqlglot', 'psycopg.pool'})

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    instance_cfg = INSTANCE_CFG[args.instance]

    config = NswVgTaskConfig.PsiIngest(
        worker_count=args.workers,
        worker_config=NswVgPsiWorkerConfig(
            db_config=instance_cfg.database,
            db_pool_size=args.worker_db_pool_size,
            db_batch_size=args.worker_db_batch_size,
            file_limit=args.worker_file_limit,
            ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
            parser_chunk_size=args.worker_parser_chunk_size,
            log_config=NswVgPsiWorkerLogConfig(
                debug_logs=args.worker_debug,
                datefmt='%Y-%m-%d %H:%M:%S',
                format=f'[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s'
            ),
        ),
        parent_config=NswVgPsiSupervisorConfig(
            target_root_dir=ZIP_DIR,
            publish_min=args.publish_min,
            publish_max=args.publish_max,
            download_min=args.download_min,
            download_max=args.download_max,
        ),
    )

    asyncio.run(_cli_main(
        config,
        instance_cfg.database,
        file_limit,
        args.truncate_earlier,
    ))
