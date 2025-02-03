import asyncio
from dataclasses import dataclass
import logging
from multiprocessing import Process, Queue as MpQueue
import resource

from lib.pipeline.nsw_vg.discovery import NswVgPublicationDiscovery
from lib.pipeline.nsw_vg.land_values.defaults import byo_land_values
from lib.pipeline.nsw_vg.land_values import (
    NswVgLvCoordinatorClient,
    NswVgLvAbstractCsvDiscovery,
    NswVgLvWebCsvDiscovery,
    NswVgLvByoCsvDiscovery,
    NswVgLvCsvDiscoveryConfig,
    NswVgLvCsvDiscoveryMode,
    NswVgLvIngestion,
    NswVgLvPipeline,
    NswVgLvTelemetry,
    NswVgLvWorker,
    NswVgLvWorkerClient,
)
from lib.service.clock import ClockService
from lib.service.io import IoService
from lib.service.database import *
from lib.service.http import AbstractClientSession
from lib.service.static_environment import StaticEnvironmentInitialiser
from lib.tasks.fetch_static_files import get_session
from lib.tooling.schema import SchemaController, SchemaDiscovery, SchemaCommand

from .config import NswVgTaskConfig

_ZIPDIR = './_out_zip'

async def cli_main(cfg: NswVgTaskConfig.LandValue.Main) -> None:
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(soft_limit * 0.8)
    io = IoService.create(file_limit)
    db = DatabaseServiceImpl.create(cfg.child_cfg.db_config, 1)
    clock = ClockService()

    async with get_session(io, 'lv') as session:
        await ingest_land_values(cfg, io, db, clock, session)

async def ingest_land_values(cfg: NswVgTaskConfig.LandValue.Main,
                    io: IoService,
                    db: DatabaseService,
                    clock: ClockService,
                    session: AbstractClientSession) -> None:
    static_env = StaticEnvironmentInitialiser.create(io, session)
    logger = logging.getLogger(f'{__name__}.spawn')
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    if cfg.truncate_raw_earlier:
        logger.info('dropping earlier raw data')
        await controller.command(SchemaCommand.Truncate(
            ns='nsw_vg',
            range=range(2, 3),
            cascade=True,
        ))
    recv_q: MpQueue = MpQueue()
    telemetry = NswVgLvTelemetry.create(clock)

    discovery_cfg = NswVgLvCsvDiscoveryConfig(cfg.discovery_mode, _ZIPDIR)
    discovery: NswVgLvAbstractCsvDiscovery
    match cfg.land_value_source:
        case 'web':
            discovery = NswVgLvWebCsvDiscovery(
                discovery_cfg,
                io,
                telemetry,
                NswVgPublicationDiscovery(session),
                static_env,
            )
        case 'byo':
            discovery = NswVgLvByoCsvDiscovery(
                discovery_cfg,
                io,
                telemetry,
                byo_land_values,
            )

    pipeline = NswVgLvPipeline(recv_q, telemetry, discovery)

    for id in range(0, cfg.child_n):
        send_q: MpQueue = MpQueue()
        proc = Process(target=spawn_worker, args=(id, cfg.child_cfg, recv_q, send_q))
        proc.start()
        pipeline.add_worker(NswVgLvWorkerClient(id, proc, send_q))

    await pipeline.start()

def spawn_worker(id: int,
                 cfg: NswVgTaskConfig.LandValue.Child,
                 send_q: MpQueue,
                 recv_q: MpQueue):

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(soft_limit * 0.8)

    async def runloop() -> None:
        logger = logging.getLogger(f'{__name__}.spawn')
        io = IoService.create(file_limit)
        db = DatabaseServiceImpl.create(cfg.db_config, cfg.db_conn)
        ingestion = NswVgLvIngestion(cfg.chunk_size, io, db)
        coordinator = NswVgLvCoordinatorClient(recv_q=recv_q, send_q=send_q)
        worker = NswVgLvWorker.create(id, ingestion, coordinator, cfg.db_conn * (2 ** 4))
        try:
            logger.debug('start worker')
            await worker.start(cfg.db_conn)
        except Exception as e:
            logger.exception(e)
            raise e

    logging.basicConfig(
        level=logging.DEBUG if cfg.debug else logging.INFO,
        format=f'[{id}][%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    asyncio.run(runloop())

if __name__ == '__main__':
    import argparse
    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="nsw vg lv ingestion")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--debug-worker", action='store_true', default=False)
    parser.add_argument('--land-value-src', choices=['byo', 'web'], default='byo')
    parser.add_argument('--discovery-mode', choices=['each-year', 'all', 'latest'], default=None)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--worker-db-conn", type=int, default=8)
    parser.add_argument("--worker-chunk-size", type=int, default=1000)
    parser.add_argument("--truncate-raw-earlier", action='store_true', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    db_config = INSTANCE_CFG[args.instance].database

    if args.discovery_mode:
        mode = NswVgLvCsvDiscoveryMode.from_text(args.discovery_mode)
    else:
        mode = INSTANCE_CFG[args.instance].nswvg_lv_discovery_mode

    cfg = NswVgTaskConfig.LandValue.Main(
        land_value_source=args.land_value_src,
        discovery_mode=mode,
        truncate_raw_earlier=args.truncate_raw_earlier,
        child_n=args.workers,
        child_cfg=NswVgTaskConfig.LandValue.Child(
            debug=args.debug_worker,
            db_conn=args.worker_db_conn,
            db_config=db_config,
            chunk_size=args.worker_chunk_size,
        ),
    )
    asyncio.run(cli_main(cfg))
