from dataclasses import dataclass, field
from typing import List, Literal, Optional
import logging

from lib.service.clock import ClockService
from lib.service.database import *
from lib.service.io import IoService, IoServiceImpl
from lib.tasks.fetch_static_files import get_session

from ..fetch_static_files import Environment, get_session, initialise
from .config import NswVgTaskConfig
from .ingest_deduplicate import ingest_deduplicate
from .ingest_property_descriptions import ingest_property_description
from .ingest_property_sales import ingest_property_sales_rows
from .ingest_land_values import ingest_land_values

ZIP_DIR = './_out_zip'
_logger = logging.getLogger(__name__)

async def ingest_nswvg(
    environment: Environment,
    clock: ClockService,
    db: DatabaseService,
    io: IoService,
    config: NswVgTaskConfig.Ingestion,
):
    if config.load_raw_land_values:
        lv_conf = config.load_raw_land_values
        async with get_session(io, 'lv') as session:
            await ingest_land_values(lv_conf, io, db, clock, session)

    if config.load_raw_property_sales:
        await ingest_property_sales_rows(
            environment,
            clock,
            io,
            config.load_raw_property_sales,
        )

    if config.deduplicate:
        await ingest_deduplicate(db, io, clock, config.deduplicate)

    if config.property_descriptions:
        try:
            await ingest_property_description(db, config.property_descriptions)
        except Exception as e:
            _logger.exception(e)
            _logger.error('failed to ingest all property descriptions')

if __name__ == '__main__':
    import argparse
    import asyncio
    import os
    import resource
    import sys
    from datetime import date
    from lib.defaults import INSTANCE_CFG
    from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG, PropertySalesIngestion
    from lib.pipeline.nsw_vg.property_sales.orchestration import NswVgPsiSupervisorConfig, NswVgPsiWorkerConfig, NswVgPsiWorkerLogConfig
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="Ingest NSW VG Data")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--main-debug", action='store_true', default=False)
    parser.add_argument("--main-db-pool-size", type=int, default=8)

    parser.add_argument("--load-land-values", action='store_true', default=False)
    parser.add_argument("--lv-workers", action='store_true', default=1)
    parser.add_argument("--lv-worker-debug", action='store_true', default=False)
    parser.add_argument("--lv-worker-db-pool-size", type=int, default=1)
    parser.add_argument("--lv-worker-chunk-size", type=int, default=1000)
    parser.add_argument("--lv-truncate-earlier", action='store_true', default=False)

    parser.add_argument("--load-property-sales", action='store_true', default=False)
    parser.add_argument("--ps-publish-min", type=int, default=None)
    parser.add_argument("--ps-publish-max", type=int, default=None)
    parser.add_argument("--ps-download-min", type=date.fromisoformat, default=None)
    parser.add_argument("--ps-download-max", type=date.fromisoformat, default=None)
    parser.add_argument("--ps-workers", type=int, default=1)
    parser.add_argument("--ps-worker-debug", type=int, default=False)
    parser.add_argument("--ps-worker-file-limit", type=int, default=None)
    parser.add_argument("--ps-worker-db-pool-size", type=int, default=None)
    parser.add_argument("--ps-worker-db-batch-size", type=int, default=50)
    parser.add_argument("--ps-worker-parser-chunk-size", type=int, default=8 * 2 ** 10)

    parser.add_argument("--nswlrs-propdesc", action='store_true', default=False)
    parser.add_argument("--nswlrs-propdesc-workers", type=int, default=1)
    parser.add_argument("--nswlrs-propdesc-subworkers", type=int, default=1)
    parser.add_argument("--nswlrs-propdesc-child-debug", action='store_true', default=False)

    parser.add_argument("--dedup", action='store_true', default=False)
    parser.add_argument("--dedup-reinitialise-destination-schema", action='store_true', default=False)
    parser.add_argument("--dedup-initial-truncate", action='store_true', default=False)
    parser.add_argument("--dedup-drop-raw", action='store_true', default=False)
    parser.add_argument("--dedup-run-from", type=int, default=1)
    parser.add_argument("--dedup-run-till", type=int, default=12)

    parser.add_argument("--load-parcels", action='store_true', default=False)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.main_debug)
    logging.debug(args)

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    instance_cfg = INSTANCE_CFG[args.instance]

    load_land_values = None
    if args.load_land_values:
        load_land_values = NswVgTaskConfig.LandValue.Main(
            land_value_source='byo',
            discovery_mode=instance_cfg.nswvg_lv_discovery_mode,
            truncate_raw_earlier=args.lv_truncate_earlier,
            child_n=args.lv_workers,
            child_cfg=NswVgTaskConfig.LandValue.Child(
                debug=args.lv_worker_debug,
                db_conn=args.lv_worker_db_pool_size,
                db_config=instance_cfg.database,
                chunk_size=args.lv_worker_chunk_size,
            ),
        )


    load_raw_property_sales = None
    if args.load_property_sales:
        required_args = [
            'ps_workers',
            'ps_worker_db_pool_size'
        ]

        missing_args = [arg for arg in required_args if getattr(args, arg) is None]
        if missing_args:
            print(f"Error: The following arguments are required when --a is provided: {', '.join(missing_args)}")
            sys.exit(1)

        load_raw_property_sales = NswVgTaskConfig.PsiIngest(
            worker_count=args.ps_workers,
            worker_config=NswVgPsiWorkerConfig(
                db_config=instance_cfg.database,
                db_pool_size=args.ps_worker_db_pool_size,
                db_batch_size=args.ps_worker_db_batch_size,
                file_limit=args.ps_worker_file_limit,
                ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
                parser_chunk_size=args.ps_worker_parser_chunk_size,
                log_config=NswVgPsiWorkerLogConfig(
                    debug_logs=args.ps_worker_debug,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format=f'[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s'
                ),
            ),
            parent_config=NswVgPsiSupervisorConfig(
                target_root_dir=ZIP_DIR,
                publish_min=args.ps_publish_min,
                publish_max=args.ps_publish_max,
                download_min=args.ps_download_min,
                download_max=args.ps_download_max,
            ),
        )

    deduplicate = None
    if args.dedup:
        deduplicate = NswVgTaskConfig.Dedup(
            run_from=args.dedup_run_from,
            run_till=args.dedup_run_till,
            truncate=args.dedup_initial_truncate,
            drop_raw=args.dedup_drop_raw,
            drop_dst_schema=args.dedup_reinitialise_destination_schema,
        )

    property_description_config = None
    if args.nswlrs_propdesc:
        property_description_config = NswVgTaskConfig.PropDescIngest(
            db_config=instance_cfg.database,
            worker_debug=args.nswlrs_propdesc_child_debug,
            workers=args.nswlrs_propdesc_workers,
            sub_workers=args.nswlrs_propdesc_subworkers,
        )

    config = NswVgTaskConfig.Ingestion(
        load_raw_land_values=load_land_values,
        load_raw_property_sales=load_raw_property_sales,
        deduplicate=deduplicate,
        property_descriptions=property_description_config,
    )

    async def _cli_main(
        file_limit: int,
        parent_db_pool_limit: int,
        parent_db_config: DatabaseConfig,
        config: NswVgTaskConfig.Ingestion,
    ) -> None:
        clock = ClockService()
        io = IoServiceImpl.create(file_limit)
        db = DatabaseServiceImpl.create(
            parent_db_config,
            parent_db_pool_limit,
        )

        async with get_session(io, 'env-nswvg-cli') as session:
            environment = await initialise(io, session)

        try:
            await db.open()
            await ingest_nswvg(
                environment,
                clock,
                db,
                io,
                config,
            )
        finally:
            await db.close()

    asyncio.run(_cli_main(
        file_limit,
        args.main_db_pool_size,
        instance_cfg.database,
        config,
    ))

