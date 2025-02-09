import logging
from typing import List

from lib.service.clock import ClockService
from lib.service.database import *
from lib.service.io import IoService, IoServiceImpl
from lib.tooling.schema import SchemaCommand, create_schema_controller
from lib.utility.format import fmt_time_elapsed

from .config import NswVgTaskConfig

all_scripts = [
    f'./sql/nsw_vg/tasks/from_raw_derive/{script}.sql'
    for script in [
        '001_identifiers',
        '002_source',
        '003_property',
        '004_addresses/001_from_land_values',
        '004_addresses/002_from_psi',
        '004_addresses/003_from_psi_archive',
        '004_addresses/004_rematerialize',
        '005_populate_lrs/001_setup',
        '005_populate_lrs/002_ingest_land_values/001_legal_descriptions',
        '005_populate_lrs/002_ingest_land_values/002_property_area',
        '005_populate_lrs/002_ingest_land_values/003_land_valuation',
        '005_populate_lrs/002_ingest_land_values/004_zone_observation',
        '005_populate_lrs/002_ingest_land_values/005_strata_plan',
        '005_populate_lrs/003_ingest_psi_post_2001/001_notice_of_sale',
        '005_populate_lrs/003_ingest_psi_post_2001/002_legal_description',
        '005_populate_lrs/003_ingest_psi_post_2001/003_property_area',
        '005_populate_lrs/003_ingest_psi_post_2001/004_primary_purpose',
        '005_populate_lrs/003_ingest_psi_post_2001/005_nature_of_property',
        '005_populate_lrs/003_ingest_psi_post_2001/006_zone_observation',
        '005_populate_lrs/004_ingest_psi_pre_2001',
        '005_populate_lrs/005_cleanup',
    ]
]

async def ingest_deduplicate(
    db: DatabaseService,
    io: IoService,
    clock: ClockService,
    config: NswVgTaskConfig.Dedup,
):
    logger = logging.getLogger(__name__)

    run_from = config.run_from or 1
    run_till = config.run_till or len(all_scripts)
    if 1 > run_from or len(all_scripts) < run_from:
        raise ValueError(f'dedup run from {config.run_from} is out of scope')
    else:
        scripts = all_scripts[run_from - 1:run_till]

    controller = create_schema_controller(io, db)

    async def run_commands(commands: List[SchemaCommand]):
        for c in commands:
            await controller.command(c)

    if config.truncate:
        await run_commands([
            SchemaCommand.truncate(ns='nsw_vg', cascade=True, ns_range=range(4, 6)),
            SchemaCommand.truncate(ns='nsw_gnb', cascade=True),
            SchemaCommand.truncate(ns='nsw_lrs', cascade=True),
            SchemaCommand.truncate(ns='nsw_planning', cascade=True),
            SchemaCommand.truncate(ns='meta', cascade=True),
        ])

    if config.drop_dst_schema:
        await run_commands([
            SchemaCommand.drop(ns='nsw_vg', ns_range=range(4, 6)),
            SchemaCommand.drop(ns='nsw_gnb'),
            SchemaCommand.drop(ns='nsw_lrs'),
            SchemaCommand.drop(ns='nsw_planning'),
            SchemaCommand.drop(ns='meta'),
            SchemaCommand.create(ns='meta'),
            SchemaCommand.create(ns='nsw_planning'),
            SchemaCommand.create(ns='nsw_lrs'),
            SchemaCommand.create(ns='nsw_gnb'),
            SchemaCommand.create(ns='nsw_vg', ns_range=range(4, 6)),
        ])


    async with (
        db.async_connect() as conn,
        conn.cursor() as cursor,
    ):
        start_time = clock.time()
        for i, script_path in enumerate(scripts):
            t = fmt_time_elapsed(start_time, clock.time(), format="hms")
            pos = (config.run_from or 1) + i
            _, short_name = script_path.split('from_raw_derive/')
            logger.info(f'({t}) running [#{pos}] {short_name}')
            await cursor.execute(await io.f_read(script_path))

    logger.info('finished deduplicating')

    await run_commands([
        SchemaCommand.reindex(ns='nsw_vg', allowed={'table'}),
        SchemaCommand.reindex(ns='nsw_gnb', allowed={'table'}),
        SchemaCommand.reindex(ns='nsw_lrs', allowed={'table'}),
        SchemaCommand.reindex(ns='nsw_planning', allowed={'table'}),
        SchemaCommand.reindex(ns='meta', allowed={'table'}),
    ])

    logger.info('finished reindexing')

    if config.drop_raw:
        raise NotImplementedError()

if __name__ == '__main__':
    import asyncio
    import argparse

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="Ingest NSW VG Data")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--db-pool-size", type=int, default=8)
    parser.add_argument("--reinitialise-destination-schema", action='store_true', default=False)
    parser.add_argument("--initial-truncate", action='store_true', default=False)
    parser.add_argument("--drop-raw", action='store_true', default=False)
    parser.add_argument("--run-from", type=int, default=1)
    parser.add_argument("--run-till", type=int, default=len(all_scripts))

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.debug)

    logging.debug(args)
    db_config = INSTANCE_CFG[args.instance].database
    config = NswVgTaskConfig.Dedup(
        run_from=args.run_from,
        run_till=args.run_till,
        truncate=args.initial_truncate,
        drop_raw=args.drop_raw,
        drop_dst_schema=args.reinitialise_destination_schema,
    )

    async def _cli_main() -> None:
        clock = ClockService()
        io = IoServiceImpl.create(None)
        db = DatabaseServiceImpl.create(
            db_config,
            args.db_pool_size,
        )
        try:
            await db.open()
            await ingest_deduplicate(db, io, clock, config)
        finally:
            await db.close()

    asyncio.run(_cli_main())

