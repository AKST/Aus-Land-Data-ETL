import logging
from typing import List

from lib.service.clock import ClockService
from lib.service.database import *
from lib.service.io import IoService
from lib.tooling.schema import SchemaCommand, SchemaController, SchemaDiscovery
from lib.utility.format import fmt_time_elapsed

from .config import GisTaskConfig

all_scripts = [
    f'./sql/nsw_spatial/tasks/{script}.sql'
    for script in [
        'dedup_lot_layer',
        'dedup_prop_layer',
    ]
]


async def ingest_deduplication(
    db: DatabaseService,
    io: IoService,
    clock: ClockService,
    cfg: GisTaskConfig.Deduplication
):
    logger = logging.getLogger(__name__)

    run_from = cfg.run_from or 1
    run_till = cfg.run_till or len(all_scripts)
    if 1 > run_from or len(all_scripts) < run_from:
        raise ValueError(f'dedup run from {cfg.run_from} is out of scope')
    else:
        scripts = all_scripts[run_from - 1:run_till]

    discovery = SchemaDiscovery.create(io)
    controller = SchemaController(io, db, discovery)

    async def run_commands(commands: List[SchemaCommand.BaseCommand]):
        for c in commands:
            await controller.command(c)

    if cfg.truncate:
        await run_commands([
            SchemaCommand.Truncate(ns='nsw_lrs', cascade=True, range=range(4, 5)),
        ])

    async with (
        db.async_connect() as conn,
        conn.cursor() as cursor,
    ):
        start_time = clock.time()
        for i, script_path in enumerate(scripts):
            t = fmt_time_elapsed(start_time, clock.time(), format="hms")
            pos = (cfg.run_from or 1) + i
            _, short_name = script_path.split('tasks/')
            logger.info(f'({t}) running [#{pos}] {short_name}')
            await cursor.execute(await io.f_read(script_path))

    logger.info('finished deduplicating')

if __name__ == '__main__':
    import asyncio
    import argparse

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="Deduplicate GIS")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--run-from", type=int, default=1)
    parser.add_argument("--run-till", type=int, default=len(all_scripts))
    parser.add_argument("--truncate", action='store_true', default=False)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.debug)
    logging.debug(args)

    cli_conf = GisTaskConfig.Deduplication(
        run_from=args.run_from,
        run_till=args.run_till,
        truncate=args.truncate,
    )

    db_config = INSTANCE_CFG[args.instance].database

    async def _cli_main() -> None:
        clock = ClockService()
        io = IoService.create(None)
        db = DatabaseServiceImpl.create(db_config, 1)
        try:
            await db.open()
            await ingest_deduplication(db, io, clock, cli_conf)
        finally:
            await db.close()

    asyncio.run(_cli_main())

