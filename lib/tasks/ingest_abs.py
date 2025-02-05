import asyncio


from lib.pipeline.abs import *
from lib.service.database import DatabaseService, DatabaseServiceImpl, DatabaseConfig
from lib.service.io import IoService, IoServiceImpl
from lib.tasks.schema.count import run_count_for_schemas
from lib.tooling.schema import SchemaController, SchemaDiscovery, SchemaCommand
from lib.tooling.schema.config import ns_dependency_order

_OUTDIR = './_out_zip'

async def ingest_all(config: AbsIngestionConfig,
                     db: DatabaseService,
                     io: IoService) -> None:
    """
    TODO make concurrent. Before I can do that I need to
    handle the schema initialisation more gracefully.
    """
    abs_ingestion = AbsIngestionSupervisor(db, _OUTDIR)

    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    await controller.command(SchemaCommand.Drop(ns='abs'))
    await controller.command(SchemaCommand.Create(ns='abs', omit_foreign_keys=True))
    await abs_ingestion.ingest(config)
    async with db.async_connect() as conn:
        clean_dzn_sql = await io.f_read('./sql/abs/tasks/clean_dzn_post_ingestion.sql')
        await conn.execute(clean_dzn_sql)
    await controller.command(SchemaCommand.AddForeignKeys(ns='abs'))

async def _main(
    config: AbsIngestionConfig,
    db_conf: DatabaseConfig,
    file_limit: int,
) -> None:
    db = DatabaseServiceImpl.create(db_conf, 4)
    io = IoServiceImpl.create(file_limit)

    async with get_session(io, 'env-abs-cli') as session:
        await initialise(io, session)
    try:
        await db.open()
        await ingest_all(config, db, io)
    finally:
        await db.close()
    await run_count_for_schemas(db_conf, ['abs'])

if __name__ == '__main__':
    import argparse
    import logging
    import resource

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    from .fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--worker-logs", action='store_true', default=False)
    parser.add_argument("--worker-db-connections", type=int, default=8)
    parser.add_argument("--debug", action='store_true', default=False)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.debug)

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    db_config = INSTANCE_CFG[args.instance].database
    config = AbsIngestionConfig(
        ingest_sources=[
            ABS_MAIN_STRUCTURES,
            NON_ABS_MAIN_STRUCTURES,
            INDIGENOUS_STRUCTURES,
        ],
        worker_count=args.workers,
        worker_config=AbsWorkerConfig(
            db_config=db_config,
            db_connections=args.worker_db_connections,
            enable_logging=args.worker_logs,
            enable_logging_debug=args.debug,
        ),
    )

    asyncio.run(_main(config, db_config, file_limit))

