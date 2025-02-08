import logging
from lib.pipeline import gnaf
from lib.service.database import DatabaseServiceImpl, DatabaseService
from lib.service.io import IoService, IoServiceImpl

_logger = logging.getLogger(__name__)

async def ingest_gnaf(
    cfg: gnaf.GnafConfig,
    db: DatabaseService,
    io: IoService,
):
    async with db.async_connect() as c, c.cursor() as cursor:
        for script in [
            cfg.target.create_tables_sql,
            cfg.target.fk_constraints_sql,
            'sql/gnaf/tasks/move_gnaf_to_schema.sql',
        ]:
            _logger.info(f"running {script}")
            await cursor.execute(await io.f_read(script))
    await gnaf.ingest(cfg, io)

if __name__ == '__main__':
    import asyncio
    import argparse
    import logging
    import resource

    from lib.defaults import INSTANCE_CFG
    from lib.tooling.schema import SchemaCommand, create_schema_controller
    from lib.utility.logging import config_logging
    from .fetch_static_files import get_session, initialise

    parser = argparse.ArgumentParser(description="Initialise nswvg db schema")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--reset-schema", action='store_true', default=False)

    args = parser.parse_args()
    config_logging(worker=None, debug=args.debug)

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    instance_cfg = INSTANCE_CFG[args.instance]

    async def main() -> None:
        io = IoServiceImpl.create(file_limit)
        db = DatabaseServiceImpl.create(instance_cfg.database, 1)
        async with get_session(io, 'env-gnaf-cli') as session:
            env = await initialise(io, session)

        if env.gnaf.publication is None:
            raise ValueError('no gnaf publication')

        if args.reset_schema:
            controller = create_schema_controller(io, db)
            await controller.command(SchemaCommand.drop(ns='gnaf', cascade=True))
            await controller.command(SchemaCommand.create(ns='gnaf'))

        config = gnaf.GnafConfig(
            target=env.gnaf.publication,
            states=instance_cfg.gnaf_states,
            workers=args.workers,
            worker_config=gnaf.GnafWorkerConfig(
                db_config=instance_cfg.database,
                db_poolsize=1,
                batch_size=1000,
            ),
        )

        await ingest_gnaf(config, db, io)

    asyncio.run(main())
