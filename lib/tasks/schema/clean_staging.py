import logging

from lib.service.io import IoService, IoServiceImpl
from lib.service.database import DatabaseServiceImpl, DatabaseService, DatabaseConfig

from lib.tooling.schema import (
    SchemaCommand,
    SchemaController,
    SchemaDiscovery,
)

_logger = logging.getLogger(__name__)

async def clean_staging_data(db: DatabaseService, io: IoService):
    _logger.info('cleaning staging data')
    discovery = SchemaDiscovery.create(io)
    controller = SchemaController(io, db, discovery)

    await controller.command(
        SchemaCommand.Truncate(ns='nsw_vg', range=range(2, 6), cascade=True),
    )

    await controller.command(
        SchemaCommand.Truncate(ns='nsw_spatial', range=range(2, 3), cascade=True),
    )
    _logger.info('staging data cleaned')


async def _main(db_cfg: DatabaseConfig):
    db = DatabaseServiceImpl.create(db_cfg, 1)
    io = IoServiceImpl.create(None)
    await clean_staging_data(db, io)

if __name__ == '__main__':
    import argparse
    import asyncio
    from lib.utility.logging import config_vendor_logging, config_logging
    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="clean staging data")
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--instance', type=int, required=True)

    args = parser.parse_args()
    config_vendor_logging(set())
    config_logging(worker=None, debug=args.debug)
    asyncio.run(_main(INSTANCE_CFG[args.instance].database))
