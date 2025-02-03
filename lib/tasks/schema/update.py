from dataclasses import dataclass, field
import logging
from typing import List, Dict
from sys import maxsize

from lib.service.database import DatabaseServiceImpl, DatabaseService
from lib.service.io import IoService
from lib.tooling.schema import SchemaController, SchemaDiscovery, SchemaCommand
from lib.tooling.schema.config import ns_dependency_order, schema_ns
from lib.tooling.schema.type import SchemaNamespace

_logger = logging.getLogger(f'{__name__}.initialise_db_schema')

@dataclass
class UpdateSchemaConfig:
    packages: List[SchemaNamespace]
    range: Dict[SchemaNamespace, range | int] | int | range | None
    apply: bool = field(default=True)
    revert: bool = field(default=False)

async def update_schema(
    config: UpdateSchemaConfig,
    db: DatabaseService,
    io: IoService,
) -> None:
    _logger.info('initalising nsw_vg db schema')
    discovery = SchemaDiscovery.create(io)
    controller = SchemaController(io, db, discovery)

    ordered = [p for p in ns_dependency_order if p in config.packages]
    drop_list = reversed(ordered) if config.revert else []
    create_list = ordered if config.apply else []

    def get_range(ns: SchemaNamespace) -> range | None:
        if config.range is None:
            return None
        elif isinstance(config.range, int):
            return range(config.range, config.range + 1)
        elif isinstance(config.range, range):
            return config.range
        elif isinstance(config.range, dict):
            value = config.range[ns]
            if isinstance(value, int):
                return range(value, value + 1)
            elif isinstance(value, range):
                return value

    for ns in drop_list:
        try:
            r = get_range(ns)
            await controller.command(SchemaCommand.Drop(ns=ns, range=r, cascade=True))
        except Exception as e:
            logging.error(f'failed on dropping {ns}')
            raise e

    for ns in create_list:
        try:
            r = get_range(ns)
            command = SchemaCommand.Create(ns=ns, range=r)
            allowed_schemas = {'nsw_lrs', 'nsw_gnb'}
            await controller.command(SchemaCommand.Create(ns=ns, range=r))
        except Exception as e:
            logging.error(f'failed on creating {ns}')
            raise e

async def run_script(f: str, db: DatabaseService, io: IoService) -> None:
    async with db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute(await io.f_read(f))

async def nuke(db: DatabaseService, io: IoService) -> None:
    await update_schema(
        UpdateSchemaConfig(
            packages=ns_dependency_order,
            range=None,
            apply=True,
            revert=True,
        ),
        db,
        io,
    )

if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--debug", action='store_true', default=False)

    command = parser.add_subparsers(dest='command')

    task_parser = command.add_parser('task')
    task_parser.add_argument("--path", type=str, required=True)

    refine_parser = command.add_parser('schema')
    refine_parser.add_argument("--packages", nargs='*', required=True)
    refine_parser.add_argument("--range-min", type=int, required=True)
    refine_parser.add_argument("--range-max", type=int, required=True)
    refine_parser.add_argument("--enable-revert", action='store_true', default=False)
    refine_parser.add_argument("--disable-apply", action='store_true', default=False)

    nuke_parser = command.add_parser('nuke')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    psycopg_logger = logging.getLogger('psycopg')
    psycopg_logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    psycopg_logger = logging.getLogger('psycopg.sql_logger')
    psycopg_logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)
    db_conf = INSTANCE_CFG[args.instance].database

    main_logger = logging.getLogger(f'{__name__}::init')
    main_logger.debug(f'operating on instance #{args.instance}')
    main_logger.debug(f'db config {db_conf}')
    main_logger.debug(f'file limit {file_limit}')


    async def main(f) -> None:
        io = IoService.create(file_limit)
        db = DatabaseServiceImpl.create(db_conf, 1)
        try:
            await db.open()
            await f(db, io)
        finally:
            await db.close()

    f = None

    # TODO make single command
    match args.command:
        case 'schema':
            if args.packages:
                packages = [p for p in ns_dependency_order if p in args.packages]
            else:
                packages = ns_dependency_order

            config = UpdateSchemaConfig(
                packages=packages,
                range=range(args.range_min, args.range_max + 1),
                apply=not args.disable_apply,
                revert=args.enable_revert,
            )

            main_logger.debug(f'config {config}')
            f = lambda db, io: update_schema(config, db, io)
        case 'nuke':
            f = lambda db, io: nuke(db, io)
        case 'task':
            f = lambda db, io: run_script(args.path, db, io)
        case other:
            raise TypeError('unknown command')

    asyncio.run(main(f))

