import logging
from typing import List, Self

from lib.service.database import DatabaseService, DatabaseServiceImpl, DatabaseConfig
from lib.tooling.schema.config import schema_ns
from lib.tooling.schema.type import SchemaNamespace

class Application:
    def __init__(self: Self, db: DatabaseService) -> None:
        self.db = db

    async def count(self: Self, namespace: str, table: str) -> int:
        async with self.db.async_connect() as c, c.cursor() as cursor:
            await cursor.execute(f'SELECT COUNT(*) FROM {namespace}.{table}')
            results = await cursor.fetchone()
            return results[0]

    async def tables(self: Self, namespace: str) -> List[str]:
        async with self.db.async_connect() as c, c.cursor() as cursor:
            await cursor.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{namespace}'
            """)
            return [it[0] for it in await cursor.fetchall()]

async def run_count_for_schemas(db_conf: DatabaseConfig, packages: List[SchemaNamespace]):
    db = DatabaseServiceImpl.create(db_conf, 1)
    app = Application(db)
    logger = logging.getLogger(f'{__name__}.count')

    logger.info('# Row Count')
    for pkg in [schema for pkg in packages for schema in package_schemas(pkg)]:
        tables = await app.tables(pkg)
        for tlb in tables:
            count = await app.count(pkg, tlb)
            logger.info(f' - "{pkg}.{tlb}" {count} rows')

def package_schemas(package: SchemaNamespace) -> List[str]:
    match package:
        case 'nsw_vg': return [
            'nsw_vg',
            'nsw_vg_raw',
        ]
        case 'nsw_spatial': return [
            'nsw_spatial',
            'nsw_spatial_lppt_raw',
        ]
        case other: return [other]

if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--packages", nargs='*')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    db_conf = INSTANCE_CFG[args.instance].database
    packages = [s for s in schema_ns if s in args.packages] if args.packages else list(schema_ns)
    asyncio.run(run_count_for_schemas(db_conf, packages))

