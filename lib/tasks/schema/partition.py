from dataclasses import dataclass
import logging
from typing import Literal

from lib.service.database import *
from lib.service.io import *

@dataclass
class PartitionedTable:
    schema: str
    table: str
    partition_kind: Literal['h', 'r', 'l']
    partition_props: list[str]

@dataclass
class TablePartition:
    schema: str
    partition_name: str

_logger = logging.getLogger(__name__)

async def partition(
    db: DatabaseService,
    io: IoService,
    partitions: int,
):
    try:
        async with db.async_connect() as conn:
            async with await conn.execute('''
              SELECT n.nspname AS schema_name,
                     c.relname AS partitioned_table,
                     pt.partstrat AS partition_strategy,
                     array_agg(a.attname) AS partition_keys
                FROM pg_partitioned_table pt
                JOIN pg_class c ON pt.partrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN unnest(pt.partattrs) AS attr_num(attnum) ON a.attnum = attr_num.attnum
                WHERE a.attnum > 0  -- Exclude system columns
                GROUP BY n.nspname, c.relname, pt.partstrat
                ORDER BY schema_name, partitioned_table;
            ''') as cursor:
                tables = [PartitionedTable(*row) for row in await cursor.fetchall()]

            for t in tables:
                _logger.info(f'repartiting {t.schema}.{t.table}')
                async with await conn.execute(f'''
                    SELECT cn.nspname as schema_name, c.relname AS partition_name
                      FROM pg_inherits i
                      JOIN pg_class c ON i.inhrelid = c.oid
                      JOIN pg_class p ON i.inhparent = p.oid
                      JOIN pg_namespace pn ON p.relnamespace = pn.oid
                      JOIN pg_namespace cn ON c.relnamespace = cn.oid
                      WHERE p.relname = %s AND pn.nspname = %s;
                ''', [t.table, t.schema]) as cursor:
                    t_old_partitions = [TablePartition(*row) for row in await cursor.fetchall()]

                async with (
                    conn.cursor() as cursor,
                    io.mk_tmp_file() as tmp_f,
                ):
                    query = f'COPY (SELECT * FROM {t.schema}.{t.table}) TO STDOUT WITH CSV'
                    async with (
                        io.f_writter(tmp_f.name) as writer,
                        cursor.copy(query) as copy,
                    ):
                        _logger.info(f'making copy of data to {tmp_f.name}')
                        async for copy_out in copy:
                            await writer.write(copy_out)

                    for tp in t_old_partitions:
                        _logger.info(f'dropping partition {tp.schema}.{tp.partition_name}')
                        await cursor.execute(f'DROP TABLE {tp.schema}.{tp.partition_name}')

                    for p_id in range(0, partitions):
                        await cursor.execute(f'''
                            CREATE TABLE {t.schema}.{t.table}_p{p_id}
                              PARTITION OF {t.schema}.{t.table}
                              FOR VALUES WITH (MODULUS {partitions}, REMAINDER {p_id})
                        ''')

                    query = f'COPY {t.schema}.{t.table} FROM STDOUT WITH CSV'
                    async with cursor.copy(query) as copy_in:
                        _logger.info(f'restoring data from {tmp_f.name}')
                        async for chunk in io.f_read_chunks(tmp_f.name):
                            await copy_in.write(chunk)
    except Exception as e:
        logging.exception(e)
        raise e

async def _cli_main(db_cfg: DatabaseConfig, partitions: int):
    db = DatabaseServiceImpl.create(db_cfg, 8)
    io = IoServiceImpl.create(None)
    await db.open()
    await partition(db, io, partitions)
    await db.close()


if __name__ == '__main__':
    import argparse
    import asyncio

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="Partition Tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--partitions", type=int, required=True)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.debug)
    logging.debug(args)

    instance_cfg = INSTANCE_CFG[args.instance]
    asyncio.run(_cli_main(
        instance_cfg.database,
        args.partitions,
    ))

