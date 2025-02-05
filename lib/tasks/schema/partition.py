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

@dataclass
class ConfigPartitionsCfg:
    partitions: int

_logger = logging.getLogger(__name__)

async def config_partitions(cfg: ConfigPartitionsCfg, db: DatabaseService, io: IoService):
    pg_class_w_ns = 'SELECT c.*, n.nspname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid'
    try:
        async with db.async_connect() as conn:
            async with await conn.execute(f'''
              SELECT c.nspname, c.relname, pt.partstrat, array_agg(a.attname)
                FROM pg_partitioned_table pt
                JOIN ({pg_class_w_ns}) c ON pt.partrelid = c.oid
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN unnest(pt.partattrs) AS attr_num(attnum) ON a.attnum = attr_num.attnum
                WHERE a.attnum > 0 -- Exclude system columns
                GROUP BY 1, 2, 3 ORDER BY 1, 2;
            ''') as cursor:
                tables = [PartitionedTable(*row) for row in await cursor.fetchall()]

            for t in tables:
                if t.partition_kind != 'h':
                    continue

                _logger.info(f'repartiting {t.schema}.{t.table}')
                async with await conn.execute(f'''
                SELECT c.nspname as schema_name, c.relname AS partition_name
                  FROM pg_inherits i
                  JOIN ({pg_class_w_ns}) c ON i.inhrelid = c.oid
                  JOIN ({pg_class_w_ns}) p ON i.inhparent = p.oid
                  WHERE p.relname = %s AND p.nspname = %s;
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
                        _logger.info(f'Making copy of data to {tmp_f.name}')
                        async for copy_out in copy:
                            await writer.write(copy_out)

                    for tp in t_old_partitions:
                        _logger.info(f'Dropping partition {tp.schema}.{tp.partition_name}')
                        await cursor.execute(f'DROP TABLE {tp.schema}.{tp.partition_name}')

                    for p_id in range(0, cfg.partitions):
                        query = f'''
                            CREATE TABLE {t.schema}.{t.table}_p{p_id}
                              PARTITION OF {t.schema}.{t.table}
                              FOR VALUES WITH (MODULUS {cfg.partitions}, REMAINDER {p_id})
                        '''
                        _logger.info(f'Creating partition {t.schema}.{t.table}_p{p_id} (MODULUS {cfg.partitions}, REMAINDER {p_id})')
                        await cursor.execute(query)

                    query = f'COPY {t.schema}.{t.table} FROM STDOUT WITH CSV'
                    async with cursor.copy(query) as copy_in:
                        _logger.info(f'Restoring data from {tmp_f.name}')
                        async for chunk in io.f_read_chunks(tmp_f.name):
                            await copy_in.write(chunk)
    except Exception as e:
        logging.exception(e)
        raise e

async def _cli_main(db_cfg: DatabaseConfig, partitions: int):
    db = DatabaseServiceImpl.create(db_cfg, 8)
    io = IoServiceImpl.create(None)
    await db.open()
    await config_partitions(ConfigPartitionsCfg(partitions), db, io)
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
    config_logging(worker=None, debug=args.debug, runtime_fmt='elapsed')
    logging.debug(args)

    instance_cfg = INSTANCE_CFG[args.instance]
    asyncio.run(_cli_main(
        instance_cfg.database,
        args.partitions,
    ))

