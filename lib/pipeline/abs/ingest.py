import asyncio
import geopandas as gpd
import logging
from multiprocessing import Process
from typing import List, Self, Type
import pandas as pd

from lib.service.database import DatabaseService, DatabaseConfig

from .config import AbsIngestionConfig, AbsWorkerConfig, WorkerArgs, IngestionSource
from .constants import SCHEMA


def divide_list(lst, n):
    from itertools import islice
    it, size, remainder = iter(lst), len(lst) // n, len(lst) % n
    return [
        list(islice(it, size + 1 if i < remainder else size))
        for i in range(n)
    ]

class AbsIngestionSupervisor:
    _logger = logging.getLogger(f'{__name__}.AbsIngestionSupervisor')
    _db: DatabaseService
    zip_dir: str

    def __init__(self: Self, db: DatabaseService, zip_dir: str) -> None:
        self._db = db
        self.zip_dir = zip_dir

    async def ingest(self: Self, config: AbsIngestionConfig) -> None:
        processes = [
            Process(
                target=AbsIngestionWorker.run,
                args=(WorkerArgs(idx, ts, self.zip_dir, config.worker_config),),
            )
            for idx, ts in enumerate(divide_list([
                (layer_name, ingest_source)
                for ingest_source in config.ingest_sources
                for layer_name in ingest_source.layer_to_table.keys()
            ], config.worker_count))
        ]

        for process in processes:
            process.start()

        async with asyncio.TaskGroup() as tg:
            await asyncio.gather(*[
                tg.create_task(asyncio.to_thread(process.join))
                for process in processes
            ])

class AbsIngestionWorker:
    _db: DatabaseService
    _logger = logging.getLogger(f'{__name__}.AbsIngestionWorker')
    root_dir: str

    def __init__(self: Self,
                 db: DatabaseService,
                 root_dir: str):
        self._db = db
        self.root_dir = root_dir

    async def consume(self: Self, layer_name: str, source: IngestionSource) -> None:
        column_renames = source.database_column_names_for_dataframe_columns[layer_name]
        table_name = source.layer_to_table[layer_name]
        file_name = f'{self.root_dir}/{source.gpkg_export_path}'

        def run_in_thread():
            self._logger.debug(f'consuming {layer_name}')
            df = gpd.read_file(file_name, layer=layer_name)
            df = df.rename(columns=column_renames)
            df = df[list(column_renames.values())]

            if 'in_australia' in df:
                df['in_australia'] = df['in_australia'] == 'AUS'

            self._logger.debug(f'writing {layer_name}')
            df.to_postgis(table_name,
                          self._db.engine(),
                          schema='abs',
                          if_exists='append',
                          index=False)
            return len(df)

        df_size = await asyncio.to_thread(run_in_thread)

        async with self._db.async_connect() as conn:
            query = f"SELECT COUNT(*) FROM {SCHEMA}.{table_name}"
            cursor = await conn.execute(query)
            result = await cursor.fetchone()
            self._logger.info(f"Populated {SCHEMA}.{table_name} with {result[0]}/{df_size} rows.")

    @classmethod
    def run(cls: Type[Self], args: WorkerArgs) -> None:
        import logging

        worker_c = args.worker_config

        if worker_c.log_config:
            l_conf = worker_c.log_config
            logging.basicConfig(level=l_conf.level,
                                format=f'[{args.worker}]{l_conf.format}',
                                datefmt=l_conf.datefmt)

        async def start():
            db = DatabaseService.create(worker_c.db_config, worker_c.db_connections)
            worker = AbsIngestionWorker(db, args.source_root_dir)
            try:
                await db.open()
                async with asyncio.TaskGroup() as tg:
                    await asyncio.gather(*[
                        asyncio.create_task(worker.consume(layer_name, source))
                        for layer_name, source in args.tasks
                    ])
            finally:
                await db.close()

        asyncio.run(start())

