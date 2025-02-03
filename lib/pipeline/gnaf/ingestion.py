import asyncio
import multiprocessing
from typing import Iterator, List

from lib.service.io import IoService
from .config import Config, WorkerConfig, WorkerTask
from .scheduler import Scheduler

_SCHEMA = 'gnaf'

async def ingest(config: Config, io: IoService):
    loop = asyncio.get_running_loop()
    scheduler = Scheduler(io)
    task_groups = await Scheduler(io).get_tasks(config)
    with multiprocessing.Pool(config.workers) as pool:
        result = pool.starmap_async(worker, [
            (i, config.worker_config, grp)
            for i, grp in enumerate(task_groups)
        ])
        await loop.run_in_executor(None, result.get)

def worker(id: int, config: WorkerConfig, tasks: List[WorkerTask]):
    import asyncio

    async def main() -> None:
        import csv
        import logging
        import os

        from lib.service.database import DatabaseServiceImpl
        from lib.utility.logging import config_logging
        db = DatabaseServiceImpl.create(config.db_config, config.db_poolsize)

        config_logging(worker=id, debug=False)
        logger = logging.getLogger(__name__)

        for task in tasks:
            table_name, file = task.table_name, task.file_source
            with db.connect() as conn, conn.cursor() as cursor:
                cursor.execute("SET session_replication_role = 'replica';")
                with open(file, 'r') as f:
                    logger.info(f"Loading {os.path.basename(file)}")
                    reader = csv.reader(f, delimiter='|')
                    headers = next(reader)
                    insert_query = f"""
                        INSERT INTO {_SCHEMA}.{table_name} ({', '.join(headers)})
                        VALUES ({', '.join(['%s'] * len(headers))})
                        ON CONFLICT DO NOTHING
                    """

                    for batch_index, batch in enumerate(_get_batches(config.batch_size, reader)):
                        try:
                            cursor.executemany(insert_query, batch)
                        except Exception as e:
                            logger.error(f"Error inserting batch {batch_index + 1} into {table_name}: {e}")
                            raise e
                    conn.commit()
                cursor.execute("SET session_replication_role = 'origin';")
            logger.info(f"Loaded {os.path.basename(file)}")
        logger.info(f"DONE")
    asyncio.run(main())

def _get_batches(batch_size: int, reader) -> Iterator[List[str]]:
    batch = []
    for row in reader:
        row = [(None if v == "" else v) for v in (v.strip() for v in row)]
        batch.append(row)

        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
