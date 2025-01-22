import asyncio
import psycopg
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import create_engine
import time
from typing import Self, Optional
import warnings

from .config import DatabaseConfig

class DatabaseService:
    config: DatabaseConfig
    pool_size: int
    _pool: AsyncConnectionPool

    def __init__(self: Self,
                 pool: AsyncConnectionPool,
                 pool_size: int,
                 config: DatabaseConfig) -> None:
        self.config = config
        self.pool_size = pool_size
        self._pool = pool

    @staticmethod
    def create(config: DatabaseConfig,
               pool_size: int) -> 'DatabaseService':
        # The logging here assumes I'm creating
        # the pool outside the async runloop
        with warnings.catch_warnings():
            pool = AsyncConnectionPool(
                config.connection_str,
                min_size=pool_size,
            )
        return DatabaseService(pool, pool_size, config)

    async def open(self):
        await self._pool.open()

    async def close(self):
        await self._pool.close()

    def engine(self: Self):
        return create_engine(self.config.psycopg2_url)

    def connect(self: Self) -> psycopg.Connection:
        return psycopg.connect(
            dbname=self.config.dbname,
            **self.config.kwargs,
        )

    def async_connect(self: Self, timeout: Optional[float] = None):
        return self._pool.connection(timeout=timeout)

    async def wait_till_running(self: Self, interval=5, timeout=60):
        start_time = time.time()
        while True:
            try:
                conn = await psycopg.AsyncConnection.connect(
                    dbname='postgres',
                    **self.config.kwargs,
                )
                await conn.close()
                break
            except psycopg.OperationalError as e:
                if time.time() - start_time > timeout:
                    raise e
                await asyncio.sleep(interval)

