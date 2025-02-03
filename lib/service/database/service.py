import abc
import asyncio
import psycopg
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import create_engine
import time
from typing import Any, Self, Optional, Protocol, overload, Sequence
import warnings

from .config import DatabaseConfig

class CursorLike(Protocol):
    async def __aexit__(self: Self, *args, **kwargs):
        pass

    async def __aenter__(self: Self) -> Self:
        pass

    async def execute(self: Self, sql: str, args: Sequence[Any] = []) -> None:
        ...

    async def executemany(self: Self, sql: str, values: list[list[Any]]) -> None:
        ...

    async def fetchone(self: Self) -> list[Any]:
        ...

    async def fetchall(self: Self) -> list[list[Any]]:
        ...

class ConnectionLike(Protocol):
    @property
    def info(self: Self) -> Any:
        ...

    async def __aexit__(self: Self, *args, **kwargs):
        ...

    async def __aenter__(self: Self) -> Self:
        ...

    async def commit(self: Self) -> Self:
        ...

    async def set_autocommit(self: Self, value: bool) -> None:
        ...

    def cursor(self: Self) -> CursorLike:
        pass

    async def execute(self: Self, sql: str, args: Sequence[Any] = []) -> CursorLike:
        ...

class DatabaseService(abc.ABC):
    @abc.abstractmethod
    async def open(self: Self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def close(self: Self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def wait_till_running(self: Self, interval: int = 5, timeout: int = 60):
        raise NotImplementedError()

    @abc.abstractmethod
    def async_connect(self: Self) -> ConnectionLike:
        raise NotImplementedError()

    @property
    def config(self: Self) -> DatabaseConfig:
        raise NotImplementedError()


class DatabaseServiceImpl(DatabaseService):
    config: DatabaseConfig

    def __init__(self: Self,
                 pool: AsyncConnectionPool,
                 pool_size: int,
                 config: DatabaseConfig) -> None:
        self.config = config
        self.pool_size = pool_size
        self._pool = pool

    @staticmethod
    def create(config: DatabaseConfig,
               pool_size: int) -> 'DatabaseServiceImpl':
        # The logging here assumes I'm creating
        # the pool outside the async runloop
        with warnings.catch_warnings():
            pool = AsyncConnectionPool(
                config.connection_str,
                min_size=pool_size,
            )
        return DatabaseServiceImpl(pool, pool_size, config)

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

