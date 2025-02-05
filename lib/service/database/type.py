import abc
from typing import Any, Self, Protocol, Sequence

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

    async def commit(self: Self) -> None:
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


