import abc
from typing import Any, AsyncIterator, AsyncGenerator, Self, Protocol, Sequence, Optional

from .config import DatabaseConfig

class CopyLike(Protocol):
    async def write_row(self: Self, row: Sequence[Any]) -> None:
        ...

    async def write(self: Self, buffer: bytes | str) -> None:
        ...

    async def read(self: Self) -> bytes:
        ...

    def rows(self: Self) -> AsyncIterator[tuple[Any, ...]]:
        ...

    async def read_row(self: Self) -> Optional[tuple[Any, ...]]:
        ...

    def __aiter__(self) -> AsyncGenerator[bytes, None]:
        ...

    async def __aexit__(self: Self, *args, **kwargs):
        ...

    async def __aenter__(self: Self) -> Self:
        ...

class CursorLike(Protocol):
    async def __aexit__(self: Self, *args, **kwargs):
        ...

    async def __aenter__(self: Self) -> Self:
        ...

    async def execute(self: Self, sql: str, args: Sequence[Any] = []) -> None:
        ...

    async def executemany(self: Self, sql: str, values: list[list[Any]]) -> None:
        ...

    async def fetchone(self: Self) -> list[Any]:
        ...

    async def fetchall(self: Self) -> list[list[Any]]:
        ...

    async def abort(self: Self):
        ...

    def copy(self: Self, statement: str, params: list[str] | None = None) -> CopyLike:
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

class DatabaseService(Protocol):
    config: DatabaseConfig

    async def open(self: Self):
        ...

    async def close(self: Self):
        ...

    async def wait_till_running(self: Self, interval: int = 5, timeout: int = 60):
        ...

    def async_connect(self: Self) -> ConnectionLike:
        ...

