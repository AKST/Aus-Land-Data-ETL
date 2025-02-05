from dataclasses import dataclass, field
from typing import Any, Self, Protocol, Sequence

from .type import DatabaseService, CursorLike, ConnectionLike

@dataclass
class MockDbState:
    fetchone_i: int = field(default=0)
    fetchall_i: int = field(default=0)
    fetchone_ret: list[list[Any]] = field(default_factory=lambda: [])
    fetchall_ret: list[list[list[Any]]] = field(default_factory=lambda: [])
    execute_args: list[tuple[str, Sequence[Any]]] = field(default_factory=lambda: [])
    executemany_args: list[tuple[str, list[list[Any]]]] = field(default_factory=lambda: [])

@dataclass
class MockCursor(CursorLike):
    state: MockDbState

    async def __aexit__(self: Self, *args, **kwargs):
        return

    async def __aenter__(self: Self) -> Self:
        return self

    async def execute(self: Self, sql: str, args: Sequence[Any] = []) -> None:
        self.state.execute_args.append((sql, args))

    async def executemany(self: Self, sql: str, values: list[list[Any]]) -> None:
        self.state.executemany_args.append((sql, values))

    async def fetchone(self: Self) -> list[Any]:
        return self.state.fetchone_ret[self.state.fetchone_i]

    async def fetchall(self: Self) -> list[list[Any]]:
        return self.state.fetchall_ret[self.state.fetchall_i]


@dataclass
class MockConnection(ConnectionLike):
    state: MockDbState

    @property
    def info(self: Self) -> Any:
        raise Exception()

    async def __aexit__(self: Self, *args, **kwargs):
        return

    async def __aenter__(self: Self) -> Self:
        return self

    async def commit(self: Self) -> None:
        return

    async def set_autocommit(self: Self, value: bool) -> None:
        return

    def cursor(self: Self) -> CursorLike:
        return MockCursor(self.state)

    async def execute(self: Self, sql: str, args: Sequence[Any] = []) -> CursorLike:
        return MockCursor(self.state)

@dataclass
class MockDatabaseService(DatabaseService):
    state: MockDbState = field(default_factory=lambda: MockDbState())

    async def open(self):
        return

    async def close(self):
        return

    async def wait_till_running(self):
        return

    def async_connect(self) -> ConnectionLike:
        return MockConnection(self.state)
