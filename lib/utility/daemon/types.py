from dataclasses import dataclass
from typing import Any, Callable, Protocol, overload, Self, Type, TypeVar

class MessageNamespace(Protocol):
    @overload
    def define(self, message_id: str) -> Callable[[Type[Any]], Type[Any]]:
        ...

    @overload
    def define(self, message_id: str, message_cls: Type[Any]) -> Callable[[Type[Any]], Type[Any]]:
        ...

    def define(self, message_id: str, *args, **kwargs):
        ...

    def encode(self, message: Any) -> bytes:
        ...

    def decode(self, data: bytes) -> Any:
        ...


_Req = TypeVar("_Req", bound="Request")  # Request type
_Res = TypeVar("_Res", covariant=True)

class Request(Protocol[_Res]):
    ...

class DaemonClientRpc(Protocol):
    @property
    def connected(self: Self) -> bool:
        ...

    async def call(self: Self, message: Request[_Res], res_t: Type[_Res]) -> _Res:
        ...

    async def cast(self: Self, message: Any) -> None:
        ...

    async def connect(self: Self) -> None:
        ...

    async def disconnect(self: Self) -> None:
        ...

    async def __aenter__(self: Self) -> Self:
        ...

    async def __aexit__(self: Self, *args, **kwargs):
        ...

class ClientErrors:
    class Base(Exception):
        ...

    class CouldNotConnect(Base):
        ...

    class ResponseTimeout(Base):
        ...

    class AlreadyConnected(Base):
        ...

    class UnexpectedResponse(Base):
        def __init__(self, m: Any, *args, **kwargs):
            super().__init__(f'Unexpected Response {m}', *args, **kwargs)
            self.message = m

    class NotConnectedOnCall(Base):
        def __init__(self, m: Request[Any], *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.message = m

    class NotConnectedOnCast(Base):
        def __init__(self, m: Any, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.message = m

