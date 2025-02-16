from typing import Any, Callable, Protocol, overload, Self, Type

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

class DaemonClientRpc(Protocol):
    async def connect(self: Self) -> None:
        ...

    def disconnect(self: Self) -> None:
        ...
