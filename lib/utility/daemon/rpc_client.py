from abc import ABC
from asyncio import open_connection, StreamReader, StreamWriter
from dataclasses import dataclass
from typing import Self, Optional, TypeVar

from .standard_messages import Sys
from .types import (
    ClientErrors,
    DaemonClientConfig,
    DaemonClientRpc,
    MessageNamespace,
    Request,
)

@dataclass
class _Connection:
    reader: StreamReader
    writer: StreamWriter

_Response = TypeVar("_Response", covariant=True)

class BaseRcpClient(DaemonClientRpc):
    __conn: Optional[_Connection] = None

    def __init__(self: Self,
                 host: str,
                 port: int,
                 config: DaemonClientConfig,
                 namespace: MessageNamespace) -> None:
        self.host = host
        self.port = port
        self.config = config
        self.namespace = namespace

    @property
    def connected(self: Self) -> bool:
        return self.__conn is not None

    async def call(self: Self, m: Request[_Response], t: Type[_Response]) -> _Response:
        match self.__conn:
            case None:
                raise ClientErrors.NotConnectedOnCall(m)
            case _Connection(reader, writer):
                pass

        writer.write(self.namespace.encode(Sys.HandshakeReq()))
        await writer.drain()

        match self.namespace.decode(await reader.read(1024)):
            case response if isinstance(response, t):
                return response


    async def connect(self: Self) -> None:
        if self.__conn is not None:
            raise ClientErrors.AlreadyConnected()

        reader, writer = await open_connection(self.host, self.port)
        self.__conn = _Connection(reader, writer)
        resp = await self.call(Sys.HandshakeReq(), Sys.HandshakeAck)

    async def disconnect(self: Self) -> None:
        ...

    async def __aenter__(self: Self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self: Self, *args, **kwargs):
        await self.disconnect()

