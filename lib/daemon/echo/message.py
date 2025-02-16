from abc import ABC
from dataclasses import dataclass
from typing import Union, Self, Protocol

from lib.utility.daemon import (
    DaemonClientRpc,
    MessageRegistry,
    msg_field,
    Sys,
)

echo_ns = MessageRegistry.create()

class AppMessage(ABC):
    ...

Message = Union[AppMessage, Sys.T]

@echo_ns.define('req:app:echo')
@dataclass
class EchoRequest(AppMessage):
    message: str = msg_field(1, str)

@echo_ns.define('res:app:echo')
@dataclass
class EchoResponse(AppMessage):
    message: str = msg_field(1, str)


class EchoRpc(DaemonClientRpc, Protocol):
    async def echo(self: Self, msg: EchoRequest) -> EchoResponse:
        ...

