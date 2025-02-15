from abc import ABC
from dataclasses import dataclass, fields, field
from lib.utility.daemon import MessageRegistry, msg_field

msg_registry = MessageRegistry()

class Message(ABC):
    ...

@msg_registry.register('req:base:close')
@dataclass
class CloseRequest(Message):
    ...

@msg_registry.register('req:base:handshake')
@dataclass
class HandshakeRequest(Message):
    ...

@msg_registry.register('res:base:handshake')
@dataclass
class HandshakeResponse(Message):
    ...

@msg_registry.register('req:app:echo')
@dataclass
class EchoRequest(Message):
    message: str = msg_field(1, str)

@msg_registry.register('res:app:echo')
@dataclass
class EchoResponse(Message):
    message: str = msg_field(1, str)


