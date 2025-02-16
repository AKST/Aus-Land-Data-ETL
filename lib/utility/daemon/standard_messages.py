from abc import ABC
from dataclasses import dataclass, fields, field
from .type import MessageNamespace


class Sys:
    class T(ABC):
        ...

    @dataclass
    class Disconnect(T):
        ...

    @dataclass
    class HandshakeReq(T):
        ...

    @dataclass
    class HandshakeAck(T):
        ...

def install_system_messages(ns: MessageNamespace):
    ns.define('sys:disconnect', Sys.Disconnect)
    ns.define('sys:handshake.req', Sys.HandshakeReq)
    ns.define('sys:handshake.ack', Sys.HandshakeAck)
