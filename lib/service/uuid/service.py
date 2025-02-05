from typing import Protocol
import uuid

class UuidService(Protocol):
    def get_uuid4_hex(self) -> str:
        ...

class UuidServiceImpl(UuidService):
    def get_uuid4_hex(self) -> str:
        return uuid.uuid4().hex

