import struct
from dataclasses import dataclass, fields, field, Field
from typing import Any, Callable, Dict, List, Self, Type, TypeVar, Union, overload

from .standard_messages import install_system_messages
from .types import MessageNamespace

def msg_field(id: int, t: Type[Any], **kwargs) -> Any:
    """A decorator for fields that adds an 'id' to their metadata."""
    return field(metadata={ "type": t, "id": id, "skip": False }, **kwargs)

class MessageRegistry(MessageNamespace):
    _encode_registry: dict[Type[Any], str]
    _decode_registry: dict[str, Type[Any]]

    def __init__(self):
        self._encode_registry = {}
        self._decode_registry = {}

    @classmethod
    def create(Cls) -> 'MessageRegistry':
        registry = Cls()
        install_system_messages(registry)
        return registry

    @overload
    def define(self, message_id: str) -> Callable[[Type[Any]], Type[Any]]:
        ...

    @overload
    def define(self, message_id: str, message_cls: Type[Any]) -> Callable[[Type[Any]], Type[Any]]:
        ...

    def define(self, message_id: str, message_cls: Type[Any] | None = None):
        skip = message_id in self._decode_registry

        def _register(message_cls: Type[Any]) -> Type[Any]:
            if skip or message_cls in self._encode_registry:
                return message_cls

            self._decode_registry[message_id] = message_cls
            self._encode_registry[message_cls] = message_id
            return message_cls

        if message_cls is not None:
            return _register(message_cls)
        else:
            return _register

    def encode(self, message: Any) -> bytes:
        if type(message) not in self._encode_registry:
            raise ValueError(f"Unencodable Class: {type(message)}")

        message_id = self._encode_registry[type(message)]
        message_type_id = message_id.encode("utf-8")
        payload = struct.pack("!I", len(message_type_id)) + message_type_id
        for field in fields(message):
            if field.metadata.get("skip"):
                continue

            field_id = field.metadata.get("id")
            if field_id is None:
                raise ValueError(f"Field '{field.name}' in message '{message_id}' must have an 'id'.")

            value = getattr(message, field.name)
            if isinstance(value, int):
                payload += struct.pack("!BI", field_id, value)  # Field ID + int value
            elif isinstance(value, str):
                encoded_value = value.encode("utf-8")
                payload += struct.pack("!BI", field_id, len(encoded_value)) + encoded_value  # Field ID + str value
            else:
                raise TypeError(f"Unsupported type: {type(value)}")
        return payload

    def decode(self, data: bytes) -> Any:
        """Decode binary data into a message instance."""
        offset = 0
        # Decode the message type ID
        type_id_length, = struct.unpack_from("!I", data, offset)
        offset += 4
        type_id = data[offset:offset + type_id_length].decode("utf-8")
        offset += type_id_length

        if type_id not in self._decode_registry:
            raise ValueError(f"Unknown message ID: {type_id}")

        message_cls = self._decode_registry[type_id]
        field_map = {
            field.metadata["id"]: field
            for field in fields(message_cls)
            if not field.metadata["skip"]
        }
        field_values = {}

        # Decode fields by their IDs
        while offset < len(data):
            field_id, = struct.unpack_from("!B", data, offset)
            offset += 1
            if field_id not in field_map:
                raise ValueError(f"Unknown field ID: {field_id} in message {type_id}")
            field = field_map[field_id]
            if field.type == int:
                value, = struct.unpack_from("!I", data, offset)
                offset += 4
            elif field.type == str:
                length, = struct.unpack_from("!I", data, offset)
                offset += 4
                value = data[offset:offset + length].decode("utf-8")
                offset += length
            else:
                raise TypeError(f"Unsupported type: {field.type}")
            field_values[field.name] = value

        return message_cls(**field_values)


