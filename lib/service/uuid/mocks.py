from dataclasses import dataclass, field
from typing import List

from .service import UuidService

@dataclass
class MockUuidService(UuidService):
    values: List[str]
    _call_count: int = field(default=0)

    def get_uuid4_hex(self):
        index = self._call_count
        self._call_count = index + 1
        return self.values[index % len(self.values)]

