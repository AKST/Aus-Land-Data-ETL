from dataclasses import dataclass
from logging import getLogger
import re
from typing import cast, Self, Optional

from lib.service.io import IoService

from .config import schema_ns
from .type import SchemaNamespace

def create_file_regex(root_dir: str) -> re.Pattern:
    path_root = re.escape(root_dir)
    path_ns = r'(?P<ns>[_a-zA-Z][_a-zA-Z0-9]*)'
    path_file = r'(?P<step>\d{3})_APPLY(_(?P<name>[_a-zA-Z][_a-zA-Z0-9]*))?.sql'
    return re.compile(rf'^{path_root}/{path_ns}/schema/{path_file}$')

@dataclass
class FileDiscoveryMatch:
    ns: SchemaNamespace
    step: int
    name: str


class FileDiscovery:
    logger = getLogger(__name__)

    def __init__(self: Self, io: IoService, file_regex: re.Pattern, root_dir: str):
        self._io = io
        self.file_regex = file_regex
        self.root_dir = root_dir

    async def ns_matches(self: Self, ns: SchemaNamespace) -> list[tuple[str, FileDiscoveryMatch]]:
        return [(f, self.match_file(f)) for f in await self.ns_sql_files(ns)]

    async def ns_sql_files(self: Self, ns: SchemaNamespace) -> list[str]:
        glob_s = '*_APPLY*.sql'
        root_d = f'{self.root_dir}/{ns}/schema'
        return [f async for f in self._io.grep_dir(root_d, glob_s)]

    def match_file(self: Self, file: str) -> FileDiscoveryMatch:
        match self.file_regex.match(file):
            case None:
                raise ValueError(f'invalid file {file}')
            case match:
                ns_str = match.group('ns')
                step = int(match.group('step'))
                name = match.group('name')
                if ns_str not in schema_ns:
                    raise TypeError(f'unknown namespace {ns_str}')
                ns: SchemaNamespace = cast(SchemaNamespace, ns_str)
                return FileDiscoveryMatch(ns, step, name)

