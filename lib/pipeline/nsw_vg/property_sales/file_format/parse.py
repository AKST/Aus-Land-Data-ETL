import asyncio
from datetime import datetime
from logging import getLogger
from typing import Any, Dict, AsyncIterator, List, Optional, Tuple, Self, Type
import os
import re

from lib.pipeline.nsw_vg.property_sales import PropertySaleDatFileMetaData
from lib.service.io import IoService
from lib.service.uuid import UuidService

from .factories import AbstractFormatFactory
from .syntax import get_columns_and_syntax, Syntax
from .text_source import AbstractTextSource

class PropertySalesRowParserFactory:
    Source: Type[AbstractTextSource]
    chunk_size: int
    _uuid: UuidService
    _io: IoService

    def __init__(self: Self,
                 io: IoService,
                 uuid: UuidService,
                 Source: Type[AbstractTextSource],
                 chunk_size: int = 8 * 2 ** 10):
        self._io = io
        self._uuid = uuid
        self.Source = Source
        self.chunk_size = chunk_size

    async def create_parser(self: Self, file_data: PropertySaleDatFileMetaData) -> 'PropertySalesParser':
        date = file_data.download_date
        year = file_data.published_year
        path = file_data.file_path

        Factory, syntax = get_columns_and_syntax(date, year)
        factory = Factory.create(self._uuid, year=year, file_path=path)
        f_path = file_data.file_path
        source = await self.Source.create(
            file_data.file_path,
            self._io,
            chunk_size=self.chunk_size,
        )
        return PropertySalesParser(file_data, factory, source, syntax)

class PropertySalesParser:
    file_data: PropertySaleDatFileMetaData
    constructors: AbstractFormatFactory

    _source: AbstractTextSource
    _semi_colons: Syntax

    __last_row: str = ''
    _index: int = 0
    _logger = getLogger(f'{__name__}.PropertySalesParser')

    def __init__(self: Self,
                 file_data: PropertySaleDatFileMetaData,
                 constructors: AbstractFormatFactory,
                 source: AbstractTextSource,
                 semicolons: Syntax) -> None:
        self.file_data = file_data
        self.constructors = constructors
        self._source = source
        self._semi_colons = semicolons

    async def remaining(self: Self) -> str:
        return await self._source.read(self._index, self._index + 400)

    async def get_data_from_file(self: Self):
        kind, row = None, None
        a, b, c, d = None, None, None, None

        try:
            async for pos, variant, kind, row in self.get_rows():
                if kind == 'A':
                    a = self.constructors.create_a(pos, row, variant=variant)
                    yield a
                elif kind == 'B':
                    b = self.constructors.create_b(pos, row, a_record=a, variant=variant)
                    yield b
                elif kind == 'C':
                    c = self.constructors.create_c(pos, row, b_record=b, variant=variant)
                    yield c
                elif kind == 'D':
                    d = self.constructors.create_d(pos, row, c_record=c, variant=variant)
                    yield d
                elif kind == 'Z':
                    yield self.constructors.create_z(pos, row, a_record=a, variant=variant)
                else:
                    raise ValueError(f"Unexpected record type: {kind}")
        except Exception as e:
            self._logger.error(f'Source: {self.file_data.file_path}')
            self._logger.error(f'Target Year: {self.file_data.published_year}')
            self._logger.error(f'Download Date: {self.file_data.download_date}')
            self._logger.error(f'Factory: {type(self.constructors)}')
            self._logger.error(f'Syntax: {self._semi_colons}')
            self._logger.error(f'Index: {self._index}')
            self._logger.error(f'last row: {row}')
            self._logger.error(f'last kind: {kind}')
            self._logger.error(f'last a row: {a}')
            self._logger.error(f'last b row: {b}')
            self._logger.error(f'last c row: {c}')
            self._logger.error(f'last d row: {d}')
            self._logger.error(f'Remaining: {await self.remaining()}')
            self._logger.exception(e)
            raise e

    async def get_rows(self: Self) -> AsyncIterator[Tuple[int, str | None, str, List[str]]]:
        while self._index < self._source.size():
            position = self._index
            row_s, mode = await self._get_mode()
            variant, row_e, row = await self._get_row_body(mode, row_s)


            yield position, variant, mode, row

            if mode == 'Z':
                break

            self._index = row_e + 2

    async def _get_mode(self) -> Tuple[int, str]:
        mode_end = await self._find_nth_semicolon(1)
        mode = await self._source.read(self._index, mode_end)
        mode = mode.strip()

        if mode not in self._semi_colons:
            raise ValueError(f'Unexpected mode: "{mode}"@{self._index}, ' \
                             f'last row: "{self.__last_row}", ' \
                             f'source: {self._source.source_name}')

        return mode_end - 1, mode

    async def _get_row_body(self, mode: str, row_start: int) -> Tuple[str | None, int, List[str]]:
        sc_count_raw = self._semi_colons[mode]
        if isinstance(sc_count_raw, list):
            sc_count_ls = list(sorted(sc_count_raw, reverse=True))
        else:
            sc_count_ls = [(sc_count_raw, None)]

        for i, (sc_count, variant) in enumerate(sc_count_ls):
            row_end = await self._find_nth_semicolon(sc_count)

            if row_end == -1:
                raise ValueError(
                    f'Unexpected end of data: "{mode}"@{self._index}, ' \
                    f'last row: "{self.__last_row}", ' \
                    f'source: {self._source.source_name}'
                )

            row_s = await self._source.read(row_start+2, row_end)
            row = row_s.split(';')

            if i+1 == len(sc_count_ls):
                break

            sc_count_next, _ = sc_count_ls[i+1]
            if '\n' not in row[sc_count_next - 1]:
                break
        else:
            raise ValueError(f'No iteration occurred when parsing {mode}')

        self.__last_row = row_s
        return variant, row_end, row

    async def _find_nth_semicolon(self: Self, n: int) -> int:
        position = self._index
        semicolon_count = 0
        while semicolon_count < n:
            found_index = await self._source.find_index(';', position, self._index)
            if found_index == -1:
                return -1
            position = found_index + 1
            semicolon_count += 1
        return found_index


