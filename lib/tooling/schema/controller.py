from logging import getLogger
import psycopg
from typing import List, Set, Self, Type

from lib.service.io import IoService
from lib.service.database import DatabaseService

from lib.tooling.schema import codegen
from .config import schema_ns
from .discovery import SchemaDiscovery
from .type import Command, Transform

class SchemaController:
    _logger = getLogger(f'{__name__}.SchemaController')
    _io: IoService
    _db: DatabaseService
    _discovery: SchemaDiscovery

    def __init__(self: Self,
                 io: IoService,
                 db: DatabaseService,
                 discovery: SchemaDiscovery) -> None:
        self._db = db
        self._io = io
        self._discovery = discovery

    async def command(self: Self, command: Command) -> None:
        self._logger.info(command)
        match command.transform:
            case Transform.Create() as t:
                await self.create(command, t)
            case Transform.Drop() as t:
                await self.drop(command, t)
            case Transform.Truncate() as t:
                await self.truncate(command, t)
            case Transform.ReIndex() as t:
                await self.reindex(command, t)
            case Transform.AddForeignKeys() as t:
                await self.add_foreign_keys(command, t)
            case Transform.RemoveForeignKeys() as t:
                await self.remove_foreign_keys(command, t)
            case other:
                raise TypeError(f'unknown command {other}')

    async def create(self: Self, command: Command, t: Transform.Create) -> None:
        load_syn = not t.run_raw_schema
        file_list = await self._discovery.files(
            command.ns,
            command.ns_range,
            load_syn=load_syn)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in file_list:
                # TODO look into this
                if file.is_known_to_be_transaction_unsafe:
                    await conn.commit()
                    await conn.set_autocommit(True)
                elif conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE:
                    await conn.set_autocommit(False)

                if t.run_raw_schema:
                    sql_text = await self._io.f_read(file.file_name)
                    self._logger.debug(f'running {file.file_name}')
                    await cursor.execute(sql_text)
                    continue
                elif file.contents is None:
                    raise TypeError()

                for operation in codegen.create(
                    file.contents,
                    t.omit_foreign_keys,
                ):
                    self._logger.debug(operation)
                    try:
                        await cursor.execute(operation)
                    except Exception as e:
                        self._logger.error(f"failed on {operation}")
                        raise e

    async def drop(self: Self, command: Command, t: Transform.Drop) -> None:
        file_list = await self._discovery.files(command.ns, command.ns_range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in reversed(file_list):
                if file.contents is None:
                    raise TypeError()

                for operation in codegen.drop(file.contents, t.cascade):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

    async def truncate(self: Self, command: Command, t: Transform.Truncate) -> None:
        file_list = await self._discovery.files(command.ns, command.ns_range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in reversed(file_list):
                if file.contents is None:
                    raise TypeError()

                for operation in codegen.truncate(file.contents, t.cascade):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

    async def reindex(self: Self, command: Command, t: Transform.ReIndex) -> None:
        file_list = await self._discovery.files(command.ns, command.ns_range, load_syn=True)
        async with self._db.async_connect() as conn:
            await conn.set_autocommit(True)
            for file in reversed(file_list):
                if file.contents is None:
                    raise TypeError()

                try:
                    operation = ''
                    for operation in codegen.reindex(file.contents, t.allowed):
                        self._logger.debug(operation)
                        await conn.execute(operation)
                except:
                    self._logger.error(f'Failed on:\n'
                                       f'  - File: {file}\n'
                                       f'  - Operation: {operation}')
                    raise

    async def add_foreign_keys(self: Self, command: Command, t: Transform.AddForeignKeys) -> None:
        file_list = await self._discovery.files(command.ns, command.ns_range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in file_list:
                if file.contents is None:
                    raise TypeError()

                try:
                    operation = ''
                    for operation in codegen.add_foreign_keys(file.contents):
                        self._logger.debug(operation)
                        await cursor.execute(operation)
                except:
                    self._logger.error(f'Failed on:\n'
                                       f'  - File: {file}\n'
                                       f'  - Operation: {operation}')
                    raise

    async def remove_foreign_keys(self: Self, command: Command, t: Transform.RemoveForeignKeys) -> None:
        file_list = await self._discovery.files(command.ns, command.ns_range, load_syn=True)

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            for file in file_list:
                if file.contents is None:
                    raise TypeError()

                fk_map = await codegen.make_fk_map(file.contents, cursor)
                for operation in codegen.remove_foreign_keys(file.contents, fk_map):
                    self._logger.debug(operation)
                    await cursor.execute(operation)

