from abc import ABC
from asyncio import open_connection, StreamReader, StreamWriter
from dataclasses import dataclass
import logging
from typing import Any, Self, Optional, Type, TypeVar
import psutil

from lib.service.clock import ClockService
from lib.utility.daemon import (
    ClientErrors,
    DaemonClientRpc,
    MessageNamespace,
    Request,
    Sys,
)

from .types import (
    DaemonConnection,
    DaemonConnectionCfg,
    DaemonCandidatePid,
    DaemonService,
    DaemonServiceErrors
)

_Response = TypeVar("_Response", covariant=True)

class _CouldNotFindDaemon(Exception):
    ...

class BaseRcpClient(DaemonClientRpc):
    _logger = logging.getLogger(__name__)
    __conn: Optional[DaemonConnection] = None

    def __init__(self: Self,
                 conn_config: DaemonConnectionCfg,
                 clock: ClockService,
                 daemon_service: DaemonService,
                 namespace: MessageNamespace) -> None:
        self._clock = clock
        self._d_service = daemon_service
        self.conn_config = conn_config
        self.namespace = namespace

    @property
    def connected(self: Self) -> bool:
        return self.__conn is not None

    async def call(self: Self, request: Request[_Response], res_t: Type[_Response]) -> _Response:
        match self.__conn:
            case None:
                raise ClientErrors.NotConnectedOnCall(request)
            case DaemonConnection() as conn:
                pass
        return await self._call(conn, request, res_t)

    async def cast(self: Self, message: Any):
        match self.__conn:
            case None:
                raise ClientErrors.NotConnectedOnCast(message)
            case DaemonConnection() as conn:
                pass
        conn.writer.write(self.namespace.encode(message))
        await conn.writer.drain()

    async def connect(self: Self) -> None:
        self._logger.debug(f"connecting to {self.conn_config}")
        try:
            self.__conn = await self._find_daemon()
            self._logger.debug(f"found daemon @ {self.__conn}")
        except (_CouldNotFindDaemon, DaemonServiceErrors.ProcNotFound):
            self.__conn = await self._start_daemon()
            self._logger.debug(f"started daemon @ {self.__conn}")
        self._logger.debug(f"connected to {self.__conn}")

    async def disconnect(self: Self) -> None:
        if self.__conn:
            await self.cast(Sys.Disconnect())
            self.__conn.writer.close()
            await self.__conn.writer.wait_closed()

    async def __aenter__(self: Self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self: Self, *args, **kwargs):
        await self.disconnect()

    async def _find_daemon(self: Self) -> DaemonConnection:
        if self.__conn is not None:
            raise ClientErrors.AlreadyConnected()

        async for candidate in self._d_service.find_daemon_candidates(self.conn_config):
            async for conn in self._d_service.find_connection_candidate(candidate):
                try:
                    self._logger.debug(f"initiating handshake with {candidate}")
                    resp = await self._call(conn, Sys.HandshakeReq(), Sys.HandshakeAck)
                    self._logger.debug(f"successfully shook hands with {candidate}")
                except ClientErrors.ResponseTimeout as e:
                    continue
                return conn
        raise _CouldNotFindDaemon()

    async def _start_daemon(self: Self) -> DaemonConnection:
        candidate = await self._d_service.start_daemon(self.conn_config)
        try:
            return await self._find_daemon()
        except _CouldNotFindDaemon:
            self._logger.error('could not connect to newly created process')
            candidate.proc.kill()
        raise ClientErrors.CouldNotConnect()

    async def _find_connection(self: Self, candidate: DaemonCandidatePid) -> Optional[DaemonConnection]:
        async for conn in self._d_service.find_connection_candidate(candidate):
            try:
                resp = await self._call(conn, Sys.HandshakeReq(), Sys.HandshakeAck)
            except ClientErrors.ResponseTimeout as e:
                continue
            return conn
        return None

    async def _call(
        self: Self,
        conn: DaemonConnection,
        request: Request[_Response],
        res_t: Type[_Response],
    ) -> _Response:
        self._logger.debug(f"Sending {request}")
        conn.writer.write(self.namespace.encode(request))
        await conn.writer.drain()

        match self.namespace.decode(await conn.reader.read(1024)):
            case response if isinstance(response, res_t):
                return response
            case other:
                self._logger.warn(f"unexpected response\nexpected {res_t}\ngot {type(other)} {other}")
                raise ClientErrors.UnexpectedResponse(other)
