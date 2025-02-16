#!/usr/bin/env python
import asyncio
import logging
import os
import signal
import time
from typing import Optional, Self

from .message import (
    echo_ns,
    Message,
    EchoRequest,
    EchoResponse,
    Sys,
)

EVAR_PROC_PORT = "DB_AKST_IO_PROC_PORT"

class DaemonConnectionHandler:
    _logger = logging.getLogger(__name__)
    _server: Optional[asyncio.Server] = None
    _active_connections = 0

    async def on_connection(self: Self, reader, writer) -> None:
        _logger.info('connection')
        addr = None
        try:
            resp: Message

            self._active_connections += 1
            addr = writer.get_extra_info('peername')
            _logger.info(f"OPENING {addr}")
            while data := await asyncio.wait_for(reader.read(1024), timeout=1.0):
                message: Message = echo_ns.decode(data)
                _logger.info(f"Received: {message}")

                match message:
                    case Sys.HandshakeReq():
                        resp = Sys.HandshakeAck()
                        writer.write(echo_ns.encode(resp))
                        await writer.drain()
                    case EchoRequest(message=m):
                        resp = EchoResponse(message=m)
                        writer.write(echo_ns.encode(resp))
                        await writer.drain()
                    case Sys.Disconnect():
                        break
            _logger.info(f"CLOSING {addr}")
        except Exception as e:
            _logger.info(f"failed for {addr}")
            _logger.exception(e)
        finally:
            self._active_connections -= 1
            _logger.info(f"Connection with {addr} closed.")
            writer.close()
            await writer.wait_closed()

    def on_signal(self: Self, sig, frame):
        match sig:
            case signal.SIGTERM:
                asyncio.create_task(self.shutdown())
            case signal.SIGQUIT:
                asyncio.create_task(self.shutdown())

    @classmethod
    async def create(Cls, host, port=0, *args, **kwargs):
        instance = Cls(*args, **kwargs)
        server = await asyncio.start_server(instance.on_connection, host, port)
        instance._server = server
        return instance

    async def serve(self: Self) -> None:
        if not self._server:
            return

        port = str(self._server.sockets[0].getsockname()[1])
        _logger.info(f"daemon @ {os.getpid()} listening on {port}")
        monitor = asyncio.create_task(self._inactivity_monitor())

        async with self._server:
            await self._server.serve_forever()
        await monitor

    async def shutdown(self: Self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _inactivity_monitor(self):
        while self._server:
            await asyncio.sleep(2.5)

            if not self._active_connections:
                _logger.info("No connectings, shutting down daemon")
                await self.shutdown()


async def start_daemon(host: str, timeout=5.0):
    _logger.info('creating daemon')
    server = await DaemonConnectionHandler.create(host, 0)
    _logger.info('assigning signals')
    signal.signal(signal.SIGTERM, lambda *args: server.on_signal(*args))
    signal.signal(signal.SIGQUIT, lambda *args: server.on_signal(*args))
    _logger.info('serving')
    await server.serve()


if __name__ == '__main__':
    from lib.utility.logging import config_vendor_logging, config_logging

    config_vendor_logging(set())
    config_logging(worker=None, debug=False, output_name='daemon-server')

    _logger = logging.getLogger(__name__)
    _logger.info(f"daemon @ {os.getpid()}, starting http")

    try:
        asyncio.run(start_daemon(host='localhost'))
    except Exception as e:
        _logger.error("failure within echo daemon")
        _logger.exception(e)
    finally:
        _logger.info("shutting echo daemon down")
