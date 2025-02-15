#!/usr/bin/env python
import asyncio
import logging
import os
import signal
import time
from typing import Optional, Self

from .message import (
    msg_registry,
    Message,
    EchoRequest,
    EchoResponse,
    HandshakeRequest,
    HandshakeResponse,
    CloseRequest,
)

logger = logging.getLogger(__name__)

EVAR_PROC_PORT = "DB_AKST_IO_PROC_PORT"

class DaemonConnectionHandler:
    _server: Optional[asyncio.Server] = None
    _active_connections = 0

    async def on_connection(self: Self, reader, writer) -> None:
        logger.info('connection')
        addr = None
        try:
            resp: Message

            self._active_connections += 1
            addr = writer.get_extra_info('peername')
            logger.info(f"OPENING {addr}")
            while data := await asyncio.wait_for(reader.read(1024), timeout=1.0):
                message: Message = msg_registry.decode(data)
                logger.info(f"Received: {message}")

                match message:
                    case HandshakeRequest():
                        resp = HandshakeResponse()
                        writer.write(msg_registry.encode(resp))
                        await writer.drain()
                    case EchoRequest(message=m):
                        resp = EchoResponse(message=m)
                        writer.write(msg_registry.encode(resp))
                        await writer.drain()
                    case CloseRequest:
                        break
            logger.info(f"CLOSING {addr}")
        except Exception as e:
            logger.info(f"failed for {addr}")
            logger.exception(e)
        finally:
            self._active_connections -= 1
            logger.info(f"Connection with {addr} closed.")
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
        logger.info(f"daemon @ {os.getpid()} listening on {port}")
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
                logger.info("No connectings, shutting down daemon")
                await self.shutdown()


async def start_daemon(host: str, timeout=5.0):
    logger.info('creating daemon')
    server = await DaemonConnectionHandler.create(host, 0)
    logger.info('assigning signals')
    signal.signal(signal.SIGTERM, lambda *args: server.on_signal(*args))
    signal.signal(signal.SIGQUIT, lambda *args: server.on_signal(*args))
    logger.info('serving')
    await server.serve()


if __name__ == '__main__':
    logging.basicConfig(
        filename=f'_out_log/http_daemon.log',
        level=logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logger.info(f"daemon @ {os.getpid()}, starting http")

    try:
        asyncio.run(start_daemon(host='localhost'))
    except Exception as e:
        logger.error("failure within echo daemon")
        logger.exception(e)
    finally:
        logger.info("shutting echo daemon down")
