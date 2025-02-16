import os
import asyncio
import logging
import psutil
import signal
import socket
import subprocess
import time

from typing import Optional

from lib.daemon.echo import *
from lib.service.daemon import *
from lib.service.clock import *
from lib.utility.daemon import *

_logger = logging.getLogger(__name__)

async def communicate_with_daemon() -> None:
    host = 'localhost'
    clock = ClockService()
    d_service = DaemonServiceImpl(clock, 'localhost')
    echo_d = EchoRpcClient.create(clock, d_service)

    try:
        await echo_d.connect()
        req_m = EchoRequest(message="Hello, daemon!")
        res_m = await echo_d.echo(req_m)
        _logger.info(f"\nSent {req_m}\nRecv {res_m}")
        await echo_d.disconnect()
    except ConnectionRefusedError:
        _logger.info("Daemon is not running.")
    except Exception as e:
        _logger.exception(e)

if __name__ == '__main__':
    from lib.utility.logging import config_vendor_logging, config_logging

    config_vendor_logging(set())
    config_logging(worker=None, debug=True, output_name='daemon-client:echo')

    asyncio.run(communicate_with_daemon())
