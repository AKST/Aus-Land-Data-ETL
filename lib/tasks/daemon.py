import os
import asyncio
import psutil
import signal
import socket
import subprocess
import time

from typing import List, Optional, Tuple

from lib.daemon.echo import *

EVAR_PROC_NAME = "DB_AKST_IO_PROC_NAME"
EVAR_PROC_PORT = "DB_AKST_IO_PROC_PORT"

class DaemonNotFound(Exception):
    def __init__(self):
        super()

class DaemonPortNotFound(Exception):
    def __init__(self):
        super()

def is_daemon(proc: psutil.Process, proc_tag: str) -> bool:
    if proc.status() == psutil.STATUS_ZOMBIE:
        return False
    try:
        env = proc.environ()
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return False
    return EVAR_PROC_NAME in env and env[EVAR_PROC_NAME] == proc_tag

def is_port_open(host: str, port: int, timeout=0.1):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
            return True
        except (socket.timeout, ConnectionRefusedError):
            return False

async def find_daemon_port(pid: int, timeout: float) -> int:
    process = psutil.Process(pid)
    start_time = time.time()
    while time.time() - start_time < timeout:
        for conn in process.net_connections(kind="inet"):
            if conn.status != "LISTEN":
                continue

            try:
                port_candidate = conn.laddr.port
                reader, writer = await asyncio.open_connection("localhost", port_candidate)
                writer.write(msg_registry.encode(HandshakeRequest()))
                await writer.drain()

                match msg_registry.decode(await reader.read(1024)):
                    case HandshakeResponse():
                        writer.write(msg_registry.encode(CloseRequest()))
                        await writer.drain()

                        writer.close()
                        await writer.wait_closed()

                        return port_candidate
            except (ConnectionRefusedError, OSError):
                continue
    raise DaemonPortNotFound()

async def find_process(proc_tag: str) -> Tuple[int, int]:
    daemon_proc = next((
        process
        for process in psutil.process_iter(['pid', 'environ', 'status'])
        if is_daemon(process, proc_tag)
    ), None)

    if not daemon_proc:
        raise DaemonNotFound()

    try:
        daemon_port = await find_daemon_port(daemon_proc.pid, timeout=5.0)
    except DaemonPortNotFound as e:
        daemon_proc.kill()
        raise e

    return daemon_proc.pid, daemon_port

async def start_process(proc_tag: str, module_name: str, timeout: int) -> Tuple[int, int]:
    metadata = {}
    metadata.update(os.environ.copy())
    metadata.update({EVAR_PROC_NAME: "HTTP_DAEMON"})
    sub_process = subprocess.Popen(
        ["python", "-m", "lib.daemon.echo.entry"],
        env=metadata,
        preexec_fn=os.setsid,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    process = psutil.Process(sub_process.pid)

    try:
        daemon_port = await find_daemon_port(sub_process.pid, timeout)
        return process.pid, daemon_port
    except DaemonPortNotFound as e:
        os.kill(sub_process.pid, signal.SIGKILL)
        raise e

async def communicate_with_daemon() -> None:
    host = 'localhost'
    try:
        pid, port = await find_process("HTTP_DAEMON")
        print(f"found daemon @ {pid} with port {port}")
    except DaemonNotFound:
        pid, port = await start_process("HTTP_DAEMON", "lib.daemon.http.entry", timeout=5)
        print(f"started daemon @ {pid} with port {port}")

    try:
        reader, writer = await asyncio.open_connection(host, port)
        print(f"Connected to daemon at {host}:{port}")

        # Send a request
        echo_req = EchoRequest(message="Hello, daemon!")
        writer.write(msg_registry.encode(echo_req))
        await writer.drain()

        # Read the response
        echo_resp = msg_registry.decode(await reader.read(1024))
        print(f"Response from daemon: {echo_resp}")

        writer.write(msg_registry.encode(CloseRequest()))
        await writer.drain()

        writer.close()
        await writer.wait_closed()
    except ConnectionRefusedError:
        print("Daemon is not running.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    asyncio.run(communicate_with_daemon())
