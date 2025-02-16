from typing import Self
from lib.service.clock import ClockService
from lib.service.daemon import BaseRcpClient, DaemonService
from .message import EchoRpc, EchoRequest, EchoResponse

class EchoRpcClient(BaseRcpClient, EchoRpc):
    async def echo(self: Self, msg: EchoRequest) -> EchoResponse:
        return await self.call(msg, EchoResponse)

    @staticmethod
    def create(clock: ClockService, d_service: DaemonService) -> 'EchoRpcClient':
        from .defaults import ECHO_CONN_CFG
        from .message import echo_ns
        return EchoRpcClient(ECHO_CONN_CFG, clock, d_service, echo_ns)

