import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger, Logger
from typing import Self, AsyncIterator, Sequence, List, Literal

from lib.pipeline.nsw_vg.discovery import NswVgTarget, LandValueDiscovery
from lib.service.io import IoService
from lib.service.static_environment import StaticEnvironmentInitialiser

from ..discovery import NswVgPublicationDiscovery, NSWVG_LV_DISCOVERY_CFG
from ._util import select_targets
from .config import NswVgLvTaskDesc, DiscoveryMode, ByoLandValue
from .telemetry import NswVgLvTelemetry

_BYO_LV_DIR = '_cfg_byo_lv'

@dataclass(frozen=True)
class Config:
    kind: DiscoveryMode.T
    unzip_dir: str
    byo_dir: str

class CsvAbstractDiscovery:
    @abc.abstractmethod
    def files(self: Self) -> AsyncIterator[NswVgLvTaskDesc.Parse]:
        pass

class ByoCsvDiscovery(CsvAbstractDiscovery):
    _logger = getLogger(f'{__name__}.ByoCsvDiscovery')

    def __init__(self: Self,
                 config: Config,
                 io: IoService,
                 telemetry: NswVgLvTelemetry,
                 fixtures: List[ByoLandValue]) -> None:
        self.config = config
        self.fixtures = fixtures
        self._io = io
        self._telemetry = telemetry

    async def files(self: Self) -> AsyncIterator[NswVgLvTaskDesc.Parse]:
        for target in select_targets(self.config.kind, self.fixtures):
            root_out = f'{self.config.unzip_dir}/{dst_name_from_src_name(target.src_dst)}'
            await self._unzip(target, root_out)

            for f in sorted(await self._io.ls_dir(root_out)):
                if not f.endswith("csv"):
                    continue

                f_path = f'{root_out}/{f}'
                f_size = await self._io.f_size(f_path)
                self._telemetry.record_file_queue(f_path, f_size)
                yield NswVgLvTaskDesc.Parse(f_path, f_size, target)

    async def _unzip(self: Self, target: ByoLandValue, out_loc: str):
        src_loc = f'{self.config.byo_dir}/{target.src_dst}.zip'

        if await self._io.is_dir(src_loc):
            return

        self._logger.info(f'Creating zip output dir "{target.src_dst}"')
        try:
            await self._io.extract_zip(src_loc, out_loc)
        except Exception as e:
            self._logger.error(f'failed to unzip, {src_loc} to {out_loc}')
            raise e

class RemoteCsvDiscovery(CsvAbstractDiscovery):
    def __init__(self: Self,
                 config: Config,
                 io: IoService,
                 telemetry: NswVgLvTelemetry,
                 web_discovery: NswVgPublicationDiscovery,
                 static_environment: StaticEnvironmentInitialiser) -> None:
        self.config = config
        self._io = io
        self._telemetry = telemetry
        self._web_discovery = web_discovery
        self._env = static_environment

    async def files(self: Self) -> AsyncIterator[NswVgLvTaskDesc.Parse]:
        targets = select_targets(
            self.config.kind,
            await self._web_discovery.load_links(NSWVG_LV_DISCOVERY_CFG),
        )

        async for target in self._env.with_targets(targets):
            root = f'{self.config.unzip_dir}/{target.zip_dst}'
            for f in sorted(await self._io.ls_dir(root)):
                if not f.endswith("csv"):
                    continue

                f_path = f'{self.config.unzip_dir}/{target.zip_dst}/{f}'
                f_size = await self._io.f_size(f_path)
                self._telemetry.record_file_queue(f_path, f_size)
                yield NswVgLvTaskDesc.Parse(f_path, f_size, target)

def dst_name_from_src_name(src_dst: str) -> str:
    prefix, date_str = src_dst.split("_")
    year, month, day = date_str[:4], date_str[4:6], date_str[6:]
    month_abbr = datetime.strptime(month, "%m").strftime("%b")

    # Construct the original file name
    original_name = f"nswvg_lv_{day}_{month_abbr}_{year}"
    return original_name

