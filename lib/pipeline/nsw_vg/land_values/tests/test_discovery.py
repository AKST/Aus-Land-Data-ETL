from datetime import datetime
import pytest
from unittest.mock import AsyncMock, call

from lib.service.io import IoService

from ..config import (
    ByoLandValue,
    DiscoveryMode,
    NswVgLvTaskDesc,
)
from ..defaults import byo_land_values
from ..discovery import (
    ByoCsvDiscovery,
    Config,
    dst_name_from_src_name,
)
from ..telemetry import NswVgLvTelemetry

def test_filesnames_correct():
    fnames = [
        dst_name_from_src_name(t.src_dst)
        for t in byo_land_values
    ]
    generated = [
        f'nswvg_lv_{t.datetime.strftime("%d_%b_%Y")}'
        for t in byo_land_values
    ]
    assert fnames == generated

@pytest.mark.asyncio
async def test_unzip_behaviour():
    dt = datetime(2012, 1, 1)
    target = ByoLandValue('LV_20120101', dt)

    io = AsyncMock(spec=IoService)
    tel = AsyncMock(spec=NswVgLvTelemetry)
    cfg = Config(DiscoveryMode.Latest(), 'unzip_root', 'byo_root')

    io.is_dir.return_value = False
    io.ls_dir.return_value = ['a.csv', 'b.csv']
    io.f_size.return_value = 100
    dis = ByoCsvDiscovery(cfg, io, tel, [target])
    parse_tasks = [t async for t in dis.files()]

    io.extract_zip.assert_called_once_with(
        'byo_root/LV_20120101.zip',
        'unzip_root/nswvg_lv_01_Jan_2012',
    )

    io.f_size.assert_has_calls([
        call('unzip_root/nswvg_lv_01_Jan_2012/a.csv'),
        call('unzip_root/nswvg_lv_01_Jan_2012/b.csv'),
    ])

    assert parse_tasks == [
        NswVgLvTaskDesc.Parse(f, 100, target)
        for f in [
            f'unzip_root/nswvg_lv_{dt.strftime("%d_%b_%Y")}/a.csv',
            f'unzip_root/nswvg_lv_{dt.strftime("%d_%b_%Y")}/b.csv',
        ]
    ]







