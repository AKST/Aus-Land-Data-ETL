import pytest
from unittest.mock import AsyncMock

from lib.service.database.mock import MockDatabaseService, clean_sql
from lib.service.io import IoService
from lib.service.uuid.mocks import MockUuidService
from .mocks import get_mock_row
from ..config import NswVgLvTaskDesc, RawLandValueRow
from ..ingest import NswVgLvIngestion, get_load_values

@pytest.mark.asyncio
async def test_load_task() -> None:
    db = MockDatabaseService()
    io = AsyncMock(spec=IoService)
    uuid = MockUuidService(values=[str(i) for i in range(0, 4)])
    ingestion = NswVgLvIngestion(100, uuid, io, db)
    rows = [get_mock_row(property_id=i) for i in range(0, 4)]
    load_task = NswVgLvTaskDesc.Load(file='mock', offset=0, rows=rows)
    column_str, values_str, values = get_load_values(load_task)

    await ingestion.load(load_task)

    assert db.state.executemany_args[0] == (
        clean_sql(f'''
            INSERT INTO nsw_vg_raw.land_value_row ( {column_str} )
            VALUES ( {values_str} )
        '''),
        values,
    )

