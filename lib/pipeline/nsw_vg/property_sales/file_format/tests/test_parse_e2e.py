import pytest
from pprint import pformat
from datetime import datetime

from lib.pipeline.nsw_vg.property_sales.data import PropertySaleDatFileMetaData
from lib.service.io import IoServiceImpl
from lib.service.uuid.mocks import MockUuidService

from ..text_source import *
from ..parse import PropertySalesRowParserFactory

@pytest.mark.asyncio
@pytest.mark.parametrize("file_name,published_year,download_date", [
    ('ps_2021_20210823.dat', 2021, datetime(2021, 8, 23)),
    ('ps_2011_20111003.dat', 2011, datetime(2011, 10, 3)),
    ('ps_2004_20040916.dat', 2004, datetime(2004, 9, 16)),
    ('ps_2001_20010822.dat', 2001, datetime(2001, 8, 22)),
    ('ps_2001_20010720.dat', 2001, datetime(2001, 7, 20)),
    ('ps_1990_fake.dat', 1990, None),
])
async def test_end_to_end(file_name: str,
                          published_year: int,
                          download_date: datetime | None):
    io = IoServiceImpl.create(1)
    uuid = MockUuidService(values=[str(i) for i in range(0, 10)])
    s_factory = PropertySalesRowParserFactory(io,
                                              uuid,
                                              StringTextSource)
    uuid = MockUuidService(values=[str(i) for i in range(0, 10)])
    b_factory = PropertySalesRowParserFactory(io,
                                              uuid,
                                              BufferedFileReaderTextSource,
                                              chunk_size=8 * 2 ** 5)
    file_path = f'./_fixtures/{file_name}'
    file_size = await io.f_size(file_path)
    file_data = PropertySaleDatFileMetaData(file_path=file_path, published_year=published_year, download_date=download_date, size=file_size)
    s_parser = await s_factory.create_parser(file_data)
    b_parser = await b_factory.create_parser(file_data)
    s_items = [it async for it in s_parser.get_data_from_file()]
    b_items = [it async for it in b_parser.get_data_from_file()]

    # these differ which is fine
    for a, b in zip(s_items, b_items):
        a.position = b.position

    assert s_items == b_items

@pytest.mark.asyncio
@pytest.mark.parametrize("file_name,published_year,download_date", [
    ('ps_2021_20210823.dat', 2021, datetime(2021, 8, 23)),
    ('ps_2011_20111003.dat', 2011, datetime(2011, 10, 3)),
    ('ps_2004_20040916.dat', 2004, datetime(2004, 9, 16)),
    ('ps_2001_20010822.dat', 2001, datetime(2001, 8, 22)),
    ('ps_2001_20010720.dat', 2001, datetime(2001, 7, 20)),
    ('ps_1990_fake.dat', 1990, None),
])
async def test_snapshot(snapshot,
                        file_name: str,
                        published_year: int,
                        download_date: datetime | None):
    io = IoServiceImpl.create(1)
    uuid = MockUuidService(values=[str(i) for i in range(0, 10)])
    s_factory = PropertySalesRowParserFactory(io,
                                              uuid,
                                              StringTextSource)
    uuid = MockUuidService(values=[str(i) for i in range(0, 10)])
    b_factory = PropertySalesRowParserFactory(io,
                                              uuid,
                                              BufferedFileReaderTextSource,
                                              chunk_size=8 * 2 ** 5)
    file_path = f'./_fixtures/{file_name}'
    file_size = await io.f_size(file_path)
    file_data = PropertySaleDatFileMetaData(file_path=file_path,
                                            published_year=published_year,
                                            download_date=download_date,
                                            size=file_size)
    s_parser = await s_factory.create_parser(file_data)
    s_items = [it async for it in s_parser.get_data_from_file()]
    snapshot.assert_match(pformat(s_items, width=150), file_name)

