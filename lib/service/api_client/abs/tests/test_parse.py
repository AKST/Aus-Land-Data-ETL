import json
import pytest
from pprint import pformat
from typing import Any

from lib.service.io import IoServiceImpl

from ..parse import (
    parse_codelist_meta,
    parse_conceptscheme_meta,
    parse_dataflow_meta,
    parse_datastructure_meta,
    parse_data_all_dimensions,
)

@pytest.mark.asyncio
@pytest.mark.parametrize("dataflow_id", [
    'C21_T17_SA2',
])
async def test_parse_data_all_dimensions_snapshot(snapshot, dataflow_id: str):
    io = IoServiceImpl.create(1)
    name = f'absapi_data_{dataflow_id}-*-_T-AUS-*-*.json'
    resp = json.loads(await io.f_read(f'./_fixtures/{name}'))
    data = parse_data_all_dimensions(resp)
    snapshot.assert_match(pformat(list(data), width=150), 'snap')

@pytest.mark.asyncio
@pytest.mark.parametrize("dataflow_id,structure", [
    ('C21_T17_SA2', 'dataflow'),
    ('C21_T17_SA2', 'conceptscheme'),
    ('C21_T17_SA2', 'codelist'),
    ('C21_T17_SA2', 'datastructure'),
])
async def test_parse_metadata(snapshot, dataflow_id: str, structure: str):
    io = IoServiceImpl.create(1)

    data_nesting = ''
    parse_f: Any = None
    match structure:
        case 'dataflow':
            parse_f, data_nesting = parse_dataflow_meta, 'dataflows'
        case 'conceptscheme':
            parse_f, data_nesting = parse_conceptscheme_meta, 'conceptSchemes'
        case 'codelist':
            parse_f, data_nesting = parse_codelist_meta, 'codelists'
        case 'datastructure':
            parse_f, data_nesting = parse_datastructure_meta, 'dataStructures'

    name = f'absapi_meta_{dataflow_id}_dataflow_with_all_refs.json'
    resp = json.loads(await io.f_read(f'./_fixtures/{name}'))
    data = [parse_f(d) for d in resp['data'][data_nesting]]
    snapshot.assert_match(pformat(list(data), width=150), 'snap')

