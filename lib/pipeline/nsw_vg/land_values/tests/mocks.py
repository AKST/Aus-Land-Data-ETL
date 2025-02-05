from datetime import datetime
from typing import Unpack, Any

from ..config import RawLandValueRow

def get_mock_row(**kwargs) -> RawLandValueRow:
    defaults = {
        'district_code': 1,
        'district_name': 'mock_district',
        'property_id': 1,
        'property_type': 'NORMAL',
        'property_name': None,
        'unit_number': None,
        'house_number': '1',
        'street_name': 'mockStreet',
        'suburb_name': 'mockSuburb',
        'postcode': '1234',
        'property_description': '1234/1234',
        'zone_code': 'R2',
        'zone_standard': 'ep&a_2006',
        'area': 100,
        'area_type': 'M',
        'land_value_1': 10000,
        'base_date_1': datetime(2012, 12, 12),
        'authority_1': '?',
        'basis_1': '?',
        'land_value_2': None,
        'base_date_2': None,
        'authority_2': None,
        'basis_2': None,
        'land_value_3': None,
        'base_date_3': None,
        'authority_3': None,
        'basis_3': None,
        'land_value_4': None,
        'base_date_4': None,
        'authority_4': None,
        'basis_4': None,
        'land_value_5': None,
        'base_date_5': None,
        'authority_5': None,
        'basis_5': None,
        'source_file_name': 'mockSourceFile',
        'source_line_number': 0,
        'source_date': datetime(2012, 12, 12),
    }

    fields: Any = { **defaults, **kwargs }
    return RawLandValueRow(**fields)
