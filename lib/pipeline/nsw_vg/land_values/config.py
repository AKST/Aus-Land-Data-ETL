from abc import ABC
from datetime import datetime
from dataclasses import dataclass, fields, field
from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Self,
    Set,
    Tuple,
    Union,
)

import lib.pipeline.nsw_vg.raw_data.rows as util
from lib.pipeline.nsw_vg.raw_data.zoning import ZoningKind

from ..discovery import NswVgTarget

@dataclass
class ByoLandValue:
    src_dst: str
    datetime: datetime

class DiscoveryMode:
    class T(ABC): pass
    class Latest(T): pass
    class EachYear(T): pass
    class All(T): pass

    @dataclass
    class EachNthYear(T):
        n: int
        include_first: bool

    @dataclass
    class TheseYears(T):
        years: Set[int]

    @staticmethod
    def from_text(t: str) -> 'DiscoveryMode.T':
        match t:
            case 'all': return DiscoveryMode.All()
            case 'each-year': return DiscoveryMode.EachYear()
            case 'evert-2nd-year': return DiscoveryMode.EachNthYear(2, False)
            case 'evert-3rd-year': return DiscoveryMode.EachNthYear(3, False)
            case 'evert-4th-year': return DiscoveryMode.EachNthYear(4, False)
            case 'every-5th-year': return DiscoveryMode.EachNthYear(5, False)
            case 'latest': return DiscoveryMode.Latest()
            case other:
                raise ValueError(f'unknown lv discovery mode {t}')

class NswVgLvTaskDesc:
    class Base:
        pass

    @dataclass(frozen=True)
    class Parse(Base):
        file: str
        size: int
        target: Union[ByoLandValue, NswVgTarget] = field(repr=False)

    @dataclass(frozen=True)
    class Load(Base):
        file: str
        offset: int
        rows: List['RawLandValueRow'] = field(repr=False)

class NswVgLvParentMsg:
    class Base:
        pass

    @dataclass(frozen=True)
    class FileRowsParsed(Base):
        sender: int
        file: str
        size: int

    @dataclass(frozen=True)
    class FileRowsSaved(Base):
        sender: int
        file: str
        size: int

class NswVgLvChildMsg:
    class Base:
        def workload(self: Self) -> int:
            return 0

    @dataclass(frozen=True)
    class RequestClose(Base):
        pass

    @dataclass(frozen=True)
    class Ingest(Base):
        task: NswVgLvTaskDesc.Parse = field(repr=False)

        def workload(self: Self) -> int:
            return self.task.size

@dataclass(frozen=True)
class RawLandValueRow:
    district_code: int
    district_name: Optional[str]
    property_id: int
    property_type: str
    property_name: Optional[str]
    unit_number: Optional[str]
    house_number: Optional[str]
    street_name: Optional[str]
    suburb_name: str
    postcode: Optional[str]
    property_description: Optional[str]
    zone_code: Optional[str]
    zone_standard: ZoningKind | None
    area: Optional[float]
    area_type: Optional[str]
    land_value_1: Optional[float]
    base_date_1: Optional[datetime]
    authority_1: Optional[str]
    basis_1: Optional[str]
    land_value_2: Optional[float]
    base_date_2: Optional[datetime]
    authority_2: Optional[str]
    basis_2: Optional[str]
    land_value_3: Optional[float]
    base_date_3: Optional[datetime]
    authority_3: Optional[str]
    basis_3: Optional[str]
    land_value_4: Optional[float]
    base_date_4: Optional[datetime]
    authority_4: Optional[str]
    basis_4: Optional[str]
    land_value_5: Optional[float]
    base_date_5: Optional[datetime]
    authority_5: Optional[str]
    basis_5: Optional[str]

    source_file_name: str
    source_line_number: int
    source_date: datetime

    @staticmethod
    def from_row(row: Dict[str, str],
                 uuid: str,
                 line_number: int,
                 file_path: str,
                 source_date: datetime) -> 'RawLandValueRow':
        return RawLandValueRow(
            district_code=util.read_int(row, 'DISTRICT CODE', 'district_code'),
            district_name=row['DISTRICT NAME'] or None,
            property_id=util.read_int(row, 'PROPERTY ID', 'property_id'),
            property_type=util.read_str(row, 'PROPERTY TYPE', 'property_type'),
            property_name=row['PROPERTY NAME'] or None,
            unit_number=row['UNIT NUMBER'] or None,
            house_number=row['HOUSE NUMBER'] or None,
            street_name=row['STREET NAME'] or None,
            suburb_name=util.read_str(row, 'SUBURB NAME', 'suburb_name'),
            postcode=util.read_postcode(row, 'POSTCODE', 'postcode'),
            property_description=row['PROPERTY DESCRIPTION'] or None,
            zone_code=util.StrCheck(max_len=4).read_optional(row, 'ZONE CODE', 'zone_code'),
            zone_standard=util.read_zone_std(row, 'ZONE CODE', 'zone_standard'),
            area=util.read_optional_float(row, 'AREA', 'area'),
            area_type=util.read_area_type(row, 'AREA TYPE', 'area_type'),
            land_value_1=util.read_optional_float(row, 'LAND VALUE 1', 'land_value_1'),
            base_date_1=util.read_optional_date_pre_2002(row, 'BASE DATE 1', 'base_date_1'),
            authority_1=row['AUTHORITY 1'] or None,
            basis_1=row['BASIS 1'] or None,
            land_value_2=util.read_optional_float(row, 'LAND VALUE 2', 'land_value_2'),
            base_date_2=util.read_optional_date_pre_2002(row, 'BASE DATE 2', 'base_date_2'),
            authority_2=row['AUTHORITY 2'] or None,
            basis_2=row['BASIS 2'] or None,
            land_value_3=util.read_optional_float(row, 'LAND VALUE 3', 'land_value_3'),
            base_date_3=util.read_optional_date_pre_2002(row, 'BASE DATE 3', 'base_date_3'),
            authority_3=row['AUTHORITY 3'] or None,
            basis_3=row['BASIS 3'] or None,
            land_value_4=util.read_optional_float(row, 'LAND VALUE 4', 'land_value_4'),
            base_date_4=util.read_optional_date_pre_2002(row, 'BASE DATE 4', 'base_date_4'),
            authority_4=row['AUTHORITY 4'] or None,
            basis_4=row['BASIS 4'] or None,
            land_value_5=util.read_optional_float(row, 'LAND VALUE 5', 'land_value_5'),
            base_date_5=util.read_optional_date_pre_2002(row, 'BASE DATE 5', 'base_date_5'),
            authority_5=row['AUTHORITY 5'] or None,
            basis_5=row['BASIS 5'] or None,
            source_file_name=file_path,
            source_line_number=line_number,
            source_date=source_date,
        )

    def db_columns(self) -> List[str]:
        return [f.name for f in fields(self)]
