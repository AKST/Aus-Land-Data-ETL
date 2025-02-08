import abc
from dataclasses import dataclass, field, fields, Field
from datetime import datetime
from typing import Optional, Self, Literal, List, Tuple

from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.pipeline.nsw_vg.raw_data.zoning import ZoningKind

class BasePropertySaleFileRow(abc.ABC):
    @abc.abstractmethod
    def db_columns(self: Self) -> List[str]:
        raise NotImplementedError('ahh')

@dataclass
class SaleRecordFileLegacy(BasePropertySaleFileRow):
    ps_row_a_legacy_id: str
    position: int
    file_path: str = field(repr=False)
    year_of_sale: int = field(repr=False)
    submitting_user_id: Optional[str]
    date_provided: datetime

    def db_columns(self: Self) -> List[str]:
        return list(map(lambda f: f.name, fields(self)))

@dataclass
class SalePropertyDetails1990(BasePropertySaleFileRow):
    ps_row_b_legacy_id: str
    position: int
    file_path: str = field(repr=False)
    parent: SaleRecordFileLegacy = field(repr=False)
    district_code: int
    source: Optional[str]
    valuation_number: Optional[str]
    property_id: Optional[int]
    unit_number: Optional[str]
    house_number: Optional[str]
    street_name: Optional[str]
    locality_name: Optional[str]
    postcode: Optional[str]
    contract_date: Optional[datetime]
    purchase_price: Optional[float]
    land_description: Optional[str]
    area: Optional[float]
    area_type: Optional[str]
    dimensions: Optional[str]
    comp_code: Optional[str]
    zone_code: Optional[str]
    zone_standard: ZoningKind | None

    def db_columns(self: Self) -> List[str]:
        not_allowed = {'parent'}
        return [f.name for f in fields(self) if f.name not in not_allowed]

@dataclass
class SaleRecordFile(BasePropertySaleFileRow):
    ps_row_a_id: str
    position: int
    year_of_sale: int
    file_path: str = field(repr=False)
    file_type: Optional[str]
    district_code: int
    date_provided: datetime
    submitting_user_id: str

    def db_columns(self: Self) -> List[str]:
        return list(map(lambda f: f.name, fields(self)))

@dataclass
class SalePropertyDetails(BasePropertySaleFileRow):
    ps_row_b_id: str
    position: int
    file_path: str = field(repr=False)
    parent: SaleRecordFile = field(repr=False)
    district_code: int
    property_id: Optional[int]
    sale_counter: int
    date_provided: datetime
    property_name: Optional[str]
    unit_number: Optional[str]
    house_number: Optional[str]
    street_name: Optional[str]
    locality_name: Optional[str]
    postcode: Optional[str]
    area: Optional[float]
    area_type: Optional[str]
    contract_date: Optional[datetime]
    settlement_date: Optional[datetime]
    """
    From 2002 onwards always defined.
    """
    purchase_price: Optional[float]
    zone_code: Optional[str]
    zone_standard: ZoningKind | None
    nature_of_property: str
    primary_purpose: Optional[str]
    strata_lot_number: Optional[int]
    comp_code: Optional[str]
    sale_code: Optional[str]
    interest_of_sale: Optional[int]
    dealing_number: str

    def db_columns(self: Self) -> List[str]:
        not_allowed = {'parent'}
        return [f.name for f in fields(self) if f.name not in not_allowed]

@dataclass
class SalePropertyLegalDescription(BasePropertySaleFileRow):
    ps_row_c_id: str
    position: int
    file_path: str = field(repr=False)
    parent: SalePropertyDetails = field(repr=False)

    district_code: int

    """
    Missing in property sale records from July 2001
    """
    property_id: Optional[int]
    sale_counter: int
    date_provided: datetime
    property_description: Optional[str]

    def db_columns(self: Self) -> List[str]:
        not_allowed = {'parent'}
        return [f.name for f in fields(self) if f.name not in not_allowed]

@dataclass
class SaleParticipant(BasePropertySaleFileRow):
    ps_row_d_id: str
    position: int
    file_path: str = field(repr=False)
    parent: SalePropertyLegalDescription = field(repr=False)
    district_code: int
    """
    Missing in property sale records from July 2001
    """
    property_id: Optional[int]
    sale_counter: int
    date_provided: datetime
    participant: str

    def db_columns(self: Self) -> List[str]:
        not_allowed = {'parent'}
        return [f.name for f in fields(self) if f.name not in not_allowed]

@dataclass
class SaleDataFileSummary(BasePropertySaleFileRow):
    position: int
    file_path: str = field(repr=False)
    parent: SaleRecordFile = field(repr=False)
    total_records: int
    total_sale_property_details: int

    # fields not provided in 1990 format
    total_sale_property_legal_descriptions: int

    # fields not provided in 1990 format
    total_sale_participants: int

    def db_columns(self: Self) -> List[str]:
        not_allowed = {'parent'}
        return [f.name for f in fields(self) if f.name not in not_allowed]

