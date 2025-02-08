import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generic, Self, List, TypeVar, Optional, Protocol

from lib.pipeline.nsw_vg.property_sales import data as t
from lib.pipeline.nsw_vg.raw_data.rows import *
from lib.service.uuid import UuidService

class AbstractFormatFactory(abc.ABC):
    @classmethod
    def create(cls, uuid: UuidService, year: int, file_path: str) -> 'AbstractFormatFactory':
        raise NotImplementedError('create not implemented on AbstractFormatFactory')

    @abc.abstractmethod
    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]) -> t.BasePropertySaleFileRow:
        pass

    @abc.abstractmethod
    def create_b(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]) -> t.BasePropertySaleFileRow:
        pass

    @abc.abstractmethod
    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]) -> t.SalePropertyLegalDescription:
        pass

    @abc.abstractmethod
    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]) -> t.SaleParticipant:
        pass

    @abc.abstractmethod
    def create_z(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]) -> t.SaleDataFileSummary:
        pass

class CurrentFormatFactory(AbstractFormatFactory):
    zone_standard: t.ZoningKind = 'ep&a_2006'
    zone_code_len: int = 3

    def __init__(self: Self,
                 uuid: UuidService,
                 year: int,
                 file_path: str,
                 file_path_uuid: str):
        self.uuid = uuid
        self.year = year
        self.file_path = file_path
        self.file_path_uuid = file_path_uuid

    @classmethod
    def create(Cls, uuid: UuidService, year: int, file_path: str) -> 'CurrentFormatFactory':
        return CurrentFormatFactory(uuid, year, file_path, uuid.get_uuid4_hex())

    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            ps_row_a_id=self.uuid.get_uuid4_hex(),
            position=pos,
            year_of_sale=self.year,
            file_path=self.file_path,
            file_type=row[0] or None,
            district_code=read_int(row, 1, 'district_code'),
            date_provided=read_datetime(row, 2, 'date_provided'),
            submitting_user_id=read_str(row, 3, 'submitting_user_id'),
        )

    def create_b(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SalePropertyDetails(
            ps_row_b_id=self.uuid.get_uuid4_hex(),
            position=pos,
            file_path=self.file_path,
            parent=a_record,
            district_code=read_int(row, 0, 'district_code'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=read_int(row, 2, 'sale_counter'),
            date_provided=read_datetime(row, 3, 'date_provided'),
            property_name=row[4] or None,
            unit_number=row[5] or None,
            house_number=row[6] or None,
            street_name=row[7] or None,
            locality_name=row[8] or None,
            postcode=read_postcode(row, 9, 'property_postcode'),
            area=read_optional_float(row, 10, 'area'),
            area_type=read_area_type(row, 11, 'area_type'),
            contract_date=read_optional_date(row, 12, 'contract_date'),
            settlement_date=read_optional_date(row, 13, 'settlement_date'),
            purchase_price=read_optional_float(row, 14, 'purchase_price'),
            zone_code=StrCheck(max_len=self.zone_code_len).read_optional(row, 15, 'zone_code'),
            zone_standard=read_zone_std(row, 15, 'zone_code'),
            nature_of_property=read_str(row, 16, 'nature_of_property'),
            primary_purpose=row[17] or None,
            strata_lot_number=read_optional_int(row, 18, 'strata_lot_number'),
            comp_code=row[19] or None,
            sale_code=row[20] or None,
            interest_of_sale=read_optional_int(row, 21, 'interest_of_sale'),
            dealing_number=read_str(row, 22, 'dealing_number'),
        )

    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]):
        return t.SalePropertyLegalDescription(
            ps_row_c_id=self.uuid.get_uuid4_hex(),
            position=pos,
            file_path=self.file_path,
            parent=b_record,
            district_code=read_int(row, 0, 'district_code'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=read_int(row, 2, 'sale_counter'),
            date_provided=read_datetime(row, 3, 'date_provided'),
            property_description=row[4] or None,
        )

    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]):
        return t.SaleParticipant(
            ps_row_d_id=self.uuid.get_uuid4_hex(),
            position=pos,
            file_path=self.file_path,
            parent=c_record,
            district_code=read_int(row, 0, 'district_code'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=read_int(row, 2, 'sale_counter'),
            date_provided=read_datetime(row, 3, 'date_provided'),
            participant=read_str(row, 4, 'participant'),
        )

    def create_z(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SaleDataFileSummary(
            position=pos,
            file_path=self.file_path,
            parent=a_record,
            total_records=read_int(row, 0, 'total_records'),
            total_sale_property_details=read_int(row, 1, 'total_sale_property_details'),
            total_sale_property_legal_descriptions=read_int(row, 2, 'total_sale_property_legal_descriptions'),
            total_sale_participants=read_int(row, 3, 'total_sale_participants'),
        )

class Legacy2002Format(CurrentFormatFactory):
    zone_standard = 'legacy_vg_2011'
    zone_code_len = 4

    @classmethod
    def create(cls, uuid: UuidService, year: int, file_path: str) -> 'Legacy2002Format':
        return Legacy2002Format(uuid, year, file_path, uuid.get_uuid4_hex())

    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            ps_row_a_id=self.uuid.get_uuid4_hex(),
            position=pos,
            file_path=self.file_path,
            year_of_sale=self.year,
            file_type=None,
            district_code=read_int(row, 0, 'district_code'),
            date_provided=read_datetime(row, 1, 'date_provided'),
            submitting_user_id=row[2],
        )

    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]):
        if variant is None:
            return super().create_c(pos, row, b_record, variant)
        elif variant == 'missing_property_id':
            return t.SalePropertyLegalDescription(
                ps_row_c_id=self.uuid.get_uuid4_hex(),
                position=pos,
                file_path=self.file_path,
                parent=b_record,
                district_code=read_int(row, 0, 'district_code'),
                property_id=None,
                sale_counter=read_int(row, 1, 'sale_counter'),
                date_provided=read_datetime(row, 2, 'date_provided'),
                property_description=row[3] or None,
            )
        else:
            raise TypeError(f'unknown variant {variant}')

    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]):
        if variant is None:
            return super().create_d(pos, row, c_record, variant)
        elif variant == 'missing_property_id':
            return t.SaleParticipant(
                ps_row_d_id=self.uuid.get_uuid4_hex(),
                position=pos,
                file_path=self.file_path,
                parent=c_record,
                district_code=read_int(row, 0, 'district_code'),
                property_id=None,
                sale_counter=read_int(row, 1, 'sale_counter'),
                date_provided=read_datetime(row, 2, 'date_provided'),
                participant=read_str(row, 3, 'participant'),
            )
        else:
            raise TypeError(f'unknown variant {variant}')

class Legacy1990Format(AbstractFormatFactory):
    def __init__(self: Self,
                 uuid: UuidService,
                 year: int,
                 file_path: str,
                 file_path_uuid: str):
        self.uuid = uuid
        self.year = year
        self.file_path = file_path
        self.file_path_uuid = file_path_uuid

    @classmethod
    def create(cls, uuid: UuidService, year: int, file_path: str):
        return Legacy1990Format(uuid, year, file_path, uuid.get_uuid4_hex())

    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]):
        """
        Column 0 in the row will always be empty. I guess is this is
        to maintain some level of consistency with the later formats.
        """
        return t.SaleRecordFileLegacy(
            ps_row_a_legacy_id=self.uuid.get_uuid4_hex(),
            position=pos,
            file_path=self.file_path,
            year_of_sale=self.year,
            submitting_user_id=row[1],
            date_provided=read_datetime(row, 2, 'date_provided'),
        )

    def create_b(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SalePropertyDetails1990(
            ps_row_b_legacy_id=self.uuid.get_uuid4_hex(),
            position=pos,
            file_path=self.file_path,
            parent=a_record,
            district_code=read_int(row, 0, 'district_code'),
            source=row[1] or None,
            valuation_number=row[2] or None,
            property_id=read_optional_int(row, 3, 'property_id'),
            unit_number=row[4] or None,
            house_number=row[5] or None,
            street_name=row[6] or None,
            locality_name=row[7] or None,
            postcode=read_postcode(row, 8, 'property_postcode'),
            contract_date=read_date_pre_2002(row, 9, 'contract_date'),
            purchase_price=read_float(row, 10, 'purchase_price'),
            land_description=row[11] or None,
            area=read_optional_float(row, 12, 'area'),
            area_type=read_area_type(row, 13, 'area_type'),
            dimensions=row[14] or None,
            comp_code=row[15] or None,
            zone_code=StrCheck(max_len=4).read_optional(row, 16, 'zone_code'),
            zone_standard=read_zone_std(row, 16, 'zone_standard'),
        )

    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]):
        raise TypeError('c record not allowed in 1990 format')

    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]):
        raise TypeError('d record not allowed in 1990 format')

    def create_z(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SaleDataFileSummary(
            position=pos,
            file_path=self.file_path,
            parent=a_record,
            total_records=read_int(row, 0, 'total_records'),
            total_sale_property_details=read_int(row, 1, 'total_sale_property_details'),

            # field not provided in 1990 format
            total_sale_property_legal_descriptions=0,
            total_sale_participants=0,
        )


