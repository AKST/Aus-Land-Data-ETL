from typing import Self, List, Set, Union, Any, Tuple, Generator, Literal
from logging import getLogger
import re

from . import types as t
from . import grammar as g
from .parcel_parser import ParcelsParser, parse_parcel_data
from .. import data
from ..builder import PropertyDescriptionBuilder

logger = getLogger(__name__)

def parse_land_parcel_ids(desc: str):
    parser = ParcelsParser(desc)
    parcels = list(parser.read_parcels())
    return parser.remains, parcels

def parse_property_description(description: str) -> Tuple[str, List[t.ParseItem]]:
    parsed_items: List[t.ParseItem] = []

    for s_pattern in g.sanitize_patterns:
        description = s_pattern.re.sub(s_pattern.out, description)

    for s_pattern in g.sanitize_pre_parcels_patterns:
        description = s_pattern.re.sub(s_pattern.out, description)

    description = re.sub(r'\s+', ' ', description)
    description, land_parcels = parse_land_parcel_ids(description)
    parsed_items.extend(land_parcels)

    for s_pattern in g.sanitize_post_parcels_patterns:
        description = s_pattern.re.sub(s_pattern.out, description)

    for i_pattern in g.ignore_pre_patterns:
        description = i_pattern.sub('', description)

    for id_pattern in g.id_patterns:
        for match in id_pattern.re.finditer(description):
            parsed_items.append(id_pattern.Const(id=match.group(1)))
        description = id_pattern.re.sub('', description)

    for n_pattern in g.named_group_patterns:
        for match in n_pattern.re.finditer(description):
            parsed_item = n_pattern.Const(
                **{ k: match.group(k) for k in n_pattern.id_names },
                **{
                    k: match.group(k) is not None
                    for k in n_pattern.bool_names
                },
            )
            parsed_items.append(parsed_item)
        description = n_pattern.re.sub('', description)

    for f_pattern in g.flag_patterns:
        for match in f_pattern.re.finditer(description):
            parsed_items.append(f_pattern.Const())
        description = f_pattern.re.sub('', description)

    for i_pattern in g.ignore_post_patterns:
        description = i_pattern.sub('', description)

    description = re.sub(r'\s+', ' ', description)
    description = '' if description == ' ' else description

    return description, parsed_items

def parse_property_description_data(desc: str) -> Tuple[data.PropertyDescription, str]:
    builder = PropertyDescriptionBuilder()
    desc_out, items = parse_property_description(desc)

    try:
        for item in items:
            match item:
                case t.Folio(folio_id, partial):
                    folio = parse_parcel_data(folio_id)
                    builder.add_folio(folio, partial)
                case t.EnclosurePermit(id):
                    builder.add_permit('enclosure', id)

        return builder.create(desc), desc_out
    except Exception as e:
        logger.error(f'failed with "{desc}"')
        logger.error(f'remains "{desc_out}"')
        logger.error(f'items "{items}"')
        raise e
