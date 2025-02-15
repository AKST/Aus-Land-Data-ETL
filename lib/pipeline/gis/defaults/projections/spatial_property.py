from lib.pipeline.gis.config import GisSchema, GisProjection, SchemaField
from lib.pipeline.gis.predicate import DatePredicateFunction, FloatPredicateFunction

from ._constants import SPATIAL_NSW
from ._shared import *

SNSW_PROP_SCHEMA = GisSchema(
    url=f'{SPATIAL_NSW}/NSW_Land_Parcel_Property_Theme/FeatureServer/12',
    db_relation='nsw_spatial_lppt_raw.property_feature_layer',
    debug_field='Shape__Area',
    shard_scheme=[
        DatePredicateFunction.create(field='lastupdate', default_range=(FIRST_YEAR, NEXT_YEAR)),
        FloatPredicateFunction(field='Shape__Area', default_range=(0.0, AREA_MAX)),
    ],
    id_field='RID',
    result_limit=75,
    result_depth=15000,
    fields=[
        SchemaField('id', 'RID', 1, rename='rid'),
        SchemaField('meta', 'createdate', 1, rename='create_date', format='timestamp_ms'),
        SchemaField('assoc', 'propid', 1, rename='property_id'),
        SchemaField('assoc', 'gurasid', 3),
        SchemaField('assoc', 'principaladdresssiteoid', 2, rename='principal_address_site_oid', format='number'),

        # no real clue what valnet, can only presume they
        # are a company that does valuations or a data
        # base where they are stored.
        SchemaField('data', 'valnetpropertystatus', 3),
        SchemaField('data', 'valnetpropertytype', 3),
        SchemaField('data', 'valnetlotcount', 3),
        SchemaField('assoc', 'valnetworkflowid', 3),

        # No clue what this is
        SchemaField('data', 'propertytype', 2, rename='property_type'),
        SchemaField('data', 'dissolveparcelcount', 2, rename='dissolve_parcel_count', format='number'),
        SchemaField('data', 'superlot', 1, rename='super_lot'),
        SchemaField('data', 'housenumber', 3, rename='house_number'),
        SchemaField('data', 'address', 1),
        SchemaField('meta', 'startdate', 1, rename='start_date', format='timestamp_ms'),
        SchemaField('meta', 'enddate', 1, rename='end_date', format='timestamp_ms'),
        SchemaField('meta', 'lastupdate', 1, rename='last_update', format='timestamp_ms'),
        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),
        SchemaField('meta', 'shapeuuid', 2, rename='shape_uuid'),
        SchemaField('meta', 'changetype', 3, rename='change_type'),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('data', 'principaladdresstype', 2, rename='principal_address_type', format='number'),
        SchemaField('assoc', 'addressstringoid', 1, rename='address_string_oid', format='number'),
        SchemaField('geo', 'Shape__Length', 1, rename='shape_length'),
        SchemaField('geo', 'Shape__Area', 1, rename='shape_area'),
    ],
)

SNSW_PROP_PROJECTION = GisProjection(
    id="nsw_spatial_property",
    schema=SNSW_PROP_SCHEMA,
    fields=FIELD_PRIORITY,
    epsg_crs=GDA2020_CRS)
