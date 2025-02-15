from lib.pipeline.gis.config import GisSchema, GisProjection, SchemaField
from lib.pipeline.gis.predicate import DatePredicateFunction, FloatPredicateFunction

from ._constants import SPATIAL_NSW
from ._shared import *

SNSW_LOT_SCHEMA = GisSchema(
    url=f'{SPATIAL_NSW}/NSW_Land_Parcel_Property_Theme/FeatureServer/8',
    db_relation='nsw_spatial_lppt_raw.lot_feature_layer',
    debug_field='Shape__Area',
    shard_scheme=[
        DatePredicateFunction.create(field='lastupdate', default_range=(FIRST_YEAR, NEXT_YEAR)),
        FloatPredicateFunction(field='Shape__Area', default_range=(0.0, AREA_MAX)),
    ],
    id_field='objectid',
    result_limit=75,
    result_depth=15000,
    fields=[
        SchemaField('id', 'objectid', 1, rename='object_id'),
        SchemaField('assoc', 'lotidstring', 1, rename='lot_id_string'),
        SchemaField('assoc', 'controllingauthorityoid', 1, rename='controlling_authority_oid'),
        SchemaField('assoc', 'cadid', 2, rename='cad_id'),

        SchemaField('meta', 'createdate', 1, rename='create_date', format='timestamp_ms'),
        SchemaField('meta', 'modifieddate', 1, rename='modified_date', format='timestamp_ms'),
        SchemaField('meta', 'startdate', 1, rename='start_date', format='timestamp_ms'),
        SchemaField('meta', 'enddate', 1, rename='end_date', format='timestamp_ms'),
        SchemaField('meta', 'lastupdate', 1, rename='last_update', format='timestamp_ms'),

        SchemaField('data', 'planoid', 2, rename='plan_oid', format='number'),
        SchemaField('data', 'plannumber', 1, rename='plan_number', format='number'),
        SchemaField('data', 'planlabel', 1, rename='plan_label'),
        SchemaField('data', 'itstitlestatus', 2, rename='its_title_status', format='number'),
        SchemaField('data', 'itslotid', 2, rename='its_lot_id', format='number'),
        SchemaField('data', 'stratumlevel', 2, rename='stratum_level'),
        SchemaField('data', 'hasstratum', 2, rename='has_stratum'),
        SchemaField('data', 'classsubtype', 2, rename='class_subtype', format='number'),
        SchemaField('data', 'lotnumber', 1, rename='lot_number'),
        SchemaField('data', 'sectionnumber', 1, rename='section_number'),
        SchemaField('data', 'planlotarea', 3, rename='plan_lot_area', format='number'),
        SchemaField('data', 'planlotareaunits', 3, rename='plan_lot_area_units'),

        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),

        SchemaField('meta', 'shapeuuid', 2, rename='shape_uuid'),
        SchemaField('meta', 'changetype', 3, rename='change_type'),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('geo', 'Shape__Length', 1, rename='shape_length'),
        SchemaField('geo', 'Shape__Area', 1, rename='shape_area'),
    ],
)

SNSW_LOT_PROJECTION = GisProjection(
    id="nsw_spatial_lot",
    schema=SNSW_LOT_SCHEMA,
    fields=FIELD_PRIORITY,
    epsg_crs=GDA2020_CRS)


