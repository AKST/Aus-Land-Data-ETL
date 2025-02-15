from lib.pipeline.gis.config import GisSchema, GisProjection, SchemaField
from lib.pipeline.gis.predicate import DatePredicateFunction

from ._constants import ENVIRONMENT_NSW
from ._shared import *

ENSW_ZONE_SCHEMA = GisSchema(
    url=f'{ENVIRONMENT_NSW}/Planning/Principal_Planning_Layers/MapServer/11',
    db_relation=None,
    debug_field='SYM_CODE',
    shard_scheme=[
        DatePredicateFunction.create(field='PUBLISHED_DATE', default_range=(FIRST_YEAR, NEXT_YEAR)),
    ],
    id_field='OBJECTID',
    result_limit=100,
    result_depth=15000,
    fields=[
        SchemaField('data', "OBJECTID", 1),
        SchemaField('data', "EPI_NAME", 2),
        SchemaField('data', "LGA_NAME", 1),
        SchemaField('data', "PUBLISHED_DATE", 1),
        SchemaField('data', "COMMENCED_DATE", 1),
        SchemaField('data', "CURRENCY_DATE", 1),
        SchemaField('data', "AMENDMENT", 1),
        SchemaField('data', "LAY_CLASS", 1),
        SchemaField('data', "SYM_CODE", 1),
        SchemaField('data', "PURPOSE", 1),
        SchemaField('data', "LEGIS_REF_AREA", 1),
        SchemaField('data', "EPI_TYPE", 1),
        # SchemaField('geo', "SHAPE", 1),
    ],
)


ENSW_ZONE_PROJECTION = GisProjection(
    id="nsw_planning_zoning",
    schema=ENSW_ZONE_SCHEMA,
    fields='*',
    epsg_crs=GDA2020_CRS)

