from lib.pipeline.gis.config import GisSchema, GisProjection, SchemaField
from lib.pipeline.gis.predicate import DatePredicateFunction, FloatPredicateFunction

from ._constants import ENVIRONMENT_NSW
from ._shared import *

ENSW_DA_SCHEMA = GisSchema(
    url=f'{ENVIRONMENT_NSW}/Planning/DA_Tracking/MapServer/0',
    db_relation=None,
    debug_field='STATUS',
    shard_scheme=[
        DatePredicateFunction.create(field='SUBMITTED_DATE', default_range=(FIRST_YEAR, NEXT_YEAR)),
    ],
    id_field='PLANNING_PORTAL_APP_NUMBER',
    result_limit=100,
    result_depth=15000,
    fields=[
        SchemaField('id', "PLANNING_PORTAL_APP_NUMBER", 1),
        SchemaField('id', "DA_NUMBER", 1),
        SchemaField('meta', "LAST_UPDATED_DATE", 1),
        SchemaField('meta', "TIMESTAMP", 1),
        SchemaField('meta', "OBJECTID", 1),

        SchemaField('data:timeline', "SUBMITTED_DATE", 1),
        SchemaField('data:timeline', "DETERMINED_DATE", 1),
        SchemaField('data:timeline', "LODGEMENT_DATE", 1),
        SchemaField('data:timeline', "EXHIBIT_OR_NOTIFY_DATE_START", 1),
        SchemaField('data:timeline', "EXHIBIT_OR_NOTIFY_DATE_END", 1),
        SchemaField('data:timeline', "RETURNED_APPLICATION_DATE", 1),
        SchemaField('data:timeline', "SITE_INSPECTION_COMPLETED_DATE", 1),

        SchemaField('data:site', "SITE_ADDRESS", 1),
        SchemaField('data:site', "PRIMARY_ADDRESS", 1),
        SchemaField('data:site', "LGA_NAME", 1),
        SchemaField('data:site', "COUNCIL_NAME", 1),
        SchemaField('data:site', "POSTCODE", 1),
        SchemaField('data:site', "SUBURBNAME", 1),

        SchemaField('data:proposition', "GROSS_FLOOR_AREA_OF_BUILDING", 1),
        SchemaField('data:proposition', "UNITS_OR_DWELLINGS_PROPOSED", 1),
        SchemaField('data:proposition', "STOREYS_PROPOSED", 1),
        SchemaField('data:proposition', "PROPOSED_SUBDIVISION", 1),
        SchemaField('data:proposition', "STAFF_PROPOSED_NUMBER", 1),
        SchemaField('data:proposition', "PARKING_SPACES", 1),
        SchemaField('data:proposition', "LOADING_BAYS", 1),
        SchemaField('data:proposition', "NEW_ROAD_PROPOSED", 1),
        SchemaField('data:proposition', "PROPOSED_HERITAGE_TREE_REMOVAL", 1),
        SchemaField('data:proposition', "PROPOSED_CROWN_DEVELOPMENT", 1),
        SchemaField('data:proposition', "DWELLINGS_TO_BE_DEMOLISHED", 1),
        SchemaField('data:proposition', "DWELLINGS_TO_BE_CONSTRUCTED", 1),

        SchemaField('data', "DEVELOPMENT_SITE_OWNER", 1),
        SchemaField('data', "NUMBER_OF_EXISTING_LOTS", 1),
        SchemaField('data', "PRE_EXISTING_DWELLINGS_ON_SITE", 1),
        SchemaField('data', "PROPOSED_MODIFICATION_DESC", 1),

        SchemaField('data', "ASSESMENT_RESULT", 1),
        SchemaField('data', "DETERMINING_AUTHORITY", 1),
        SchemaField('data', "STATUS", 1),
        SchemaField('data', "APPLICATION_TYPE", 1),
        SchemaField('data', "COST_OF_DEVELOPMENT_RANGE", 1),
        SchemaField('data', "COST_OF_DEVELOPMENT", 1),
        SchemaField('data', "TYPE_OF_DEVELOPMENT", 1),
        SchemaField('data', "APPLIED_ON_BEHALF_OF_COMPANY", 1),
        SchemaField('data', "ANTICIPATED_DETERMINATION_BODY", 1),
        SchemaField('data', "SUBJECT_TO_SIC", 1),
        SchemaField('data', "VPA_NEEDED", 1),
        SchemaField('data', "TYPE_OF_MODIFICATION_REQUESTED", 1),
        SchemaField('data', "DA_NUMBER_PROPOSED_TO_BE_MOD", 1),
        SchemaField('data', "PROPOSED_MODIFICATION_DESC", 1),
        SchemaField('data', "DA_NUMBER_PROPOSED_TO_BE_REVD", 1),
        SchemaField('data', "DEVELOPMENT_DETAILED_DESC", 1),
        SchemaField('data', "STAGED_DEVELOPMENT", 1),
        SchemaField('data', "IS_AN_INTEGRATED_DA", 1),
        SchemaField('data', "IMPACTS_THREATENED_SPECIES", 1),
        SchemaField('data', "DEVELOPMENT_STANDARD_IN_EPI", 1),
        SchemaField('data', "APPROVAL_REQUIRED_UNDER_s68", 1),
        SchemaField('data', "DEV_INC_HERITAGE_ITEM_OR_AREA", 1),
        SchemaField('data', "DEV_PROP_TO_HERITAGE_BUILDINGS", 1),
        SchemaField('data', "NEWBUILD_TO_OTHER_NEWBUILD", 1),
        SchemaField('data', "NEWRESBUILD_TO_ATTACH", 1),
        SchemaField('data', "CONCURRENCE_OR_REFERRAL_SOUGHT", 1),
        SchemaField('data', "APP_NOTIFIED_OR_EXHIBITED", 1),
        SchemaField('geo', "X", 1),
        SchemaField('geo', "Y", 1),
        SchemaField('meta', "TYPE_OF_DEVELOPMENT_GROUPING", 3),
        # SchemaField('geo', "SHAPE", 1),
    ]
)

ENSW_DA_PROJECTION = GisProjection(
    id="nsw_planning_da",
    schema=ENSW_DA_SCHEMA,
    fields='*',
    epsg_crs=GDA2020_CRS)


