from lib.pipeline.gis.config import FieldPriority

from datetime import datetime

FIRST_YEAR = 2000
NEXT_YEAR = datetime.now().year + 1
AREA_MAX = 1_000_000_000_000_000

FIELD_PRIORITY: FieldPriority = ['id', ('assoc', 2), ('data', 2), ('meta', 2), 'geo']

GDA2020_CRS = 7844


