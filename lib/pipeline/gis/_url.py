from typing import Any, Dict, Optional, Self, Set, List
from urllib.parse import urlencode

from lib.service.http import url_with_params
from .config import GisProjection, FeaturePageDescription

UrlParams = Dict[str, str | bool | int]

def get_page_url(schema_url: str, params: UrlParams) -> str:
    return url_with_params(f'{schema_url}/query', params)

def get_page_url_params(
        offset: int,
        projection: GisProjection,
        feature_page: FeaturePageDescription) -> UrlParams:
    return {
        'returnGeometry': True,
        'resultOffset': offset,
        'resultRecordCount': projection.schema.result_limit,
        'where': feature_page.where_clause,
        'geometryType': 'esriGeometryEnvelope',
        'outSR': projection.epsg_crs,
        'outFields': ','.join(f.name for f in projection.get_fields()),
        'f': 'json',
    }

def get_count_url_params(where_clause: Optional[str]) -> UrlParams:
    return { 'where': where_clause or '1=1', 'returnCountOnly': True, 'f': 'json' }


