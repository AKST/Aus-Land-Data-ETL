from abc import ABC, abstractmethod
from logging import getLogger
from pprint import pformat
from typing import List, Set, Self
from urllib.parse import urlencode

from lib.service.http import HttpLocalCache

from .config import GisProjection, FeaturePageDescription
from ._url import get_page_url_params, UrlParams

def get_page_clauses(schema_url: str, params: UrlParams, keys: Set[str]) -> List[str]:
    return [schema_url, *[
        urlencode({key: value})
        for key, value in params.items()
        if key in keys
    ]]

class AbstractCacheCleaner(ABC):
    async def forget_page_cache(
        self: Self,
        projection: GisProjection,
        feature_page: FeaturePageDescription,
    ) -> None:
        await self.forget_cache(projection, feature_page, False)

    async def forget_partition_cache(
        self: Self,
        projection: GisProjection,
        feature_page: FeaturePageDescription,
    ) -> None:
        await self.forget_cache(projection, feature_page, True)

    @abstractmethod
    async def forget_cache(
        self: Self,
        projection: GisProjection,
        feature_page: FeaturePageDescription,
        forget_total_shard: bool,
    ) -> None:
        raise NotImplementedError()

class DisabledCacheCleaner(AbstractCacheCleaner):
    async def forget_cache(*args, **kwargs) -> None:
        return

class CacheCleaner(AbstractCacheCleaner):
    _logger = getLogger(__name__)

    def __init__(self: Self, file_cache: HttpLocalCache):
        self._http_file_cache = file_cache

    async def forget_cache(
        self: Self,
        projection: GisProjection,
        feature_page: FeaturePageDescription,
        forget_total_shard: bool,
    ) -> None:
        try:
            page_url_clauses = get_page_clauses(
                projection.schema.url,
                get_page_url_params(feature_page.offset, projection, feature_page),
                {'where'} if forget_total_shard else {'where', 'resultOffset'},
            )
            try:
                await self._http_file_cache.forget_by_clause(
                    clauses=page_url_clauses,
                    partition=projection.partition_key(),
                )
            except:
                self._logger.error(page_url_clauses)
                raise
        except:
            self._logger.error(f"failed to clear cache for\n" \
                               f"{pformat(projection)}\n{pformat(feature_page)}")
            raise

