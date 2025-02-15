import asyncio
from dataclasses import dataclass
from logging import getLogger
from pprint import pformat
from typing import Any, Dict, Optional, Self, Set, List
from urllib.parse import urlencode

from lib.service.clock import ClockService
from lib.service.http import (
    AbstractClientSession,
    HttpLocalCache,
    CacheHeader,
    url_with_params,
)

from .config import GisProjection, FeaturePageDescription
from .cache_cleaner import AbstractCacheCleaner
from ._url import get_count_url_params, get_page_url_params

@dataclass(frozen=True)
class FeatureExpBackoff:
    allowed_attempts: int

class FeatureServerClient:
    """
    Client for GIS Feature Server
    """
    _logger = getLogger(f'{__name__}.GisApiClient')

    def __init__(self: Self,
                 exp_backoff_cfg: FeatureExpBackoff,
                 clock: ClockService,
                 session: AbstractClientSession,
                 cache_cleaner: AbstractCacheCleaner):
        self.exp_backoff_cfg = exp_backoff_cfg
        self._clock = clock
        self._session = session
        self._cache_cleaner = cache_cleaner

    async def get_page(
            self: Self,
            projection: GisProjection,
            feature_page: FeaturePageDescription) -> List[Any]:
        try:
            url_params = get_page_url_params(
                feature_page.offset,
                projection,
                feature_page)

            allowed_attempts = self.exp_backoff_cfg.allowed_attempts
            while allowed_attempts > 0:
                data = await self.get_json(
                    projection.schema.url,
                    params=url_params,
                    partition=projection.partition_key(),
                    use_cache=feature_page.use_cache,
                    cache_name='page')

                features = data.get('features', [])
                if len(features) < feature_page.expected_results:
                    attempt = self.exp_backoff_cfg.allowed_attempts - allowed_attempts
                    allowed_attempts -= 1

                    await self._cache_cleaner.forget_page_cache(projection, feature_page)
                    await self._clock.sleep(1 * (2 * attempt))
                else:
                    break
        except GisNetworkError as e:
            self._logger.error(f'failed on schema {projection.schema.url}')
            self._logger.error(f'failed on task {feature_page}')
            raise GisTaskNetworkError(feature_page, e.http_status, e.response)

        if len(features) < feature_page.expected_results:
            self._logger.error(f"Potenial data loss has occured, response:\n{pformat(data)}")
            await self._cache_cleaner.forget_partition_cache(projection, feature_page)
            raise MissingResultsError(
                f'{feature_page.where_clause} OFFSET {feature_page.offset}, '
                f'got {len(features)} wanted {feature_page.expected_results}')

        return features

    async def get_where_count(self: Self,
                              projection: GisProjection,
                              where_clause: Optional[str],
                              use_cache: bool) -> int:
        response = await self.get_json(
            projection.schema.url,
            get_count_url_params(where_clause),
            partition=projection.partition_key(),
            use_cache=use_cache,
            cache_name='count',
        )
        count = response.get('count', 0)
        self._logger.debug(f'count for "{where_clause}" is {count}')
        return count

    async def get_json(self: Self,
                       feature_url: str,
                       params: Dict[str, Any],
                       use_cache: bool,
                       partition: str,
                       cache_name=None):
        url = url_with_params(f'{feature_url}/query', params)
        try:
            async with self._session.get(url, headers={
                CacheHeader.EXPIRE: 'never' if use_cache else 'delta:days:2',
                CacheHeader.FORMAT: 'json',
                CacheHeader.LABEL: cache_name,
                CacheHeader.PARTITION: partition,
            }) as response:
                if response.status != 200:
                    self._logger.error(f"Crashed at {url}")
                    self._logger.error(response)
                    raise GisNetworkError(response.status, response)
                return await response.json()
        except asyncio.CancelledError:
            raise
        except:
            self._logger.error(f'failed on {url}')
            raise

class GisNetworkError(Exception):
    def __init__(self: Self, http_status, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_status = http_status
        self.response = response

class GisTaskNetworkError(GisNetworkError):
    def __init__(self: Self, task: FeaturePageDescription, http_status, response, *args, **kwargs):
        super().__init__(http_status, response, *args, **kwargs)
        self.task = task

class MissingResultsError(Exception):
    pass
