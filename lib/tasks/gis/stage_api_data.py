from datetime import datetime
from dataclasses import dataclass
from functools import reduce
import geopandas as gpd
from logging import getLogger
import math
import pandas as pd
import time
from typing import Any, List, Optional

from lib.pipeline.gis import (
    BACKOFF_CONFIG,
    AbstractCacheCleaner,
    CacheCleaner,
    DisabledCacheCleaner,
    FeaturePaginationSharderFactory,
    FeatureServerClient,
    FeatureExpBackoff,
    GisIngestion,
    GisIngestionConfig,
    GisPipeline,
    GisPipelineTelemetry,
    GisProjection,
    DateRangeParam,
    HOST_SEMAPHORE_CONFIG,
    ENSW_DA_PROJECTION,
    SNSW_LOT_PROJECTION,
    SNSW_PROP_PROJECTION,
    ENSW_ZONE_PROJECTION,
    YearMonth,
)
from lib.service.database import *
from lib.service.io import IoService
from lib.service.clock import ClockService
from lib.service.http import (
    CachedClientSession,
    ExpBackoffClientSession,
    HostSemaphoreConfig,
    HttpLocalCache,
    ThrottledClientSession,
)
from lib.service.http.middleware.exp_backoff import BackoffConfig, RetryPreference
from lib.tooling.schema import SchemaController, SchemaDiscovery, SchemaCommand

from .config import GisTaskConfig

def http_limits_of(ss: List[HostSemaphoreConfig]) -> int:
    return reduce(lambda acc, it: acc + it.limit, ss, 0)

def _parse_date_range(s: Optional[str], clock: ClockService) -> Optional[DateRangeParam]:
    match s:
        case None:
            return None
        case '':
            return None
        case non_null_str:
            pass

    match [datetime.strptime(s, '%Y%m') for s in non_null_str.split(':')]:
        case [datetime(year=y1), datetime(year=y2)] if y1 > y2:
            raise ValueError('first year greater than second')
        case [datetime(month=m1), datetime(month=m2)] if m1 > m2:
            raise ValueError('first month greater than second')
        case [datetime(year=y1, month=m1), datetime(year=y2, month=m2)]:
            return DateRangeParam(YearMonth(y1, m1), YearMonth(y2, m2), clock)
        case other:
            raise ValueError('unknown date format')

async def stage_gis_api_data(
    io: IoService,
    db: DatabaseService,
    clock: ClockService,
    conf: GisTaskConfig.StageApiData,
) -> None:
    """
    The http client composes a cache layer and a throttling
    layer. Some GIS serves, such as the ones below may end
    up blocking you if you perform to much traffic at once.

      - Caching ensure requests that do not need to be
        repeated, are not repeated.

      - Expotenial backoff is here to retry when in the
        the event any requests to the GIS server fail.

      - Throttling of course ensure we do not have more
        active requests to one host at a time. The max
        active requests is set on a host basis.
    """

    def get_session(cacher: Optional[HttpLocalCache]):
        exp_boff_sesh = ExpBackoffClientSession.create(
            session=ThrottledClientSession.create(HOST_SEMAPHORE_CONFIG),
            config=BACKOFF_CONFIG,
        )

        if cacher is None:
            return exp_boff_sesh
        else:
            return CachedClientSession.create(
                session=exp_boff_sesh,
                file_cache=cacher,
                io_service=io,
            )

    cache_cleaner: AbstractCacheCleaner

    if conf.disable_cache:
        http_file_cache = None
        cache_cleaner = DisabledCacheCleaner()
    else:
        http_file_cache = HttpLocalCache.create(io, 'gis')
        cache_cleaner = CacheCleaner(http_file_cache)

    projections: List[GisProjection] = []

    if 'snsw_lot' in conf.projections:
        projections.append(SNSW_LOT_PROJECTION)
    if 'snsw_prop' in conf.projections:
        projections.append(SNSW_PROP_PROJECTION)

    match conf.db_mode:
        case 'write':
            api_workers = http_limits_of(HOST_SEMAPHORE_CONFIG)
            db_workers = conf.db_workers
        case 'skip':
            api_workers = http_limits_of(HOST_SEMAPHORE_CONFIG)
            db_workers = 1
        case 'print_head_then_quit':
            api_workers, db_workers = 1, 1

    async with get_session(http_file_cache) as session:
        feature_client = FeatureServerClient(
            FeatureExpBackoff(conf.exp_backoff_attempts),
            clock, session, cache_cleaner)
        telemetry = GisPipelineTelemetry.create(clock)

        ingestion = GisIngestion.create(
            GisIngestionConfig(
                api_workers=api_workers,
                api_worker_backpressure=db_workers * 4,
                db_mode=conf.db_mode,
                db_workers=db_workers,
                chunk_size=None),
            feature_client,
            db,
            telemetry,
            cache_cleaner)
        sharder_factory = FeaturePaginationSharderFactory(feature_client, telemetry)
        pipeline = GisPipeline(sharder_factory, ingestion)

        await pipeline.start([
            (p, conf.gis_params)
            for p in projections
        ])

async def run_in_console(
    open_file_limit: int,
    db_config: DatabaseConfig,
    config: GisTaskConfig.StageApiData,
) -> None:
    io = IoService.create(open_file_limit)
    db = DatabaseServiceImpl.create(db_config, config.db_workers)
    clock = ClockService()
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    match config.db_mode:
        case 'write':
            await controller.command(SchemaCommand.Drop(ns='nsw_spatial'))
            await controller.command(SchemaCommand.Create(ns='nsw_spatial'))
    await stage_gis_api_data(io, db, clock, config)

if __name__ == '__main__':
    import asyncio
    import argparse
    import logging
    import resource

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--gis-range", type=str)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--db-connections", type=int, default=32)
    parser.add_argument("--db-mode", choices=['write', 'print_head_then_quit', 'skip'], required=True)
    parser.add_argument("--exp-backoff-attempts", type=int, default=8)
    parser.add_argument("--disable-cache", action='store_true', required=False)
    parser.add_argument('--projections', nargs='*', choices=GisTaskConfig.projection_kinds)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'}, {'asyncio'})
    config_logging(worker=None, debug=args.debug, output_name='gis_stage')

    slim, hlim = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(slim * 0.8) - (args.db_connections + http_limits_of(HOST_SEMAPHORE_CONFIG))
    if file_limit < 1:
        raise ValueError(f"file limit of {file_limit} is just too small")


    instance_cfg = INSTANCE_CFG[args.instance]

    try:
        match _parse_date_range(args.gis_range, ClockService()):
            case other if other is not None:
                params = [other]
            case None:
                params = []

        asyncio.run(
            run_in_console(
                open_file_limit=file_limit,
                db_config=instance_cfg.database,
                config=GisTaskConfig.StageApiData(
                    db_workers=args.db_connections,
                    db_mode=args.db_mode,
                    gis_params=params,
                    exp_backoff_attempts=args.exp_backoff_attempts,
                    disable_cache=args.disable_cache,
                    projections=args.projections or GisTaskConfig.projection_kinds,
                ),
            ),
        )
    except asyncio.CancelledError:
        pass
    except ExceptionGroup as eg:
        def is_cancelled_error(exc: BaseException) -> bool:
            return isinstance(exc, asyncio.CancelledError)

        cancelled_errors, remaining_eg = eg.split(is_cancelled_error)

        if remaining_eg:
            for e in remaining_eg.exceptions:
                logging.exception(e)
            raise remaining_eg
        else:
            raise eg
