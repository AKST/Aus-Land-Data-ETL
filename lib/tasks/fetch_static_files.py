import asyncio
from dataclasses import dataclass

from lib.pipeline.abs.defaults import (
    ABS_MAIN_STRUCTURES,
    NON_ABS_MAIN_STRUCTURES,
    INDIGENOUS_STRUCTURES,
)
from lib.pipeline.gnaf import GnafPublicationDiscovery
from lib.pipeline.nsw_vg.defaults import THROTTLE_CONFIG
from lib.pipeline.nsw_vg.discovery import WeeklySalePriceDiscovery, AnnualSalePriceDiscovery, LandValueDiscovery
from lib.service.io import IoService, IoServiceImpl
from lib.service.http import AbstractClientSession, ClientSession, CachedClientSession, ExpBackoffClientSession, ThrottledClientSession
from lib.service.http import BackoffConfig, RetryPreference
from lib.service.http.middleware.cache import HttpLocalCache
from lib.service.static_environment import StaticEnvironmentInitialiser
from lib.service.static_environment.defaults import get_static_dirs

BACKOFF_CONFIG = BackoffConfig(
    RetryPreference(allowed=16, pause_other_requests_while_retrying=True),
)

@dataclass
class Environment:
    land_value: LandValueDiscovery
    sale_price_weekly: WeeklySalePriceDiscovery
    sale_price_annual: AnnualSalePriceDiscovery
    gnaf: GnafPublicationDiscovery

async def initialise(io_service: IoService, session: AbstractClientSession) -> Environment:
    initialiser = StaticEnvironmentInitialiser.create(io_service, session)

    land_value_dis = LandValueDiscovery()
    w_sale_price = WeeklySalePriceDiscovery()
    a_sale_price = AnnualSalePriceDiscovery()
    gnaf_dis = GnafPublicationDiscovery.create()

    await asyncio.gather(
        land_value_dis.load_links(session),
        w_sale_price.load_links(session),
        a_sale_price.load_links(session),
        gnaf_dis.load_publication(session),
    )

    for d in get_static_dirs():
        initialiser.queue_directory(d)

    initialiser.queue_target(ABS_MAIN_STRUCTURES.static_file_target)
    initialiser.queue_target(NON_ABS_MAIN_STRUCTURES.static_file_target)
    initialiser.queue_target(INDIGENOUS_STRUCTURES.static_file_target)

    if gnaf_dis.publication:
        initialiser.queue_target(gnaf_dis.publication)

    if land_value_dis.latest:
        initialiser.queue_target(land_value_dis.latest)

    for sale_price_target in w_sale_price.links:
        initialiser.queue_target(sale_price_target)

    for sale_price_target in a_sale_price.links:
        initialiser.queue_target(sale_price_target)

    await initialiser.initalise_environment()

    return Environment(land_value=land_value_dis,
                       sale_price_weekly=w_sale_price,
                       sale_price_annual=a_sale_price,
                       gnaf=gnaf_dis)

def get_session(io_service: IoService, cache_id: str) -> CachedClientSession:
    file_cache = HttpLocalCache.create(io_service, cache_id)
    session_t = ThrottledClientSession.create(THROTTLE_CONFIG)
    session_e = ExpBackoffClientSession.create(BACKOFF_CONFIG, session_t, )
    return CachedClientSession.create(file_cache, io_service, session_e)

if __name__ == '__main__':
    import resource
    import logging

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    async def main():
        io_service = IoServiceImpl.create(file_limit)
        async with get_session(io_service) as session:
            await initialise(io_service, session)

    asyncio.run(main())
