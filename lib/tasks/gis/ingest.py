from lib.service.clock import ClockService
from lib.service.database import DatabaseService
from lib.service.io import IoService
from lib.service.uuid import UuidService

from .config import GisTaskConfig
from .stage_api_data import stage_gis_api_data
from .ingest_deduplicate import ingest_deduplication

async def ingest(
    io: IoService,
    db: DatabaseService,
    uuid: UuidService,
    clock: ClockService,
    config: GisTaskConfig.Ingestion,
):
    if config.staging:
        await stage_gis_api_data(
            io,
            db,
            uuid,
            clock,
            config.staging,
        )

    if config.deduplication:
        await ingest_deduplication(
            db,
            io,
            uuid,
            clock,
            config.deduplication,
        )
