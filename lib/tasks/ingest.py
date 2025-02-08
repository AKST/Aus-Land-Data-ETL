from dataclasses import dataclass, field
from datetime import datetime
import logging
import shutil
from typing import Optional, Set

import lib.pipeline.abs.config as abs_config
from lib.pipeline.abs import *
from lib.pipeline.gis import HOST_SEMAPHORE_CONFIG
from lib.pipeline.gnaf import GnafWorkerConfig, GnafConfig, GnafState
from lib.pipeline.nsw_vg.config import *
from lib.pipeline.nsw_vg.land_values import NswVgLvCsvDiscoveryMode
from lib.pipeline.nsw_vg.property_sales.ingestion import NSW_VG_PS_INGESTION_CONFIG
from lib.service.clock import ClockService
from lib.service.docker import DockerService, ImageConfig, ContainerConfig
from lib.service.database import DatabaseServiceImpl, DatabaseConfig
from lib.service.io import IoService, IoServiceImpl
from lib.service.uuid import UuidServiceImpl
from lib.tasks.fetch_static_files import initialise, get_session
from lib.tasks.gis import ingest_gis, GisTaskConfig, http_limits_of
from lib.tasks.ingest_gnaf import ingest_gnaf
from lib.tasks.ingest_abs import ingest_all as ingest_abs
from lib.tasks.nsw_vg.config import NswVgTaskConfig
from lib.tasks.nsw_vg.ingest import ingest_nswvg
from lib.tasks.schema.count import run_count_for_schemas
from lib.tasks.schema.update import update_schema, UpdateSchemaConfig
from lib.tasks.schema.clean_staging import clean_staging_data
from lib.tasks.schema.partition import config_partitions, ConfigPartitionsCfg
from lib.tooling.schema.config import ns_dependency_order
from lib.utility.format import fmt_time_elapsed

@dataclass
class IngestConfig:
    io_file_limit: Optional[int]
    db_config: DatabaseConfig
    db_connections: int
    gnaf_states: Set[GnafState]
    nswvg_psi_publish_min: Optional[int]
    nswvg_lv_depth: NswVgLvCsvDiscoveryMode.T
    docker_volume: str
    docker_image_config: ImageConfig
    docker_container_config: ContainerConfig
    enable_abs: bool
    enable_gis: bool
    enable_gnaf: bool
    enable_clean_staging_data: bool

_logger = logging.getLogger(__name__)

async def ingest_all(config: IngestConfig):
    clock = ClockService()
    uuid = UuidServiceImpl()
    io_service = IoServiceImpl.create(config.io_file_limit)

    t_start = clock.time()

    async with get_session(io_service, 'env') as session:
        environment = await initialise(io_service, session)

    async with DockerService.create() as docker_service:
        await docker_service.reset_volume(config.docker_volume)

        image = docker_service.create_image(config.docker_image_config)
        await image.prepare()

        container = docker_service.create_container(image, config.docker_container_config)
        await container.clean()
        await container.prepare(config.db_config)
        await container.start()

    db_service_config = config.db_config
    db_service = DatabaseServiceImpl.create(db_service_config, config.db_connections)

    await db_service.wait_till_running()
    await db_service.open()

    if environment.gnaf.publication is None:
        raise TypeError('missing gnaf publication')

    await update_schema(
        UpdateSchemaConfig(packages=ns_dependency_order, range=None, apply=True),
        db_service,
        io_service,
    )

    await config_partitions(
        ConfigPartitionsCfg(partitions=16),
        db_service,
        io_service,
    )

    if config.enable_abs:
        await ingest_abs(
            AbsIngestionConfig(
                ingest_sources=[
                    ABS_MAIN_STRUCTURES,
                    NON_ABS_MAIN_STRUCTURES,
                    INDIGENOUS_STRUCTURES,
                ],
                worker_count=4,
                worker_config=AbsWorkerConfig(
                    db_config=db_service_config,
                    db_connections=2,
                    enable_logging=True,
                    enable_logging_debug=False,
                ),
            ),
            db_service,
            io_service,
        )


    await ingest_nswvg(
        environment,
        clock,
        db_service,
        io_service,
        NswVgTaskConfig.Ingestion(
            load_raw_land_values=NswVgTaskConfig.LandValue.Main(
                land_value_source='byo',
                discovery_mode=config.nswvg_lv_depth,
                truncate_raw_earlier=False,
                child_n=8,
                child_cfg=NswVgTaskConfig.LandValue.Child(
                    debug=False,
                    db_conn=16,
                    chunk_size=1000,
                    db_config=db_service_config,
                ),
            ),
            load_raw_property_sales=NswVgTaskConfig.PsiIngest(
                worker_count=8,
                worker_config=NswVgPsiWorkerConfig(
                    db_config=db_service_config,
                    db_pool_size=16,
                    db_batch_size=1000,
                    file_limit=None,
                    ingestion_config=NSW_VG_PS_INGESTION_CONFIG,
                    parser_chunk_size=8 * 2 ** 10,
                    log_config=None,
                ),
                parent_config=NswVgPsiSupervisorConfig(
                    target_root_dir='./_out_zip',
                    publish_min=config.nswvg_psi_publish_min,
                    publish_max=None,
                    download_min=None,
                    download_max=None,
                ),
            ),
            deduplicate=NswVgTaskConfig.Dedup(
                run_from=None,
                run_till=None,
            ),
            property_descriptions=NswVgTaskConfig.PropDescIngest(
                worker_debug=False,
                workers=8,
                sub_workers=8,
                db_config=db_service_config,
            ),
        ),
    )

    if config.enable_gnaf:
        await ingest_gnaf(
            GnafConfig(
                target=environment.gnaf.publication,
                states=config.gnaf_states,
                workers=8,
                worker_config=GnafWorkerConfig(
                    db_config=instance_cfg.database,
                    db_poolsize=8,
                    batch_size=1000,
                ),
            ),
            db_service,
            io_service,
        )

    if config.enable_gis:
        await ingest_gis(
            io_service,
            db_service,
            uuid,
            clock,
            GisTaskConfig.Ingestion(
                deduplication=GisTaskConfig.Deduplication(
                    run_from=None,
                    run_till=None,
                    truncate=False,
                ),
                staging=GisTaskConfig.StageApiData(
                    db_workers=config.db_connections,
                    db_mode='write',
                    gis_params=[],
                    exp_backoff_attempts=8,
                    disable_cache=False,
                    projections=GisTaskConfig.projection_kinds,
                ),
            )
        )

    await run_count_for_schemas(db_service_config, ns_dependency_order)

    if config.enable_clean_staging_data:
        await clean_staging_data(db_service, io_service)
    else:
        _logger.info('staging data not cleaned')

    _logger.info(f"({fmt_time_elapsed(t_start, clock.time(), format="hms")}) Done")
    _logger.debug(repr(config))

if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)

    args = parser.parse_args()

    config_vendor_logging({'sqlglot', 'psycopg.pool'})
    config_logging(worker=None, debug=args.debug)

    DB_CONNECTIONS = 32
    instance_cfg = INSTANCE_CFG[args.instance]
    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8) - (DB_CONNECTIONS + http_limits_of(HOST_SEMAPHORE_CONFIG))

    config = IngestConfig(
        io_file_limit=file_limit,
        db_config=instance_cfg.database,
        db_connections=DB_CONNECTIONS,
        nswvg_psi_publish_min=instance_cfg.nswvg_psi_min_pub_year,
        nswvg_lv_depth=instance_cfg.nswvg_lv_discovery_mode,
        docker_volume=instance_cfg.docker_volume,
        docker_image_config=instance_cfg.docker_image,
        docker_container_config=instance_cfg.docker_container,
        gnaf_states=instance_cfg.gnaf_states,
        enable_abs=instance_cfg.enable_abs,
        enable_gnaf=instance_cfg.enable_gnaf,
        enable_gis=instance_cfg.enable_gis,
        enable_clean_staging_data=instance_cfg.clean_staging_data,
    )

    asyncio.run(ingest_all(config))
