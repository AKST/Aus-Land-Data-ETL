from dataclasses import dataclass
from typing import Set

from lib.pipeline.gnaf.config import GnafState
from lib.pipeline.nsw_vg.land_values import NswVgLvCsvDiscoveryMode
from lib.service.database.config import *
from lib.service.docker.config import *

@dataclass
class InstanceCfg:
    """
    This exists to configure the tasks for different.
    Read more on instances here:

    https://github.com/AKST/Aus-Land-Data-ETL/wiki/Docs:-Instances,-or-the-%60%E2%80%90%E2%80%90instance%60-flag-for-CLI-commands
    """
    database: DatabaseConfig
    docker_volume: str
    docker_container: ContainerConfig
    docker_image: ImageConfig
    enable_abs: bool
    enable_gis: bool
    enable_gnaf: bool
    gnaf_states: Set[GnafState]
    nswvg_lv_discovery_mode: NswVgLvCsvDiscoveryMode.T
    nswvg_psi_min_pub_year: Optional[int]
    clean_staging_data: bool

