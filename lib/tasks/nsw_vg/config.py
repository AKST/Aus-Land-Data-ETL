from dataclasses import dataclass, field
from typing import Optional, Literal
from lib.pipeline.nsw_vg.config import *
from lib.pipeline.nsw_vg.land_values import NswVgLvCsvDiscoveryMode
from lib.service.database import DatabaseConfig

class NswVgTaskConfig:
    @dataclass
    class PropDescIngest:
        worker_debug: bool
        workers: int
        sub_workers: int
        db_config: DatabaseConfig
        truncate_earlier: bool

    @dataclass
    class PsiIngest:
        worker_count: int
        worker_config: NswVgPsiWorkerConfig
        parent_config: NswVgPsiSupervisorConfig

    @dataclass
    class Dedup:
        run_from: Optional[int]
        run_till: Optional[int]
        truncate: bool = field(default=False)
        drop_raw: bool = field(default=False)
        drop_dst_schema: bool = field(default=False)

    @dataclass
    class LvIngest:
        truncate_raw_earlier: bool = field(default=False)

    class LandValue:
        @dataclass
        class Child:
            chunk_size: int
            debug: bool
            db_config: DatabaseConfig
            db_conn: int

        @dataclass
        class Main:
            truncate_raw_earlier: bool
            discovery_mode: NswVgLvCsvDiscoveryMode.T
            land_value_source: Literal['byo', 'web']
            child_cfg: 'NswVgTaskConfig.LandValue.Child'
            child_n: int

    @dataclass
    class Ingestion:
        load_raw_land_values: Optional['NswVgTaskConfig.LandValue.Main']
        load_raw_property_sales: Optional['NswVgTaskConfig.PsiIngest']
        deduplicate: Optional['NswVgTaskConfig.Dedup']
        property_descriptions: Optional['NswVgTaskConfig.PropDescIngest']

