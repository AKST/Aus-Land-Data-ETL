from dataclasses import dataclass, field

import logging
import time

from typing import Literal, Optional, Set, Self
from ..format.time import time_elapsed

class RuntimeFormatter(logging.Formatter):
    start_time: float
    last_time: float
    show_delta: bool

    def __init__(self: Self, show_delta: bool, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.start_time=time.time()
        self.last_time=time.time()

    def format(self: Self, record):
        current_time = time.time()
        record.runtime = time_elapsed(self.start_time, current_time, 'hms')
        record.delta = f"{current_time - self.last_time:.2f}s"
        self.last_time = current_time
        return super().format(record)

def config_logging(
        worker: Optional[int],
        debug: bool = False,
        output_name: Optional[str] = None,
        runtime_fmt: Literal['none', 'elapsed', 'elapsed+delta'] = 'none',
        time_fmt: Literal['s', 'ms'] = 's',
):
    import os
    import time

    os.makedirs('_out_log', exist_ok=True)

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if output_name is not None:
        handlers.append(logging.FileHandler(f'_out_log/{output_name}-{int(time.time())}.log'))

    show_delta = False
    format_str_t = '%(asctime)s.%(msecs)03d' if time_fmt == 'ms' else '%(asctime)s'
    format_str = f'[{format_str_t}][%(levelname)s][%(name)s]'

    match runtime_fmt:
        case 'none':
            ...
        case 'elapsed':
            format_str = f'{format_str}[%(runtime)s]'
        case 'elapsed+delta':
            format_str = f'{format_str}[%(runtime)s][delta %(delta)s]'
            show_delta = True

    format_str = f'[{worker}]{format_str}' if worker is not None else format_str
    format_str = f'{format_str} %(message)s'
    formatter = RuntimeFormatter(
        show_delta=show_delta,
        fmt=format_str,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    for handler in handlers:
        handler.setFormatter(formatter)

    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, handlers=handlers)

_Vendor = Literal['sqlglot', 'psycopg.pool', 'asyncio']

def config_vendor_logging(
        set_to_error: Set[_Vendor],
        disable: Optional[Set[_Vendor]] = None):
    disable = disable or set()

    for vendor in set_to_error:
        logging.getLogger(vendor).setLevel(logging.ERROR)

    for vendor in disable:
        logger = logging.getLogger(vendor)
        logger.disabled = True
