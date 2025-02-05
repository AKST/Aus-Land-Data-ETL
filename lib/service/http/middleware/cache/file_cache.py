import aiofiles
import asyncio
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
import json
import os
from typing import Any, Dict, List, Optional, Self

from lib.service.clock import ClockService
from lib.service.io import IoService
from lib.service.uuid import UuidService
from lib.utility.concurrent import PartitionLock
from .constants import STATE_INIT, CACHE_VERSION
from .expiry import CacheExpire
from .headers import InstructionHeaders

# Explaination of keys and their values
#
#  1. Key One. The value of this key will be a file path
#     and the value of this key will be a dictorary of
#     file formats. It's more than likely just one file
#     format.
#  2. Key Two. This is going to be a file format, the
#     value assocatied with it will be state.
#  3. Key Three. Each key at this level represents a
#     property name and the value associated with it
#     will be the value of the property in an unparsed
#     state.
#
# Key 1 -------------------------┐
# Key 2 ---------------┐         |
# Key 3 -----┐         |         |
#            ↓         ↓         ↓
State = Dict[str, Dict[str, Dict[str, str]]]

class FileCacher:
    _logger = getLogger(__name__)

    def __init__(self,
                 save_dir: str,
                 config_path: str,
                 rc_factory: 'RequestCacheFactory',
                 io: IoService,
                 uuid: UuidService,
                 clock: ClockService,
                 state: State | None = None):
        self._save_dir = save_dir
        self._config_path = config_path
        self._state = state
        self._io = io
        self._lock = PartitionLock()
        self._uuid = uuid
        self._clock = clock
        self._rc_factory = rc_factory

    def read(self: Self, url: str, fmt: str):
        """
        No need to use partition lock, as this can syncronously
        check if it should just stop.
        """

        if self._state is None:
            raise ValueError('write occured while state was not initialised')

        if url not in self._state:
            return None, False

        if fmt not in self._state[url]:
            return None, False

        now = self._clock.now()
        state = self.parse_state(self._state, url)
        cache_found = state and fmt in state
        cache_expired = cache_found and state[fmt].has_expired(now)

        if cache_found:
            return state, not cache_expired

        return None, False

    async def forget_by_clause(
        self: Self,
        clauses: List[str],
        partition: str,
    ):
        async with self._lock.whole_partition_access(partition):
            if self._state is None:
                return

            url_to_rm: List[str] = []
            locations: List[str] = []
            for url, value in self._state.items():
                if any([c not in url for c in clauses]):
                    continue

                for fmt in self.parse_state(self._state, url).values():
                    locations.append(fmt.location)
                url_to_rm.append(url)

            for url in url_to_rm:
                del self._state[url]
            else:
                return

            self._logger.info("Removed the following from cache: \n" \
                + "\n - " + '\n - '.join(url_to_rm) \
                + "\n\nNow deleteing from cache dir")
            await self._save_cache_state()
            for l in locations:
                await self._io.f_delete(l)

    async def write(
        self: Self,
        url: str,
        meta: InstructionHeaders,
        data: str,
    ) -> Dict[str, 'RequestCache']:
        for attempt in range(0, 2):
            async with self._lock.entry_access(meta.partition):
                if self._state is None:
                    raise ValueError('write occured while state was not initialised')

                fname = f"{meta.request_label}-{self._uuid.get_uuid4_hex()}.{meta.ext}"
                fpath = os.path.join(self._save_dir, fname)
                await self._io.f_write(fpath, data)

                fmts = self._state.get(url, {})

                if meta.format in fmts:
                    cache = self._rc_factory.from_json(fmts[meta.format])
                    await self._io.f_delete(cache.location)

                request_cache = self._rc_factory.create(meta.expiry, fname, self._clock.now())
                fmts[meta.format] = request_cache.to_json()
                self._state[url] = fmts

                await self._save_cache_state()

                if url in self._state:
                    return self.parse_state(self._state, url)
        raise ValueError('cache has entered weird state')

    def parse_state(self: Self, state: Dict[Any, Any], key: str) -> Dict[str, 'RequestCache']:
        return {
            fmt: self._rc_factory.from_json(s)
            for fmt, s in state[key].items()
        }

    async def __aenter__(self: Self):
        try:
            if not await self._io.f_exists(self._config_path):
                await self._io.f_write(self._config_path, STATE_INIT)
            state = json.loads(await self._io.f_read(self._config_path))
            if state['version'] != CACHE_VERSION:
                raise Exception("cache doesn't match version")
            self._state = state['files']
        except Exception as e:
            self._logger.exception(e)
            self._logger.error("Failed to save cache state, possibly corrupted")
            raise
        return self

    async def __aexit__(self: Self, exc_type, exc_value, traceback):
        await self._save_cache_state()
        return False

    async def _save_cache_state(self: Self):
        state = { 'version': CACHE_VERSION, 'files': self._state }
        await self._io.f_write(self._config_path, json.dumps(state, indent=1))

    @staticmethod
    def create(io: IoService,
               uuid: UuidService,
               cache_id: str | None,
               cache_dir: str | None = None,
               state_dir: str | None = None):
        cache_dir = cache_dir or './_out_cache'
        state_dir = state_dir or './_out_state'
        state_path = f"{state_dir}/{cache_id or 'http'}-cache.json"
        clock = ClockService()
        factory = RequestCacheFactory(cache_dir=cache_dir)
        return FileCacher(save_dir=cache_dir,
                          config_path=state_path,
                          rc_factory=factory,
                          io=io,
                          uuid=uuid,
                          clock=ClockService())

_date_format = '%Y-%m-%d %H:%M:%S'

@dataclass
class RequestCache:
    expire: Any
    file_name: str
    age: datetime
    cache_dir: str

    @property
    def location(self: Self):
        return f'{self.cache_dir}/{self.file_name}'

    def has_expired(self: Self, now: datetime):
        return self.expire.has_expired(self.age, now)

    def to_json(self: Self):
        age = self.age.strftime(_date_format)
        return {
            'expire': str(self.expire),
            'location': self.file_name,
            'age': age,
        }

@dataclass
class RequestCacheFactory:
    cache_dir: str

    def create(self: Self, expire, fname, age) -> RequestCache:
        return RequestCache(expire, fname, age, self.cache_dir)

    def from_json(self: Self, json) -> RequestCache:
        return RequestCache(
            CacheExpire.parse_expire(json['expire']),
            json['location'],
            datetime.strptime(json['age'], _date_format),
            cache_dir=self.cache_dir,
        )
