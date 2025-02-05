from functools import reduce
import json
from typing import Any, Dict, List, Set, Tuple
from lib.service.io import IoService, IoServiceImpl

_CACHE_STATE = './_out_state/http-cache.json'
_CACHE_DIR = '_out_cache'

async def fix_cache(io: IoService) -> None:
    """
    For a number reasons it's possible for the cache to
    become kind of broken. Such reasons include:

    1. Running multiple tasks at a time that are writing
       and reading to the cache, and either of them
       recording a new asset (resulting it being orphaned).

    2. The logic for cache could be buggy in some cases.
    """
    cache_states: Dict[str, Dict[str, Any]] = {
        f: json.loads(await io.f_read(f))
        async for f in io.grep_dir('./_out_state', '*-cache.json')
    }

    files_referenced_in_cache: List[str] = reduce(lambda acc, it: [*acc, *[
        f'{_CACHE_DIR}/{fmt['location']}'
        for formats in it['files'].values()
        for fmt in formats.values()
    ]], cache_states.values(), [])

    files_in_fs = { f async for f in io.grep_dir(_CACHE_DIR, '*') }

    print('Checking if FILES on DISC are MISSING from CACHE STATE')
    for file in files_in_fs:
        if file not in files_referenced_in_cache:
            print(f"Missing from cache, {file}")
            await io.f_delete(file)

    print('CHECKING IF CACHED FILES ARE ON DISC')
    for f, state in cache_states.items():
        remove_from_state: List[Tuple[str, str]] = []

        for url, formats in state['files'].items():
            for fmt_name, fmt in formats.items():
                path = f'{_CACHE_DIR}/{fmt['location']}'
                if path not in files_in_fs:
                    remove_from_state.append((url, fmt_name))

        for url, fmt in remove_from_state:
            print(f'removing {fmt} from {url}')
            del state['files'][url][fmt]
            if not state['files'][url]:
                del state['files'][url]

        print(f"Saving {f}")
        await io.f_write(f, json.dumps(state, indent=1))


if __name__ == '__main__':
    import asyncio
    import resource

    file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    file_limit = int(file_limit * 0.8)

    io = IoServiceImpl.create(file_limit)
    asyncio.run(fix_cache(io))

