from datetime import datetime
from typing import List, Union, TypeVar, Iterator

from ..discovery import NswVgTarget
from .config import DiscoveryMode, ByoLandValue

_T = TypeVar('_T', bound=Union[NswVgTarget, ByoLandValue])

def select_targets(mode: DiscoveryMode.T, all_targets: List[_T]) -> List[_T]:
    if not all_targets:
        return all_targets

    def sorted_targets():
        return sorted(all_targets, key=lambda k: k.datetime, reverse=True)

    def latest():
        return max(all_targets, key=lambda t: t.datetime)

    def minus_year(n: int, dt: datetime) -> datetime:
        return datetime(dt.year - (1 * n), dt.month, dt.day)

    def each_nth_year(n: int, include_first: bool) -> Iterator[_T]:
        if not all_targets:
            return

        s_targets = sorted_targets()
        previous_yield = s_targets[0]
        previous_item = s_targets[0]
        yield previous_yield
        next_yield = minus_year(n, s_targets[0].datetime)

        for target in s_targets:
            if target.datetime == next_yield:
                yield target
                previous_yield = target
                next_yield = minus_year(n, next_yield)
            elif target.datetime < next_yield and previous_item != previous_yield:
                yield previous_item
                previous_yield = previous_item
                next_yield = minus_year(n, previous_item.datetime)
            elif target.datetime < next_yield:
                yield target
                previous_yield = target
                next_yield = minus_year(n, next_yield)
            previous_item = target

        if include_first and previous_yield != s_targets[-1]:
            yield s_targets[-1]

    match mode:
        case DiscoveryMode.All():
            return all_targets
        case DiscoveryMode.Latest():
            return [latest()]
        case DiscoveryMode.EachYear():
            return list(each_nth_year(1, False))
        case DiscoveryMode.EachNthYear(n, include_first):
            return list(each_nth_year(n, include_first))
        case DiscoveryMode.TheseYears(year_set):
            month = latest().datetime.month
            return [
                t
                for t in sorted_targets()
                if t.datetime.month == month and t.datetime.year in year_set
            ]
        case other:
            raise TypeError(f'unknown mode {mode}')
