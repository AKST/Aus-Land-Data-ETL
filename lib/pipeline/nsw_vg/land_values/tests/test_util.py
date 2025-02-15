from datetime import datetime
from typing import List

from .._util import select_targets, NswVgTarget
from ..config import DiscoveryMode

def make_targets() -> List[NswVgTarget]:
    letters = [chr(i) for i in range(ord('a'), ord('z'))]
    return [
        NswVgTarget(l, l, l, None, datetime(2024 - y, m, 1))
        for y in range(0, 11, 1)
        for l in letters[y:y+1]
        for m in range(1, 13, 1)
    ]

def test_select_all_targets() -> None:
    targets = make_targets()
    assert select_targets(DiscoveryMode.All(), targets) == targets

def test_select_latest() -> None:
    targets = make_targets()
    latest = NswVgTarget('a', 'a', 'a', None, datetime(2024, 12, 1))
    assert select_targets(DiscoveryMode.Latest(), targets) == [latest]
    assert select_targets(DiscoveryMode.Latest(), list(reversed(targets))) == [latest]

def test_select_these_years() -> None:
    mode = DiscoveryMode.TheseYears({2024, 2022, 2020, 2017})
    assert select_targets(mode, make_targets()) == [
        NswVgTarget('a', 'a', 'a', None, datetime(2024, 12, 1)),
        NswVgTarget('c', 'c', 'c', None, datetime(2022, 12, 1)),
        NswVgTarget('e', 'e', 'e', None, datetime(2020, 12, 1)),
        NswVgTarget('h', 'h', 'h', None, datetime(2017, 12, 1)),
    ]

def test_select_each_year() -> None:
    targets = make_targets()
    assert select_targets(DiscoveryMode.EachYear(), targets) == [
        NswVgTarget('a', 'a', 'a', None, datetime(2024, 12, 1)),
        NswVgTarget('b', 'b', 'b', None, datetime(2023, 12, 1)),
        NswVgTarget('c', 'c', 'c', None, datetime(2022, 12, 1)),
        NswVgTarget('d', 'd', 'd', None, datetime(2021, 12, 1)),
        NswVgTarget('e', 'e', 'e', None, datetime(2020, 12, 1)),
        NswVgTarget('f', 'f', 'f', None, datetime(2019, 12, 1)),
        NswVgTarget('g', 'g', 'g', None, datetime(2018, 12, 1)),
        NswVgTarget('h', 'h', 'h', None, datetime(2017, 12, 1)),
        NswVgTarget('i', 'i', 'i', None, datetime(2016, 12, 1)),
        NswVgTarget('j', 'j', 'j', None, datetime(2015, 12, 1)),
        NswVgTarget('k', 'k', 'k', None, datetime(2014, 12, 1)),
    ]

def test_select_each_nth_year_ordering() -> None:
    for targets in [
        select_targets(DiscoveryMode.EachNthYear(2, False), make_targets()),
        select_targets(DiscoveryMode.EachNthYear(2, False), list(reversed(make_targets()))),
    ]:
        assert targets == [
            NswVgTarget('a', 'a', 'a', None, datetime(2024, 12, 1)),
            NswVgTarget('c', 'c', 'c', None, datetime(2022, 12, 1)),
            NswVgTarget('e', 'e', 'e', None, datetime(2020, 12, 1)),
            NswVgTarget('g', 'g', 'g', None, datetime(2018, 12, 1)),
            NswVgTarget('i', 'i', 'i', None, datetime(2016, 12, 1)),
            NswVgTarget('k', 'k', 'k', None, datetime(2014, 12, 1)),
        ]

def test_select_each_nth_year_plus_first() -> None:
    mode = DiscoveryMode.EachNthYear(4, include_first=True)
    assert select_targets(mode, make_targets()) == [
        NswVgTarget('a', 'a', 'a', None, datetime(2024, 12, 1)),
        NswVgTarget('e', 'e', 'e', None, datetime(2020, 12, 1)),
        NswVgTarget('i', 'i', 'i', None, datetime(2016, 12, 1)),
        NswVgTarget('k', 'k', 'k', None, datetime(2014, 1, 1)),
    ]


def test_range_adjust_if_one_missing() -> None:
    mode = DiscoveryMode.EachNthYear(3, include_first=True)
    targets = [t for t in make_targets() if t.datetime != datetime(2021, 12, 1)]
    assert select_targets(mode, targets) == [
        NswVgTarget('a', 'a', 'a', None, datetime(2024, 12, 1)),
        NswVgTarget('c', 'c', 'c', None, datetime(2022, 1, 1)),
        NswVgTarget('f', 'f', 'f', None, datetime(2019, 1, 1)),
        NswVgTarget('i', 'i', 'i', None, datetime(2016, 1, 1)),
        NswVgTarget('k', 'k', 'k', None, datetime(2014, 1, 1)),
    ]

