import asyncio
import heapq
import logging
from typing import Self, Optional

from lib.service.database import *
from .type import PartitionSlice

class WorkPartitioner:
    """
    The goal of this class is break up the work into efficicent
    portions of work. This is done by ensuring inputs do not
    span partitions
    """
    _logger = logging.getLogger(__name__)
    _db_partitions: Optional[int]

    def __init__(self: Self,
                 db: DatabaseService,
                 workers: int,
                 subdivide: int):
        self._db = db
        self._workers = workers
        self._subdivide = subdivide

    async def find_partitions(self: Self) -> dict[int, list[PartitionSlice]]:
        async def _partition_count(c: DbCursorLike, name: str) -> tuple[str, int]:
            await c.execute(f"""
              SELECT count(*)::bigint AS row_count
                FROM {name}
                WHERE legal_description_kind = '> 2004-08-17'
                  AND strata_lot_number IS NULL
            """)
            return name, (await c.fetchone())[0]

        async def _partition_ntiles(c: DbCursorLike, name: str, segs: int) -> list[PartitionSlice]:
            await c.execute(f"""
                SELECT segment, MIN(property_id), MAX(property_id), COUNT(*)
                  FROM (SELECT property_id,
                               NTILE({segs}) OVER (ORDER BY property_id) AS segment
                          FROM {name}
                          WHERE legal_description_kind = '> 2004-08-17'
                            AND strata_lot_number IS NULL) t
                  GROUP BY segment
                  ORDER BY segment
            """)
            return [
                PartitionSlice(name, mn, mx, count)
                for segment, mn, mx, count in await c.fetchall()
            ]

        def distribute(items: list[PartitionSlice], n: int) -> list[list[PartitionSlice]]:
            bins: list[tuple[float, int, list[PartitionSlice]]]  = [(0, i, []) for i in range(n)]
            heapq.heapify(bins)

            for item in sorted(items, key=lambda x: x.count, reverse=True):
                current_sum, bin_index, assigned_list = heapq.heappop(bins)
                assigned_list.append(item)
                heapq.heappush(bins, (current_sum + item.count, bin_index, assigned_list))

            return [b[2] for b in sorted(bins, key=lambda b: b[1])]

        async with self._db.async_connect() as conn, conn.cursor() as c:
            await c.execute("""
              SELECT inhrelid::regclass AS partition_name FROM pg_inherits
                WHERE inhparent = 'nsw_lrs.legal_description'::regclass;
            """)
            name_w_count = await asyncio.gather(*[
                _partition_count(c, row[0]) for row in await c.fetchall()
            ])
            total_count = sum([count for _, count in name_w_count])
            total_segments = self._subdivide * self._workers
            segments_per_partition = [
                (name, count, max(1, round(count / total_count * total_segments)))
                for name, count in name_w_count
                if count > 0
            ]
            slices: list[PartitionSlice] = [s
                for name, count, segments in segments_per_partition
                for s in await _partition_ntiles(c, name, segments)
            ]

        return { i: ps for i, ps in enumerate(distribute(slices, self._workers)) }






