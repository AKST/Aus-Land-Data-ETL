import asyncio
import psutil
from typing import AsyncIterator, Iterator, Self, Literal, cast

ProcColumn = Literal['pid', 'environ', 'status']

class AsyncProcessIter(AsyncIterator[psutil.Process]):
    def __init__(self, iterator: Iterator[psutil.Process]):
        self._iterator = iterator

    @staticmethod
    async def create(columns: list[ProcColumn]) -> 'AsyncProcessIter':
        iterator = psutil.process_iter(cast(list[str], columns))
        return AsyncProcessIter(iterator)

    async def __anext__(self) -> psutil.Process:
        def f():
            try:
                return next(self._iterator)
            except StopIteration:
                return None
        it = await asyncio.to_thread(f)
        if it is None:
            raise StopAsyncIteration
        return it


    def __aiter__(self: Self) -> "AsyncProcessIter":
        return self

