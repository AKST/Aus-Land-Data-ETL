from typing import Self, Protocol, AsyncGenerator, Optional

WalkItem = tuple[str, list[str], list[str]]

class TmpFile(Protocol):
    @property
    def name(self: Self) -> str:
        ...

    async def __aenter__(self: Self) -> Self:
        ...

    async def __aexit__(self: Self, *args, **kwargs):
        ...

class IoService(Protocol):
    async def extract_zip(self, zipfile: str, unzip_to: str) -> None:
        ...

    async def mk_dir(self, dir_name: str):
        ...

    def mk_tmp_file(self: Self, mode: Optional[str] = None) -> TmpFile:
        ...

    def grep_dir(self, dir_name: str, pattern: str) -> AsyncGenerator[str, None]:
        ...

    def walk_dir(self, dir_name: str) -> AsyncGenerator[WalkItem, None]:
        ...

    async def f_read(self, file_path: str, encoding: Optional[str] = None) -> str:
        ...

    def f_read_lines(self, file_path: str, encoding: Optional[str] = None) -> AsyncGenerator[str, None]:
        ...

    async def f_read_slice(self, file_path: str, offset: int, length: int) -> bytes:
        ...

    async def f_write_chunks(self,
                             file_path: str,
                             chunks: AsyncGenerator[bytes, None]) -> None:
        ...

    def f_read_chunks(self,
                            file_path: str,
                            chunk_size=1024) -> AsyncGenerator[bytes, None]:
        ...

    async def f_write(self, file_path: str, data: str):
        ...

    async def f_delete(self, file_path: str):
        ...

    async def rmtree(self, file_path: str):
        ...

    async def f_exists(self, file_path: str) -> bool:
        ...

    async def f_size(self, file_path: str) -> int:
        ...

    async def ls_dir(self, dir_name: str) -> list[str]:
        ...

    async def is_dir(self, dir_name: str) -> bool:
        ...

    async def is_file(self, file_name: str) -> bool:
        ...

    async def is_directory_empty(self, dir_name: str) -> bool:
        ...

    async def create_tar_file(self: Self, tar_path: str, files: list[str]):
        ...


