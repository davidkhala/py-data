import pathlib

from pyarrow import NativeFile
from pyarrow.parquet import ParquetFile


def read(file_path: str | pathlib.Path | NativeFile) -> ParquetFile:
    return ParquetFile(file_path)
