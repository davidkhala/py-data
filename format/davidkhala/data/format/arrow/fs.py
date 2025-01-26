from typing import Iterable

from pyarrow import RecordBatchFileWriter, RecordBatch, NativeFile, Table, RecordBatchStreamWriter, Schema
from pyarrow.fs import FileSystem, FileInfo, FileSelector


class FS:
    """
    Abstract FileSystem
    """
    fs: FileSystem

    def open_input_stream(self, file: FileInfo):
        return self.fs.open_input_stream(file.path)

    def ls(self, base_dir: str) -> FileInfo | list[FileInfo]:
        return self.fs.get_file_info(FileSelector(base_dir, recursive=True))

    @staticmethod
    def write_batch(sink: str | NativeFile, table_or_batch: RecordBatch | Table):
        """
        :param sink: Either a file path, or a writable file object [pyarrow.NativeFile].
        :param table_or_batch:
        :return:
        """
        with RecordBatchFileWriter(sink, table_or_batch.schema) as writer:
            writer.write(table_or_batch)

    @staticmethod
    def write_stream(sink: str | NativeFile, tables_or_batches: Iterable[RecordBatch | Table]):
        writer:RecordBatchStreamWriter|None = None
        for table_or_batch in tables_or_batches:
            if not writer:
                writer = RecordBatchStreamWriter(sink, table_or_batch.schema)
            writer.write(table_or_batch)
        if writer:
            writer.close()