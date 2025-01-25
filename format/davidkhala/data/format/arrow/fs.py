from pyarrow import RecordBatchFileWriter, RecordBatch, NativeFile, Table
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
    def write(sink: str | NativeFile, table_or_batch: RecordBatch | Table):
        """
        :param sink: Either a file path, or a writable file object [pyarrow.NativeFile].
        :param table_or_batch:
        :return:
        """
        with RecordBatchFileWriter(sink, table_or_batch.schema) as writer:
            writer.write(table_or_batch)
