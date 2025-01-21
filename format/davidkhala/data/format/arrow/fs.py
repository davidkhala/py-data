from pyarrow import RecordBatch, RecordBatchFileWriter
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
    def write(file_path, record_batch: RecordBatch):
        with RecordBatchFileWriter(file_path, record_batch.schema) as writer:
            writer.write(record_batch)
