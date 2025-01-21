from pyarrow.fs import GcsFileSystem, FileSelector, FileInfo


class GCS:
    def __init__(self, public_bucket: bool):
        self.fs = GcsFileSystem(anonymous=public_bucket)

    def ls(self, bucket: str)->FileInfo|list[FileInfo]:
        return self.fs.get_file_info(FileSelector(bucket, recursive=True))
    def open_input_stream(self, file:FileInfo):
        return self.fs.open_input_stream(file.path)
