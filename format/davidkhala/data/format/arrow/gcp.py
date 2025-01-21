from pyarrow.fs import GcsFileSystem, FileInfo

from davidkhala.data.format.arrow.fs import FS


class GCS(FS):
    def __init__(self, public_bucket: bool):
        self.fs = GcsFileSystem(anonymous=public_bucket)

    def ls(self, bucket: str) -> FileInfo | list[FileInfo]:
        return super().ls(bucket)
