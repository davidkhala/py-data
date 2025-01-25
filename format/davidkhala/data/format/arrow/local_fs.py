from typing import Iterable

from pyarrow import NativeFile, RecordBatch, Table
from pyarrow.fs import LocalFileSystem

from davidkhala.data.format.arrow.fs import FS


class LocalFS(FS):
    fs = LocalFileSystem()

    def write(self, uri, tables_or_batches: Iterable[RecordBatch | Table],
              *, overwrite: bool = False
              ):

        if overwrite:
            stream: NativeFile = self.fs.open_output_stream(uri)
        else:
            stream: NativeFile = self.fs.open_append_stream(uri)
        for table_or_batch in tables_or_batches:
            FS.write(stream, table_or_batch)
        stream.close()
