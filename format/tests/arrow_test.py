import unittest
from pyarrow import input_stream

from davidkhala.data.format.arrow.fs import FS
from davidkhala.data.format.arrow.gcp import GCS
from davidkhala.data.format.parquet import Parquet


class Samples(unittest.TestCase):
    def test_input_stream(self):
        """
        https://arrow.apache.org/docs/python/generated/pyarrow.BufferReader.html#pyarrow-bufferreader
        """
        #  Create an Arrow input stream and inspect it:
        data = b'reader data'
        buf = memoryview(data)
        with input_stream(buf) as stream:
            self.assertEqual(stream.size(), 11)
            self.assertEqual(stream.read(6), b'reader')
            stream.seek(7)
            self.assertEqual(stream.read(4), b'data')

    def test_GCS(self):
        """
        https://arrow.apache.org/docs/python/filesystems.html#google-cloud-storage-file-system
        """
        fs = GCS(True)

        uri = "gcp-public-data-landsat/LC08/01/001/003/"
        file_list = fs.ls(uri)

        with fs.open_input_stream(file_list[0]) as f:
            self.assertEqual(f.read(64), b'GROUP = FILE_HEADER\n  LANDSAT_SCENE_ID = "LC80010032013082LGN03"')


    def test_parquet2arrow(self):
        parquet = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')
        arrow_path = 'fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.arrow'
        for record_batch in parquet.read_stream():
            FS.write(arrow_path,record_batch)

if __name__ == '__main__':
    unittest.main()
