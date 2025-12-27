import unittest

from davidkhala.data.format.arrow.gcp import GCS
from davidkhala.data.format.arrow.parquet import Parquet
from pyarrow import input_stream
from pyarrow.parquet import ColumnSchema, ParquetSchema, ParquetLogicalType


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
        fs = GCS(True)

        uri = "gcp-public-data-landsat/LC08/01/001/003/"
        file_list = fs.ls(uri)

        with fs.open_input_stream(file_list[0]) as f:
            self.assertEqual(f.read(64), b'GROUP = FILE_HEADER\n  LANDSAT_SCENE_ID = "LC80010032013082LGN03"')


class ParquetTestCase(unittest.TestCase):

    def setUp(self):
        self.parquet = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')

    def test_schema(self):
        self.assertIsInstance(self.parquet.schema, ParquetSchema)
        for field in self.parquet.schema:
            self.assertIsInstance(field, ColumnSchema)
            print(field)
            self.assertIsInstance(field.logical_type, ParquetLogicalType)
            self.assertEqual(str(field.logical_type), "String")

    def test_stream(self):
        for record_batch in self.parquet.read_stream():
            print(record_batch)
            print('<<<<')
            for column in record_batch.column_names:
                print(column, record_batch.column(column))
if __name__ == '__main__':
    unittest.main()
