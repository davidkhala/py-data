import unittest

from davidkhala.data.format.parquet import Parquet
from pyarrow.parquet import ColumnSchema, ParquetSchema, ParquetLogicalType


class MyTestCase(unittest.TestCase):
    parquet = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')

    def test_schema(self):
        self.assertIsInstance(self.parquet.schema, ParquetSchema)
        for field in self.parquet.schema:
            self.assertIsInstance(field, ColumnSchema)
            print(field)
            self.assertIsInstance(field.logical_type, ParquetLogicalType)
            self.assertEqual(str(field.logical_type), "String")
    def test_stream(self):
        for micro_batch in self.parquet.read_stream():
            print(micro_batch)
            print('<<<<')
            for column in micro_batch.column_names:
                print(column, micro_batch.column(column))

if __name__ == '__main__':
    unittest.main()
