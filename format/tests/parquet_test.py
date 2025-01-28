import unittest

from davidkhala.data.format.parquet import Parquet
from pyarrow.parquet import ColumnSchema, ParquetSchema, ParquetLogicalType

parquet_path = 'fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet'


class MyTestCase(unittest.TestCase):
    parquet = Parquet(parquet_path)

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

    def test_memory_map(self):
        from pyarrow.parquet import read_table
        from pyarrow import total_allocated_bytes
        read_table(parquet_path, memory_map=False)
        self.assertEqual(total_allocated_bytes(), 256)
        read_table(parquet_path, memory_map=True)
        print(total_allocated_bytes())


if __name__ == '__main__':
    unittest.main()
