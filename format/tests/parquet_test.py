import unittest

from davidkhala.data.format.parquet import read
from pyarrow.parquet import ColumnSchema, ParquetSchema, ParquetLogicalType

class MyTestCase(unittest.TestCase):
    def test_inspect(self):
        parquet_file = read('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')
        schema: ParquetSchema = parquet_file.schema
        self.assertIsInstance(schema, ParquetSchema)
        for field in schema:
            self.assertIsInstance(field, ColumnSchema)
            print(field)
            self.assertIsInstance(field.logical_type, ParquetLogicalType)
            self.assertEqual(str(field.logical_type), "String")



if __name__ == '__main__':
    unittest.main()
