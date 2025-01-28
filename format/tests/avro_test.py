import unittest

from pyarrow import table, array, int32, string, list_, float32

from davidkhala.data.format.avro import read, is_avro, write
from davidkhala.data.format.parquet import Parquet
from davidkhala.data.format.transform import Arrow2Avro


class AvroTestCase(unittest.TestCase):
    _path = 'fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.avro'
    def setUp(self):
        parquet = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')

        t = Arrow2Avro(parquet.read_batch())
        with open(self._path,'wb' ) as output_stream:
            write(output_stream, t.schema, t.records)


    def test_transform(self):
        sample_table = table({
            "id": array([1, 2, 3], type=int32()),
            "name": array(["Alice", "Bob", "Charlie"], type=string()),
            "scores": array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], type=list_(float32()))
        })
        t= Arrow2Avro(sample_table)

        with open("artifacts/dummy.avro", "wb") as out:
            write(out, t.schema, t.records)

    def test_read(self):
        self.assertTrue(is_avro(self._path))
        with open(self._path, 'rb') as file:
            for record in read(file):
                print(record)


if __name__ == '__main__':
    unittest.main()
