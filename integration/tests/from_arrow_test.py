import os
import unittest

from davidkhala.data.format.avro import read, is_avro, write
from davidkhala.data.format.arrow.parquet import Parquet
from pyarrow import table, array, int32, string, list_, float32
from davidkhala.data.format.arrow.local_fs import LocalFS

from davidkhala.data.integration.source.arrow import ToAvro
from davidkhala.data.format.arrow.gcp import GCS

class ParquetToAvroTestCase(unittest.TestCase):
    sink = 'fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.avro'
    source = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')

    def test_country_codes(self):

        t = ToAvro(self.source.read_batch())
        with open(self.sink, 'wb') as output_stream:
            write(output_stream, t.schema, t.records)
        # validate
        self.assertTrue(is_avro(self.sink))
        with open(self.sink, 'rb') as file:
            for record in read(file):
                print(record)
    def test_types(self):
        sample_table = table({
            "id": array([1, 2, 3], type=int32()),
            "name": array(["Alice", "Bob", "Charlie"], type=string()),
            "scores": array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], type=list_(float32()))
        })
        t = ToAvro(sample_table)

        with open("artifacts/dummy.avro", "wb") as out:
            write(out, t.schema, t.records)

class ParquetToArrowTestCase(unittest.TestCase):
    def test_localfs(self):
        parquet = ParquetToAvroTestCase.source

        arrow_batch_path = 'artifacts/gcp-data-davidkhala.dbt_davidkhala.country_codes.batch.arrow'
        arrow_stream_path = 'artifacts/gcp-data-davidkhala.dbt_davidkhala.country_codes.stream.arrow'

        fs = LocalFS()
        fs.overwrite = True
        fs.write_batch(arrow_batch_path, parquet.read_batch())

        fs.write_stream(arrow_stream_path, parquet.read_stream())

    def test_authN(self):
        """
        test on private bucket
        """
        for file in self.gcs.ls("davidkhala-data"):
            print(file.path)

    @property
    def gcs(self):
        private_key = os.environ.get("PRIVATE_KEY")
        if private_key:
            return GCS.from_service_account({
                'client_email': 'data-integration@gcp-data-davidkhala.iam.gserviceaccount.com',
                'private_key': private_key,
            })
        else:
            return GCS()

    def test_parquet2arrow(self):
        parquet = ParquetToAvroTestCase.source

        stream_uri = "gs://davidkhala-data/gcp-data-davidkhala.dbt_davidkhala.country_codes.stream.arrow"
        self.gcs.write_stream(stream_uri, parquet.read_stream())
        batch_uri = "gs://davidkhala-data/gcp-data-davidkhala.dbt_davidkhala.country_codes.batch.arrow"
        self.gcs.write_batch(batch_uri, parquet.read_batch())

if __name__ == '__main__':
    unittest.main()
