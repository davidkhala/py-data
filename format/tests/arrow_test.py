import os
import unittest

from pyarrow import input_stream

from davidkhala.data.format.arrow.gcp import GCS
from davidkhala.data.format.arrow.local_fs import LocalFS
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
        fs = GCS(True)

        uri = "gcp-public-data-landsat/LC08/01/001/003/"
        file_list = fs.ls(uri)

        with fs.open_input_stream(file_list[0]) as f:
            self.assertEqual(f.read(64), b'GROUP = FILE_HEADER\n  LANDSAT_SCENE_ID = "LC80010032013082LGN03"')

    def test_parquet2arrow(self):
        parquet = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')
        arrow_batch_path = 'artifacts/gcp-data-davidkhala.dbt_davidkhala.country_codes.batch.arrow'

        fs = LocalFS()
        fs.overwrite = True
        fs.write_batch(arrow_batch_path, parquet.read_batch())
        arrow_stream_path = 'artifacts/gcp-data-davidkhala.dbt_davidkhala.country_codes.stream.arrow'
        fs.write_stream(arrow_stream_path, parquet.read_stream())


class GCSTests(unittest.TestCase):
    """
    tests on private bucket
    """
    bucket = "davidkhala-data"

    def test_ADC(self):
        """
        based on GCP ADC
        """
        for file in self.gcs.ls(self.bucket):
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

    def test_service_account(self):
        GCSTests.gcs.fget(self)

    def test_parquet2arrow(self):
        parquet = Parquet('fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.parquet')
        stream_uri = "gs://davidkhala-data/gcp-data-davidkhala.dbt_davidkhala.country_codes.stream.arrow"
        self.gcs.write_stream(stream_uri, parquet.read_stream())
        batch_uri = "gs://davidkhala-data/gcp-data-davidkhala.dbt_davidkhala.country_codes.batch.arrow"
        self.gcs.write_batch(batch_uri, parquet.read_batch())


if __name__ == '__main__':
    unittest.main()
