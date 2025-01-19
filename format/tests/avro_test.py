import unittest
from davidkhala.data.format.avro import read

class AvroTestCase(unittest.TestCase):
    def test_read(self):
        read('tests/fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.avro')


if __name__ == '__main__':
    unittest.main()
