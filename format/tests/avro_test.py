import unittest
from davidkhala.data.format.avro import read, is_avro


class AvroTestCase(unittest.TestCase):
    def test_read(self):
        _path = 'fixtures/gcp-data-davidkhala.dbt_davidkhala.country_codes.avro'
        self.assertTrue(is_avro(_path))
        with open(_path, 'rb') as file:
            for record in read(file):
                print(record)


if __name__ == '__main__':
    unittest.main()
