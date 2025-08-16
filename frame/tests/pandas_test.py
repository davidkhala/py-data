import unittest
import pandas
import duckdb


class AirlineTestCase(unittest.TestCase):
    def test_import(self):
        df_airports = pandas.read_csv("fixtures/airports.csv")
        df_airlines = pandas.read_csv("fixtures/airlines.csv")

    def test_duckDB(self):
        conn = duckdb.connect()

        df_airports = pandas.read_csv("frame/tests/fixtures/airports.csv")
        conn.register("airports", df_airports)
        conn.close()



if __name__ == '__main__':
    unittest.main()
