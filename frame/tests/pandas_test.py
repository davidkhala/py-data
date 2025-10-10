import unittest
import pandas
import duckdb


class AirlineTestCase(unittest.TestCase):
    def test_import(self):
        df_airports = pandas.read_csv("fixtures/airports.csv")
        df_airlines = pandas.read_csv("fixtures/airlines.csv")

    def test_duckDB(self):
        conn = duckdb.connect()

        df_airports = pandas.read_csv("fixtures/airports.csv")
        conn.register("airports", df_airports)
        conn.close()

class SyntaxTestCase(unittest.TestCase):
    def test_upsert(self):
        from davidkhala.data.frame.pandas import upsert
        prim_key ='id'
        df = pandas.DataFrame({
            prim_key: [1, 2],
            'name': ['Alice', 'Bob'],
            'score': [85, 90]
        }).set_index(prim_key)
        new_record = {'name': 'Charlie', 'score': 95, prim_key:2}
        upsert(df, prim_key, new_record)

        self.assertEqual(95, df.at[2, 'score'])




if __name__ == '__main__':
    unittest.main()
