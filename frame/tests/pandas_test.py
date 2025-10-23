import unittest
import pandas
import duckdb


class AirlineTestCase(unittest.TestCase):
    def test_import(self):
        pandas.read_csv("fixtures/airports.csv")
        pandas.read_csv("fixtures/airlines.csv")

    def test_duckDB(self):
        conn = duckdb.connect()

        df_airports = pandas.read_csv("fixtures/airports.csv")
        conn.register("airports", df_airports)
        conn.close()


from davidkhala.data.frame.pandas import upsert


class SyntaxTestCase(unittest.TestCase):
    single_index_df = pandas.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'score': [85, 90]
    }).set_index('id')

    def test_upsert(self):
        df = self.single_index_df
        prim_key = df.index.name
        new_record = {'name': 'Charlie', 'score': 95, prim_key: 2}
        df = upsert(df, prim_key, record=new_record)
        self.assertEqual(95, df.at[2, 'score'])
        self.assertEqual(2, len(df.columns))

    def test_upsert_empty(self):
        df = pandas.DataFrame()
        prim_key = 'id'
        new_record = {'name': 'Charlie', 'score': 95, prim_key: 2}
        df = upsert(df, prim_key, record=new_record)
        self.assertEqual(3, len(df.columns))
        self.assertEqual(1, len(df))

        df = pandas.DataFrame(columns=[prim_key]).set_index(prim_key)
        df = upsert(df, prim_key, record=new_record)
        self.assertEqual(2, len(df.columns))
        self.assertEqual(1, len(df))

    def test_upsert2(self):
        df = pandas.DataFrame([
            {'School': 'Oxford', 'Country': 'UK', 'Students': 1000},
            {'School': 'Harvard', 'Country': 'US', 'Students': 1500}
        ]).set_index(['School', 'Country'])
        new_students = 2000
        record = {'School': 'Oxford', 'Country': 'UK', 'Students': new_students}
        df = upsert(df, 'School', 'Country', record=record)
        self.assertEqual(new_students, df.loc[('Oxford', 'UK')].Students)
        print(df)

    def test_upsert3(self):
        df = pandas.DataFrame([
            {'School': 'Oxford', 'Country': 'UK', 'Students': 1000},
            {'School': 'Harvard', 'Country': 'US', 'Students': 1500}
        ])
        new_students = 2000
        record = {'School': 'Oxford', 'Country': 'UK', 'Students': new_students}
        df = upsert(df, 'School', 'Country', record=record)
        self.assertEqual(new_students, df[(df['School'] == 'Oxford') & (df['Country'] == 'UK')].iloc[0].Students)


if __name__ == '__main__':
    unittest.main()
