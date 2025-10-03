import unittest
import pandas as pd
import polars as pl
from davidkhala.data.frame.pretty import md_from
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['Hong Kong', 'London', 'New York']
}

from tabulate import tabulate
class TabulateTestCase(unittest.TestCase):
    def test_pandas(self):
        df = pd.DataFrame(data)
        print('psql')
        print(tabulate(df, headers='keys', tablefmt='psql'))
        print('grid')
        print(tabulate(df, headers='keys', tablefmt='grid'))
        print('fancy_grid')
        print(tabulate(df, headers='keys', tablefmt='fancy_grid'))
        print('md_from')
        print(md_from(df))
        print('github (no index)')
        print(tabulate(df.values.tolist(), headers=df.columns, tablefmt='github'))

    def test_polars(self):
        df = pl.DataFrame(data)
        print()
        print(md_from(df))



if __name__ == '__main__':
    unittest.main()
