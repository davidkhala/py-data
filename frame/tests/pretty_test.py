import unittest
import pandas as pd
from davidkhala.data.frame.pretty import md_from
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['Hong Kong', 'London', 'New York']
}
df = pd.DataFrame(data)
class TabulateTestCase(unittest.TestCase):
    def test_tabulate(self):
        from tabulate import tabulate
        print('psql')
        print(tabulate(df, headers='keys', tablefmt='psql'))
        print('grid')
        print(tabulate(df, headers='keys', tablefmt='grid'))
        print('fancy_grid')
        print(tabulate(df, headers='keys', tablefmt='fancy_grid'))
        print('github')
        print(md_from(df))



if __name__ == '__main__':
    unittest.main()
