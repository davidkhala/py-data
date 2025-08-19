import unittest
from polars import DataFrame, read_csv, lit, col
import datetime
from fsspec import filesystem


class GettingStarted(unittest.TestCase):
    def test_rw(self):
        df = DataFrame(
            {
                "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
                "birthdate": [
                    datetime.date(1997, 1, 10),
                    datetime.date(1985, 2, 15),
                    datetime.date(1983, 3, 22),
                    datetime.date(1981, 4, 30),
                ],
                "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
                "height": [1.56, 1.77, 1.65, 1.75],  # (m)
            }
        )
        file_path = "artifacts/output.csv"
        filesystem('file').touch(file_path)

        df.write_csv(file_path)
        df_csv = read_csv(file_path, try_parse_dates=True)
        print(df_csv)

    def test_readme(self):
        df = DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            }
        )

        # embarrassingly parallel execution & very expressive query language
        r=df.sort("fruits").select(
            "fruits",
            "cars",
            lit("fruits").alias("literal_string_fruits"),
            col("B").filter(col("cars") == "beetle").sum(),
            col("A").filter(col("B") > 2).sum().over("cars").alias("sum_A_by_cars"),
            col("A").sum().over("fruits").alias("sum_A_by_fruits"),
            col("A").reverse().over("fruits").alias("rev_A_by_fruits"),
            col("A").sort_by("B").over("fruits").alias("sort_A_by_B_by_fruits"),
        )
        print(r)


if __name__ == '__main__':
    unittest.main()
