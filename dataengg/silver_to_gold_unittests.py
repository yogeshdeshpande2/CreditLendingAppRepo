import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from silver_to_gold import SilverToGold
from pyspark.sql import functions as psf
from pandas.testing import assert_frame_equal

class TestSilverToGold(unittest.TestCase):
    spark = (
        SparkSession
        .builder
        .appName('CreditLendingAppUnittests')
        .getOrCreate())

    def test_calc_collateral_status_detail(self):
        df_inp = self.spark.createDataFrame([(2, 10.0)], ["units", "price"])
        df_actual = SilverToGold.calc_collateral_status_detail(self, df_inp)

        df_expected = self.spark.createDataFrame([(2, 10.0, 20.0)], ["units", "price", "stocks_value"])

        df_actual_pd = df_actual.toPandas()
        df_expected_pd = df_expected.toPandas()

        # Assert if DataFrames are equal
        assert_frame_equal(df_actual_pd, df_expected_pd)

    def test_calc_collateral_status(self):
        df_inp = self.spark.createDataFrame([(1, 'A', 'B', '2024-02-21', 'GOOG', 1000.00),
                                             (1, 'A', 'B', '2024-02-21', 'MSFT', 2000.00)],
                                            ['client_id', 'first_name', 'last_name', 'Date', 'stock', 'stocks_value'])
        df_actual = SilverToGold.calc_collateral_status(self, df_inp)

        df_expected = self.spark.createDataFrame([(1, 'A', 'B', '2024-02-21', 3000.00)],
                                                 ['client_id', 'first_name', 'last_name', 'Date', 'stocks_value'])

        df_actual_pd = df_actual.toPandas()
        df_expected_pd = df_expected.toPandas()

        # Assert if DataFrames are equal
        assert_frame_equal(df_actual_pd, df_expected_pd)

if __name__ == '__main__':
    unittest.main()
