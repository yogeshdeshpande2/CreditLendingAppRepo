import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from pyspark.sql import SparkSession
from dataengg.utils import Utilities
import shutil


class TestUtilities(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession.builder.master("local[2]")
                      .appName("UtilsUnittests")
                      .config("spark.jars", "D:\\ABNAMRO\\CreditLendingApp\\dataengg\\jars\\delta-core_2.12-0.7.0.jar")
                      .getOrCreate())

        self.file_path = "./unittests/test.yaml"
        self.delta_file_path = r'D:\ABNAMRO\CreditLendingApp\dataengg\unittests\test.delta'
        self.parquet_file_path = r'D:\ABNAMRO\CreditLendingApp\dataengg\unittests\file.parquet'
        self.json_file_path = r'D:\ABNAMRO\CreditLendingApp\dataengg\unittests\file1.json'


    def test_read_yaml(self):
        data = {'key': 'value'}
        with patch("builtins.open", unittest.mock.mock_open(read_data="key: value\n")):
            result = Utilities.read_yaml(self.file_path)
            self.assertEqual(data, result)

    def test_read_yaml_new(self):
        data = {'key': 'value'}
        with patch("builtins.open", unittest.mock.mock_open(read_data="key: value\n")):
            result = Utilities.read_yaml_new(self.file_path)
            self.assertEqual(data, result)

    def test_read_parquet(self):
        df = self.spark.createDataFrame([(1,)], ["col"])
        df.write.format('parquet').mode('overwrite').save(self.parquet_file_path)

        df = Utilities.read_parquet(self.spark, self.parquet_file_path)
        self.assertEqual(1, df.count())

    def test_read_json(self):
        df = self.spark.createDataFrame([(1,)], ["col"])
        df.write.format('json').mode('overwrite').save(self.json_file_path)

        df = Utilities.read_json(self.spark, self.json_file_path)
        self.assertEqual(1, df.count())

    def test_read_delta(self):
        df = Utilities.read_delta(self.spark, self.delta_file_path)
        self.assertEqual(1, df.count())

    def test_write_delta(self):
        df = self.spark.createDataFrame([(1,)], ["col"])
        Utilities.write_delta(df, self.delta_file_path, in_mode='overwrite')

        df = Utilities.read_delta(self.spark, self.delta_file_path)
        self.assertEqual(1, df.count())
