import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from pyspark.sql import SparkSession
from utils import Utilities
import glob

class TestUtilities(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession.builder.master("local[2]")
                      .appName("UtilsUnittests")
                      .config("spark.jars", "D:\\ABNAMRO\\CreditLendingApp\\dataengg\\jars\\delta-core_2.12-0.7.0.jar")
                      .getOrCreate())
        self.file_path = "test.yaml"
        self.delta_file_path = "test.delta"

    def tearDown(self):
        self.spark.stop()

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
        sourcepath = "file.parquet"
        df = self.spark.read.format('parquet').load(sourcepath)
        self.assertEqual(8, df.count())


    def test_read_delta(self):
        df = Utilities.read_delta(self.spark, self.delta_file_path)
        self.assertEqual(1, df.count())

    @patch("utils.logging")
    def test_write_delta(self, mock_logging):
        df = self.spark.createDataFrame([(1,)], ["col"])
        Utilities.write_delta(df, self.delta_file_path)

        df = Utilities.read_delta(self.spark, self.delta_file_path)
        self.assertEqual(1, df.count())




        with patch.object(df.write, 'format') as mock_format:
            mock_option = mock_format.return_value.option
            mock_mode = mock_option.return_value.mode
            Utilities.write_delta(df, self.delta_file_path)
            mock_mode.assert_called_with('append')
