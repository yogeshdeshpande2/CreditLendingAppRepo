import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from silver_to_gold import SilverToGold
from pyspark.sql import functions as psf

class TestSilverToGold(unittest.TestCase):
    @patch('bronze_to_silver.Utilities')
    @patch('bronze_to_silver.psf')
    def test_execute(self, mock_psf, mock_utilities):
        # Mock SparkSession
        mock_spark = MagicMock(spec=SparkSession)

        # Mock input arguments
        arg_dict = {'logger': MagicMock()}
        configs = {
            'SILVER_TO_GOLD': {
                'SOURCES': {
                    'source1': {
                        'NAME': 'source1',
                        'PATH': 'source1_path',
                        'TARGET': 'target_path'
                    }
                },
                'TARGET': "target_path"
            }
        }
        kwargs = {'arg_dict': arg_dict, 'configs': configs, 'spark': mock_spark}
        # Mock read_parquet and write_delta methods
        mock_utilities.read_delta.return_value = mock_spark.createDataFrame([(1, 'data1'), (2, 'data2')], ['column1', 'column2'])
        mock_utilities.write_delta.return_value = None


if __name__ == '__main__':
    unittest.main()
