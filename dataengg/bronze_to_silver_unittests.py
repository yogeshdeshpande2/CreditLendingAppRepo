import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from bronze_to_silver import BronzeToSilver

class TestBronzeToSilver(unittest.TestCase):

    @patch('bronze_to_silver.Utilities')
    @patch('bronze_to_silver.psf')
    def test_execute(self, mock_psf, mock_utilities):
        # Mock SparkSession
        mock_spark = MagicMock(spec=SparkSession)

        # Mock input arguments
        arg_dict = {'logger': MagicMock()}
        configs = {
            'BRONZE_TO_SILVER': {
                'SOURCES': {
                    'source1': {
                        'NAME': 'source1',
                        'SOURCE': 'source1_path',
                        'TARGET': 'target_path'
                    }
                },
                'DATAQUALITY': {
                    'RULES': [
                        {
                            'RULE': {
                                'ruleno': 1,
                                'rulename': 'NOT_NULL',
                                'tablename': 'source1',
                                'columnname': 'column1',
                                'values': []
                            }
                        }
                    ]
                }
            }
        }
        kwargs = {'arg_dict': arg_dict, 'configs': configs, 'spark': mock_spark}

        # Mock read_parquet and write_delta methods
        mock_utilities.read_parquet.return_value = mock_spark.createDataFrame([(1, 'data1'), (2, 'data2')], ['column1', 'column2'])
        mock_utilities.write_delta.return_value = None

        # Mock lit method of psf
        mock_psf.lit.side_effect = lambda val: val

        # Initialize BronzeToSilver object
        bronze_to_silver = BronzeToSilver(**kwargs)

        # Execute the method
        bronze_to_silver.execute()

        # Assertions
        mock_utilities.read_parquet.assert_called_once_with(spark=mock_spark, file='source1_path')
        mock_psf.when.assert_called()
        mock_psf.col.assert_called()
        mock_utilities.write_delta.assert_called_once()

if __name__ == '__main__':
    unittest.main()
