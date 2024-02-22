import logging
from pyspark.sql import SparkSession
import yaml


class Utilities:

    def __init__(self, *args, **kwargs):
        self.arg_dict = kwargs["arg_dict"]
        self.spark = kwargs["spark"]
        self.configs = kwargs["configs"]
        self.logger = self.arg_dict['logger']
        self.jar_path = self.arg_dict['app_base_jar']

    @staticmethod
    def read_yaml(file_path):
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            return data

    @staticmethod
    def read_yaml_new(file_path):
        with open(file_path, 'r') as file:
            data = yaml.load(file, Loader=yaml.BaseLoader)
            return data

    @staticmethod
    def get_spark_session():
        spark = (
            SparkSession
            .builder
            .appName('CreditLendingApp')
            .getOrCreate())

        return spark


    @staticmethod
    def read_parquet(spark, file):
        """
        Purpose: Reads the parquet file and returns the pyspark dataframe
        :param spark:
        :param file: parquet file path
        :return: pyspark dataframe
        """
        df = spark.read.format('parquet').load(file)
        logging.info(f"[Utilities] Reading parquet file = {file}, count = {df.count()}")
        return df

    @staticmethod
    def read_delta(spark, file):
        """
        Purpose: Reads the delta file and returns the pyspark dataframe
        :param spark:
        :param file: delta file path
        :return: pyspark dataframe
        """
        df = spark.read.format('delta').load(file)
        logging.info(f"[Utilities] Reading delta file = {file}, count = {df.count()}")
        return df

    @staticmethod
    def write_delta(df, file, in_mode='append', is_merge_schema=False):
        """
        Purpose: Writes the pyspark dataframe to given path as delta file
        :param spark:
        :param file: file path where the delta file to be written to
        :return: None
        """
        logging.info(f"[Utilities] Writing delta file = {file}, count = {df.count()}")
        df.write.format('delta').option("mergeSchema", is_merge_schema).mode(in_mode).save(file)

    @staticmethod
    def read_json(spark, file):
        """
        Purpose: Reads the json file and returns the pyspark dataframe
        :param spark:
        :param file: json file path
        :return: pyspark dataframe
        """
        df = spark.read.format('json').load(file)
        logging.info(f"[Utilities] Reading delta file = {file}, count = {df.count()}")
        return df
