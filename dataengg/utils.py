import logging
import sys
from pyspark.sql import SparkSession
import glob
import yaml
import os
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType


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
            .appName('BITS_HousingApp')
            .getOrCreate())

        return spark


    @staticmethod
    def read_parquet(spark, file):
        df = spark.read.format('parquet').load(file)
        logging.info(f"[Utilities] Reading parquet file = {file}, count = {df.count()}")
        return df

    @staticmethod
    def read_delta(spark, file):
        df = spark.read.format('delta').load(file)
        logging.info(f"[Utilities] Reading delta file = {file}, count = {df.count()}")
        return df

    @staticmethod
    def write_delta(df, file, in_mode='append', is_merge_schema=False):
        logging.info(f"[Utilities] Writing delta file = {file}, count = {df.count()}")
        df.write.format('delta').option("mergeSchema", is_merge_schema).mode(in_mode).save(file)

    @staticmethod
    def read_json(spark, file):
        df = spark.read.format('json').load(file)
        logging.info(f"[Utilities] Reading delta file = {file}, count = {df.count()}")
        return df

