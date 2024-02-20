import logging
import sys
from pyspark.sql import SparkSession
import glob
import yaml
# import mysql.connector
from pyspark.sql import functions as psf
# from hdfs import InsecureClient
import os
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType
# import subprocess

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
    def infer_spark_type(value):
        if isinstance(value, int):
            return IntegerType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, float):
            return FloatType()
        elif isinstance(value, bool):
            return BooleanType()
        # Add more cases for other data types as needed
        else:
            # Handle unknown types or customize based on your data
            return StringType()


    @staticmethod
    def read_all_delta_files_from_folder(spark, hdfs_url, folder_path, configs):
        hdfs_client = Utilities.get_hdfs_connection(configs=configs)

        for file in hdfs_client.list(folder_path):
            delta_table_path = f"{hdfs_url}/{folder_path}/{file}"
            df = Utilities.read_delta(spark, delta_table_path)


    @staticmethod
    def exclude_attributes(source_object):
        selected_attributes = {key: value for key, value in vars(source_object).items() if key != 'arg_dict'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'spark'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'configs'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'numbers_to_generate'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'sql_connection'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'mysql_url'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'mysql_properties'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'logger'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'houseSchema'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if key != 'predictors'}
        selected_attributes = {key: value for key, value in selected_attributes.items() if 'app_base' not in key}


        selected_attributes = dict(sorted(selected_attributes.items()))
        return selected_attributes



    @staticmethod
    def update_configs(local_configs, arg_dict):
        source_target_dict = {key: value.format(**arg_dict) if isinstance(value, str) else value for key, value in local_configs.items()}
        return source_target_dict

    @staticmethod
    def replace_paths(table_configs, raw_paths):
        for table_config in table_configs:
            table_name = table_config['name']
            if table_name in raw_paths:
                table_config['path'] = raw_paths[table_name]
        return table_configs

    @staticmethod
    def get_spark_session(jar_path=None):

        if jar_path:
            jar_path = ", ".join(glob.glob(f"{jar_path}\\*jar"))

        spark = (
                SparkSession
                .builder
                .appName('LendingProductApp')
                # .config("spark.jars", jar_path)
                # .config("spark.debug.maxToStringFields", 500)
                # .config("spark.sql.debug.maxToStringFields", 500)
                # .config("spark.executor.processTreeMetrics.enabled", "false")
                # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                # .config("spark.driver.maxResultSize", "4g")  # Added as broadcasting bid trans final df writing size
                # .config("spark.rpc.message.maxSize", "1024")
                # .config("spark.driver.memory", "6g")
                # .config("spark.executor.memory", "4g")
                .getOrCreate())

        sys.path.insert(1, jar_path)
        return spark

    @staticmethod
    def read_cache_file(cache_file):
        logging.info(f"[Utilities] Reading the previously fetched max applicant id from the cache file {os.path.basename(cache_file)}..")
        with open(cache_file, 'r') as app_cache:
            last_record_id = app_cache.read()
        return last_record_id

    @staticmethod
    def update_delta_table(delta_table, id_list, pk_column, column, column_value):
        # Update the "processed" column for the specified rows
        delta_table.update(
            condition=(psf.col(pk_column).isin(id_list)),
            set={column: psf.lit(column_value)}
        )

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
    def read_csv(spark, file):
        df = spark.read.format('csv').option('InferSchema', True).option('Header', True).load(file)
        logging.info(f"[Utilities] Reading csv file = {file}, count = {df.count()}")
        return df

    @staticmethod
    def write_csv(df, file, in_mode='append'):
        df.write.format('delta').mode(in_mode).save(file)
        logging.info(f"[Utilities] Writing csv file = {file}, count = {df.count()}")

    @staticmethod
    def read_json(spark, file):
        df = spark.read.format('json').load(file)
        logging.info(f"[Utilities] Reading delta file = {file}, count = {df.count()}")
        return df

