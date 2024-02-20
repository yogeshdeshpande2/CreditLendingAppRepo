import logging
from utils import Utilities
import os
import datetime
from pyspark.sql import functions as psf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql import Row

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError


class DataQuality:
    def __init__(self, *args, **kwargs):
        logging.info(f"[DataQuality] DataQuality - Initialized!")
        self.arg_dict = kwargs["arg_dict"]
        self.spark = kwargs["spark"]
        self.configs = kwargs["configs"]
        self.logger = self.arg_dict["logger"]
        self.raw_sources = self.configs["DATA_QUALITY"]["SOURCES"]
        self.dq_base_path = self.configs["DATA_QUALITY"]["BASE_PATH"]
        self.dq_master_config = "/dbfs/mnt/dataengg/data_quality.yml"

    def extract_tables(self, table_list):
        table_names = [table["name"] for table in table_list]
        return table_names

    def get_expectation_run_log(self, expectation_results_result_file):
        try:
            last_log_run_id = 0
            df = Utilities.read_delta(self.spark, expectation_results_result_file)
            last_log_run_id = df.select(psf.max(psf.col('log_run_id')).alias('max_log_run_id')).collect()[0]["max_log_run_id"]
        except FileNotFoundError:
            last_log_run_id = 0
            logging.warning(f"File not found - {expectation_results_result_file}")
        except Exception as e:
            logging.warning(f"Unexpected exception - {e}")
        finally:
            next_log_run_id = last_log_run_id + 1

        return next_log_run_id

    def create_ge_context(self):
        cwd = os.getcwd()
        logging.info(f"[DataQuality] Current Working Directory before initiating context = {cwd}")

        os.chdir(f"{self.dq_base_path}")
        context = gx.get_context()
        return context

    def get_schema_for_logging(self, flattened_result):
        # Infer the schema dynamically
        schema = StructType([
            StructField(field, Utilities.infer_spark_type(value), True)
            for field, value in flattened_result.items()
        ])

        # Ensure that "raised_exception" field has BooleanType
        schema.fields[-4].dataType = BooleanType()
        schema.fields[-1].dataType = BooleanType()

        return schema

    def evaluate_expectations(self, table_config, global_rules, output_path, context):
        table_name = table_config["name"]
        columns_configs = table_config["columns"]
        expectation_results_result_file = f'{output_path}/{table_name}_expectation_results'

        batch_request = {
            "datasource_name": "mysql_datasource",
            "data_connector_name": "default_configured_data_connector_name",
            "data_asset_name": f"{table_name}",
            "limit": 1000,
        }

        expectation_suite_name = f"{table_name}"

        try:
            suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
            logging.info(
                f'[DataQuality] Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
            )
        except DataContextError:
            suite = context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
            logging.warning(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

        validator = context.get_validator(
            batch_request=BatchRequest(**batch_request),
            expectation_suite_name=expectation_suite_name,
        )

        for columns_config in columns_configs:
            next_log_run_id = self.get_expectation_run_log(expectation_results_result_file)
            column_name = columns_config["name"]
            column_rule = columns_config["rule"]

            expectation = next(
                rule["expectation_type"]
                for rule in global_rules
                if rule["name"] == column_rule
            )

            kwargs = next(
                rule.get("kwargs", {})
                for rule in global_rules
                if rule["name"] == column_rule
            )

            logging.info(f"[DataQuality] table = {table_name}, column = {column_name}, kwargs = {kwargs}, expectation = {expectation}")

            if kwargs != {}:
                min_value = kwargs.get('min_value', None)
                max_value = kwargs.get('max_value', None)
                value = kwargs.get('value', None)

                if value is not None:
                    if isinstance(value, str):
                        value = float(value)
                    result = getattr(validator, expectation)(f"{column_name}", value)
                else:
                    # Convert min_value and max_value to the appropriate data type
                    if isinstance(min_value, str):
                        min_value = float(min_value)  # or int() if it should be an integer
                    if isinstance(max_value, str):
                        max_value = float(max_value)  # or int() if it should be an integer

                    result = getattr(validator, expectation)(f"{column_name}", min_value=min_value, max_value=max_value)
            else:
                result = getattr(validator, expectation)(f"{column_name}")

            # Manually flatten the nested structure
            flattened_result = {
                **result["expectation_config"]["kwargs"],
                **result["result"],
                **result["exception_info"],
                **result["meta"],
                "success": result["success"]
            }

            schema = self.get_schema_for_logging(flattened_result)

            # Create a Row from the flattened dictionary
            result_row = Row(**flattened_result)

            # Convert the Row to a Spark DataFrame
            result_spark_df = self.spark.createDataFrame([result_row], schema=schema)
            result_spark_df_columns = result_spark_df.columns
            result_spark_df = (result_spark_df
                                .withColumn('log_run_id', psf.lit(next_log_run_id))
                                .withColumn('table', psf.lit(table_name))
                                .withColumn('log_date', psf.lit(datetime.datetime.now()))
                                )
            result_spark_df = result_spark_df.select(["log_run_id", "table", "log_date"] + result_spark_df_columns)
            result_spark_df = result_spark_df.withColumn('expectation', psf.lit(expectation))

            print(f"202. result_spark_df = ")
            result_spark_df.display()

            # Utilities.write_delta(result_spark_df, expectation_results_result_file, is_merge_schema=True)

    def execute(self):
        import yaml
        with open(self.dq_master_config) as yaml_file:
            dq_configs = yaml.load(yaml_file, Loader=yaml.BaseLoader)

        actual_tables_configs = []

        global_rules = dq_configs["GLOBAL_RULES"]
        output_path = dq_configs["TARGET_BASE_PATH"]

        tables_configs = dq_configs["TABLES"]

        tables_with_actual_raw_path = Utilities.update_configs(
            self.raw_tables, self.arg_dict
        )

        for tables_config in tables_configs:
            actual_table_config = {}
            actual_table_config["name"] = tables_config["name"]
            actual_table_config["path"] = tables_with_actual_raw_path.get(
                tables_config["name"], ""
            )
            actual_table_config["columns"] = tables_config["columns"]
            actual_tables_configs.append(actual_table_config)

        # Create a GE context # TODO: It needs hard-coded path to be in. Need to make it work dynamically
        # context = self.create_ge_context()
        context = gx.get_context('/dbfs/mnt/dataengg/data-quality/')
        print("Context created...")

        # Evaluate expectations for each table and column
        for table_config in actual_tables_configs:
            self.evaluate_expectations(table_config, global_rules, output_path, context)

        context.build_data_docs()

        # logging.info("[DataQuality] Read the data quality logs")
        # Utilities.read_all_delta_files_from_folder(spark=self.spark, hdfs_url=self.hdfs["HDFS_URL"], folder_path=self.hdfs["DQ_BASE_PATH"], configs=self.configs)

