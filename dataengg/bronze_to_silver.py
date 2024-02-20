from utils import Utilities
from pyspark.sql import functions as psf
class BronzeToSilver:
    def __init__(self, *args, **kwargs):
        print("Init of IngestFromSources class!")
        self.configs = kwargs["configs"]
        self.local_configs = self.configs['BRONZE_TO_SILVER']
        self.local_configs_sources = self.local_configs['SOURCES']
        self.local_configs_rules = self.local_configs['DATAQUALITY']['RULES']
        self.spark = kwargs["spark"]
        print(f"100. Variables set = ")
        print(f"\nlocal_configs = {self.local_configs}")
        print(f"\nself.local_configs_sources = {self.local_configs_sources}")
        print(f"\nlocal_configs_rules = {self.local_configs_rules}")

    def execute(self):
        print("IngestFromSources: execute method")
        for source_entity in self.local_configs_sources:
            print(f"Source Entity - {source_entity}")
            source_name = self.local_configs_sources[source_entity]['NAME']
            source_path = self.local_configs_sources[source_entity]['SOURCE']
            target_path = self.local_configs_sources[source_entity]['TARGET']
            print(f"Source path = {source_path}")
            print(f"Target path = {target_path}")

            print(f"Reading source = {source_path} parquet from bronze")
            df = Utilities.read_parquet(spark=self.spark, file=source_path)
            df = df.withColumn('status', psf.lit('processed'))
            df = df.withColumn('update_date', psf.lit(psf.current_timestamp()))
            df = df.withColumn('comments', psf.lit(None))
            df = df.withColumn('dq_check', psf.lit('PASS'))

            for rule in self.local_configs_rules:
                print(f"21. Applying rule: {rule}")
                rule_no = rule['RULE']['ruleno']
                rule_name = rule['RULE']['rulename']
                table_name = rule['RULE']['tablename']
                column_name = rule['RULE']['columnname']
                values = rule['RULE']['values']

                if source_name == table_name:
                    if rule_name == 'NOT_NULL':
                        df = df.withColumn('dq_check', psf.when(psf.col(column_name).isNull(), psf.lit('FAIL'))
                                           .otherwise(psf.col('dq_check')))
                        dq_comments = f"Rule Number: {rule_no}, Rule Name: {rule_name}, Column Name: {column_name}"
                        df = df.withColumn('comments', psf.when(psf.col(column_name).isNull(), psf.lit(dq_comments))
                                           .otherwise(psf.col('comments')))
                    if rule_name == 'CHECK':
                        df = df.withColumn('dq_check', psf.when(psf.col(column_name).isin(values), psf.col('dq_check'))
                                           .otherwise(psf.lit('FAIL')))
                        dq_comments = f"Rule Number: {rule_no}, Rule Name: {rule_name}, Column Name: {column_name}"
                        df = df.withColumn('comments', psf.when(psf.col(column_name).isNull(), psf.lit(dq_comments))
                                           .otherwise(psf.col('comments')))

                    if rule_name == 'POSITIVE_VALUE':
                        df = df.withColumn('dq_check', psf.when(psf.col(column_name) < 0, psf.lit('FAIL'))
                                           .otherwise(psf.col('dq_check')))
                        dq_comments = f"Rule Number: {rule_no}, Rule Name: {rule_name}, Column Name: {column_name}"
                        df = df.withColumn('comments', psf.when(psf.col(column_name).isNull(), psf.lit(dq_comments))
                                           .otherwise(psf.col('comments')))

            df_to_silver = df.filter(psf.col('dq_check') == 'PASS')
            df_to_silver.show()
            print("Writing to Silver layers in delta format")

            Utilities.write_delta(df_to_silver, target_path)

            print(f"Read silver delta table: ")
            df = Utilities.read_delta(self.spark, target_path)
            df.show()

