from utils import Utilities
from pyspark.sql import functions as psf

class BronzeToSilver:
    def __init__(self, *args, **kwargs):
        # Instance variables
        self.arg_dict = kwargs["arg_dict"]
        self.configs = kwargs["configs"]
        self.local_configs = self.configs['BRONZE_TO_SILVER']
        self.local_configs_sources = self.local_configs['SOURCES']
        self.local_configs_rules = self.local_configs['DATAQUALITY']['RULES']
        self.spark = kwargs["spark"]
        self.logger = self.arg_dict['logger']

    def execute(self):
        """
        Purpose: This is the starting point of this class called from Main
        :return:
        """
        # Read each source from the Bronze layer
        for source_entity in self.local_configs_sources:
            self.logger.info(f"Processing source- {source_entity}")
            source_name = self.local_configs_sources[source_entity]['NAME']
            source_path = self.local_configs_sources[source_entity]['SOURCE']
            target_path = self.local_configs_sources[source_entity]['TARGET']
            self.logger.info(f"Source path = {source_path}")
            self.logger.info(f"Target path = {target_path}")

            self.logger.info(f"Read the source in parquet format in bronze layer")
            df = Utilities.read_parquet(spark=self.spark, file=source_path)
            df = df.withColumn('status', psf.lit('processed'))
            df = df.withColumn('update_date', psf.lit(psf.current_timestamp()))
            df = df.withColumn('comments', psf.lit(None))
            df = df.withColumn('dq_check', psf.lit('PASS'))

            # Extract each rule defined in the master config for this class and apply
            for rule in self.local_configs_rules:
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
            self.logger.info("Writing the curated data to Silver layer in delta format")

            Utilities.write_delta(df_to_silver, target_path)


