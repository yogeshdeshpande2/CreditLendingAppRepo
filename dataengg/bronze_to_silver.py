from utils import Utilities
from pyspark.sql import functions as psf
class BronzeToSilver:
    def __init__(self, *args, **kwargs):
        print("Init of IngestFromSources class!")
        self.configs = kwargs["configs"]
        self.local_configs = self.configs['BRONZE_TO_SILVER']
        self.spark = kwargs["spark"]
        print(f"100. Variables set = ")
        print(f"local_configs = {self.local_configs}")

    def execute(self):
        print("IngestFromSources: execute method")
        for source_entity in self.local_configs:
            print(f"Source Entity - {source_entity}")
            source_path = self.local_configs[source_entity]['SOURCE']
            target_path = self.local_configs[source_entity]['TARGET']
            print(f"Source path = {source_path}")
            print(f"Target path = {target_path}")

            print(f"Reading source = {source_path} parquet from bronze")
            df = Utilities.read_parquet(spark=self.spark, file=source_path)
            df = df.withColumn('status', psf.lit('processed'))
            df = df.withColumn('update_date', psf.lit(psf.current_timestamp()))
            print("Wrting to Silver layers in delta format")
            Utilities.write_delta(df, target_path)

            print(f"Read silver delta table: ")
            df = Utilities.read_delta(self.spark, target_path)
            df.show()

