from utils import Utilities
from pyspark.sql import functions as psf

class SilverToGold:
    def __init__(self, *args, **kwargs):
        print("Init of SilverToGold class!")
        self.configs = kwargs["configs"]
        self.local_configs = self.configs['SILVER_TO_GOLD']
        self.local_configs_sources = self.local_configs['SOURCES']
        self.local_configs_target = self.local_configs['TARGET']
        self.spark = kwargs["spark"]
        print(f"100. Variables set = ")
        print(f"\nlocal_configs = {self.local_configs}")
        print(f"\nself.local_configs_sources = {self.local_configs_sources}")
        print(f"\nlocal_configs_target = {self.local_configs_target}")

    def execute(self):
        # Read sources
        print("SilverToGold: execute method")
        for source_entity in self.local_configs_sources:
            print(f"Source Entity - {source_entity}")
            source_name = self.local_configs_sources[source_entity]['NAME']
            source_path = self.local_configs_sources[source_entity]['PATH']

            print(f"Source path = {source_path}")

            if source_name == 'clients':
                df_clients = Utilities.read_delta(self.spark, file=source_path)
                print("1. df_clients = ")
                df_clients.show()

            if source_name == 'collaterals':
                df_collaterals = Utilities.read_delta(self.spark, file=source_path)
                df_collaterals_pd = df_collaterals.toPandas()
                df_collaterals = df_collaterals_pd.assign(stocks=df_collaterals_pd['stocks'].str.split(',')).explode('stocks')
                df_collaterals = self.spark.createDataFrame(df_collaterals)
                print("2. df_collaterals = ")
                df_collaterals.show()

            if source_name == 'stocks':
                df_stocks = Utilities.read_delta(self.spark, file=source_path)
                print("3. df_stocks = ")
                df_stocks.show()


