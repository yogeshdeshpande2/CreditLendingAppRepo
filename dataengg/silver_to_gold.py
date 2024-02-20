from utils import Utilities
from pyspark.sql import functions as psf


class SilverToGold:
    def __init__(self, *args, **kwargs):
        # Instance variables
        self.configs = kwargs["configs"]
        self.arg_dict = kwargs["arg_dict"]
        self.local_configs = self.configs['SILVER_TO_GOLD']
        self.local_configs_sources = self.local_configs['SOURCES']
        self.local_configs_target = self.local_configs['TARGET']
        self.spark = kwargs["spark"]
        self.logger = self.arg_dict['logger']

        self.df_clients = None
        self.df_collaterals = None
        self.df_stocks = None

    def explode_df(self, source_df):
        """
        Purpose: Explode at the stocks as one client can have multiple stocks
        :param source_df: input spark df
        :return: output spark df
        """
        df_pd = source_df.toPandas()
        df_pd = df_pd.assign(stocks=df_pd['stocks'].str.split(',')).explode('stocks')
        df_target = self.spark.createDataFrame(df_pd)
        df_target = df_target.withColumnRenamed('stocks', 'stock')
        return df_target

    def build_collateral_status(self):
        df_target = self.df_clients.join(self.df_collaterals, "client_id", "inner")
        df_target = df_target.join(self.df_stocks, "stock", "inner")
        return  df_target

    def execute(self):
        """
        Purpose: This is the starting point of this class called from Main
        :return:
        """
        # Read each source from the Silver layer
        for source_entity in self.local_configs_sources:
            self.logger.info(f"Processing source- {source_entity}")
            source_name = self.local_configs_sources[source_entity]['NAME']
            source_path = self.local_configs_sources[source_entity]['PATH']

            self.logger.info(f"Source path = {source_path}")

            # Derive the clients dataframe
            if source_name == 'clients':
                self.df_clients = Utilities.read_delta(self.spark, file=source_path)
                print("1. df_clients = ")
                self.df_clients.show()

            # Derive the collaterals dataframe
            if source_name == 'collaterals':
                self.df_collaterals = Utilities.read_delta(self.spark, file=source_path)

                # Explode at the stocks as one client can have multiple stocks
                self.df_collaterals = self.explode_df(self.df_collaterals)
                # df_collaterals_pd = df_collaterals.toPandas()
                # df_collaterals = df_collaterals_pd.assign(stocks=df_collaterals_pd['stocks'].str.split(',')).explode('stocks')
                # df_collaterals = self.spark.createDataFrame(df_collaterals)
                print("2. df_collaterals = ")
                self.df_collaterals.show()

            # Derive the stocks dataframe
            if source_name == 'stocks':
                self.df_stocks = Utilities.read_delta(self.spark, file=source_path)
                print("3. df_stocks = ")
                self.df_stocks.show()

        df_target = self.build_collateral_status()
        print("Joined df_target = ")
        df_target.show()


