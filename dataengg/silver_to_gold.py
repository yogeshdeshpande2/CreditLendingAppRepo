from utils import Utilities
from pyspark.sql import  functions as psf

class SilverToGold:
    def __init__(self, *args, **kwargs):
        # Instance variables
        self.configs = kwargs["configs"]
        self.arg_dict = kwargs["arg_dict"]
        self.local_configs = self.configs['SILVER_TO_GOLD']
        self.local_configs_sources = self.local_configs['SOURCES']
        self.local_configs_target_detail = self.local_configs['TARGET_DETAIL']
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

        # Split the 'stocks' column into 'stock' and 'units'
        split_col = psf.split(df_target['stocks'], '-')
        df_target = df_target.withColumn('stock', split_col.getItem(0))
        df_target = df_target.withColumn('units', split_col.getItem(1))

        df_target = df_target.drop('stocks')
        return df_target

    def join_collateral_sources(self):
        """
        Purpose: Joins the sources from silver layer
        :param: None
        :return: target dataframe
        """
        df_target = self.df_clients.join(self.df_collaterals, "client_id", "inner")
        df_target = df_target.join(self.df_stocks, "stock", "inner")
        return df_target

    def calc_collateral_status_detail(self, df_source):
        """
        Calculates the final collateral status detail table
        :param df_source:
        :return:
        """
        df_target = df_source.withColumn('stocks_value', psf.round(psf.col('units') * psf.col('price'), 2))
        return df_target

    def calc_collateral_status(self, df_source):
        """
        Calculates the final collateral status table to be consumed by the end user or downstream
        :param df_source:
        :return:
        """
        df_source = df_source.drop('stock')
        df_target = df_source.groupBy('client_id', 'first_name', 'last_name', 'Date').sum('stocks_value')
        df_target = df_target.withColumnRenamed('sum(stocks_value)', 'stocks_value')
        return df_target

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
                # self.df_clients.show()

            # Derive the collaterals dataframe
            if source_name == 'collaterals':
                self.df_collaterals = Utilities.read_delta(self.spark, file=source_path)

                # Explode at the stocks as one client can have multiple stocks
                self.df_collaterals = self.explode_df(self.df_collaterals)
                # self.df_collaterals.show()

            # Derive the stocks dataframe
            if source_name == 'stocks':
                self.df_stocks = Utilities.read_delta(self.spark, file=source_path)
                # self.df_stocks.show()

        df_joined = self.join_collateral_sources()
        select_columns = ['client_id', 'first_name', 'last_name',
                          # 'email', 'phone', 'address1', 'address2', 'city', 'zipcode',
                          # 'corporate_name', 'role', 'income_type', 'income',
                          'Date', 'stock', 'units', 'price'
                          ]

        # Join the silver sources
        df_joined = df_joined.select(select_columns)

        # Calculate the collateral detail
        df_collateral_status_detail = self.calc_collateral_status_detail(df_joined)
        # df_collateral_status_detail.show()
        self.logger.info(f"Writing collateral detail the curated data to Gold layer in delta format "
                         f"at {self.local_configs_target_detail}")
        Utilities.write_delta(df_collateral_status_detail, self.local_configs_target_detail)

        df_collateral_status_detail = df_collateral_status_detail.drop('units', 'price')

        # Calculate collateral status
        df_collateral_status = self.calc_collateral_status(df_collateral_status_detail)
        # df_collateral_status.show()
        self.logger.info(f"Writing collateral detail the curated data to Gold layer in delta format "
                         f"at {self.local_configs_target}")
        Utilities.write_delta(df_collateral_status, self.local_configs_target)
