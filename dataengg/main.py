import sys
sys.path.append("/dbfs/mnt/dataengg")


from class_map import ClassMap
import ast
import logging
import logging.config
import sys
import os
from utils import Utilities
import yaml
class Main():
    def __init__(self, arg_list):
        # arg_dict = arg_list[0]    # ast.literal_eval(args_list[0])    # ADF format args list
        arg_dict = {}
        arg_dict['module_name'] = arg_list[1]    # class to be executed
        print(f"1. arg_dict['module_name'] = {arg_dict['module_name']}")
        sys.path.append("/dbfs/mnt/dataengg")
        self.logger = logging.getLogger(__name__)
        # logging.config.fileConfig("/mnt/dataengg/logging.conf")

        logging.info("[Main] Creating a spark session")
        spark = Utilities.get_spark_session()
        # spark = Utilities.get_spark_session(self.app_base_jar)

        import yaml
        self.master_config = "/dbfs/mnt/dataengg/master_config.yml"
        with open(self.master_config) as yaml_file:
            configs = yaml.load(yaml_file, Loader=yaml.BaseLoader)

        print(f"2. configs = {configs}")

        self.app_base_dataengg = "".join(configs['BASE_PATH']['HOME_PATH'])
        self.app_base_log = "".join(configs['BASE_PATH']['LOG_PATH'])
        self.app_base_jar = "".join(configs['BASE_PATH']['JARS'])
        self.app_base_unittest = "".join(configs['BASE_PATH']['UNITTESTS'])
        self.app_base_app_path = "".join(configs['APP_PATHS'])
        self.app_base_bronze_to_silver = "".join(configs['BRONZE_TO_SILVER'])
        self.app_base_silver_to_gold = "".join(configs['SILVER_TO_GOLD'])

        for key, value in self.__dict__.items():
            if 'app_base' in key:
                arg_dict[key] = value
        arg_dict['spark'] = spark

        print(f"3. arg_dict = {arg_dict}")

        logging.info("[Main] Getting the class from the class-map")
        module_dict = ClassMap.get_class_map()

        arg_dict['logger'] = self.logger

        module_handler = module_dict[arg_dict['module_name']](spark=spark, configs=configs, arg_dict=arg_dict)
        module_handler.execute()


if __name__ == "__main__":
    Main(sys.argv[:])
    print("Main class execution complete!")

    # with Main(sys.argv[:]):
    #     print("Main class execution complete!")
