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
        arg_dict = {}
        arg_dict['module_name'] = arg_list[1]    # class to be executed

        self.logger = logging.getLogger(__name__)
        # logging.config.fileConfig("/mnt/dataengg/logging.conf")

        logging.info("[Main] Creating a spark session")
        spark = Utilities.get_spark_session()

        self.master_config = "/dbfs/mnt/dataengg/master_config.yml"
        with open(self.master_config) as yaml_file:
            configs = yaml.load(yaml_file, Loader=yaml.BaseLoader)

        arg_dict['spark'] = spark

        logging.info("[Main] Getting the class from the class-map")
        module_dict = ClassMap.get_class_map()

        arg_dict['logger'] = self.logger

        module_handler = module_dict[arg_dict['module_name']](spark=spark, configs=configs, arg_dict=arg_dict)
        module_handler.execute()


if __name__ == "__main__":
    Main(sys.argv[:])
    print("Main class execution complete!")

