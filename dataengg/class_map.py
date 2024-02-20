class ClassMap:

    @staticmethod
    def get_class_map():

        # Import all the necessary classes
        from bronze_to_silver import BronzeToSilver
        from silver_to_gold import SilverToGold
        from data_quality import DataQuality

        # Create a class map dictionary which maps the arg passed to Main to which class module to execute
        class_map_dict = {

            "BronzeToSilver": BronzeToSilver,
            "SilverToGold": SilverToGold,
            "DataQuality": DataQuality
        }
        return class_map_dict
