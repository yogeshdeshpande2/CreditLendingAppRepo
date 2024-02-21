class ClassMap:

    @staticmethod
    def get_class_map():
        """
        Purpose: This method gives the class to be called for the argument passed to Main.py from ADF
        """
        # Import all the necessary classes
        from bronze_to_silver import BronzeToSilver
        from silver_to_gold import SilverToGold

        # Create a class map dictionary which maps the arg passed to Main to which class module to execute
        class_map_dict = {

            "BronzeToSilver": BronzeToSilver,
            "SilverToGold": SilverToGold,

        }
        return class_map_dict
