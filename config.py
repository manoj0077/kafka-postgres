import configparser


def get_config(config_path):
    """
    Parses the test_aiven.ini file

    Returns:
         test_aiven.ini content in dict format
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config