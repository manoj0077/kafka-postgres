import logging
from config import get_config


def get_module_logger(mod_name, config_file_name):
    """
    common logging module which can be used across

    Args:
        mod_name: Name of the logger
        config_file_name: configuration file name

    Returns:
         logger
    """
    config = get_config(config_file_name)
    logger = logging.getLogger(mod_name)
    handler = logging.FileHandler(config['DEFAULT']['LOG_FILE'])
    formatter = logging.Formatter('%(asctime)s %(filename)-4s %(levelname)-4s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(config['DEFAULT']['LOG_LEVEL'])

    return logger
