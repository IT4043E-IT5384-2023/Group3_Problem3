import logging
import os
from dotenv import load_dotenv
load_dotenv()

LOG_DIR = os.getenv("LOG_DIR")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s -  %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

def get_logger(name):
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -  %(message)s', datefmt='%d/%m/%Y %H:%M:%S',
                        level=logging.INFO)
    
    logger = logging.getLogger(name)

    handler_file = logging.FileHandler(f"{LOG_DIR}/{name}.log", 'w')
    handler_file.setFormatter(formatter)
    logger.addHandler(handler_file)

    return logger
