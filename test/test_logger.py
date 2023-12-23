import sys
sys.path.append(".")

from logger.logger import get_logger

if __name__ == "__main__":
    logger = get_logger("test")

    logger.info("test info")
    logger.error("test error")
