import logging
from datetime import datetime
import os


LOG_FILE = f"equity_finding_logs_{datetime.now().strftime('%Y_%m_%d')}.log"

def setup_logger():
    logger = logging.getLogger("equity_finding_logger")
    logger.setLevel(logging.INFO)

    print(logging.FileHandler(LOG_FILE))

    if not logger.handlers:
        # File handler
        file_handler = logging.FileHandler(LOG_FILE)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console (stream) handler
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    return logger, LOG_FILE