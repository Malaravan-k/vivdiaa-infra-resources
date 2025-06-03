import logging
from datetime import datetime

LOG_FILE = f"pdf_extraction_logs_{datetime.now().strftime('%Y_%m_%d')}.log"

def setup_logger():
    logger = logging.getLogger("pdf_extraction_logger")
    logger.setLevel(logging.INFO)

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

# # Setup logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(f"pdf_extraction_logs_{datetime.now().strftime('%Y_%m_%d')}.log"),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)
