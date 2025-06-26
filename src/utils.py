import psycopg2
from psycopg2 import sql
import json
import boto3
import os
import logging
from datetime import datetime
import uuid
from psycopg2 import pool

REGION = os.environ.get('REGION','us-east-1')   
session = boto3.Session(region_name=REGION)
secrets_manager_session = session.client('secretsmanager')

# Define log file in /tmp directory
AIRTABLE_LOG_FILE = f"/tmp/airtable_skip_trace_log_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}.log"
LOG_FILE = f"/tmp/skip_trace_log_batch_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}.log"

def dbConnection(secret_Arn):
    try:
        secret_value = json.loads(get_secret_data(secret_Arn))
        connection = psycopg2.connect(
            f"dbname='{secret_value['dbname']}' user='{secret_value['user']}' host='{secret_value['host']}' password='{secret_value['password']}'"
        )
        cursor = connection.cursor()
    except Exception as e:
        print(f"Error in dbConnection: {e}")
        return None, None, None
    else:
        return connection, cursor, secret_value['schema']

def get_secret_data(secret_Arn):
    try:
        secret_value = secrets_manager_session.get_secret_value(SecretId=secret_Arn)['SecretString']
        return secret_value  
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None  

def setup_logger():
    logger = logging.getLogger("skip_trace_logger")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent logs from propagating to parent loggers

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

    logger.debug(f"Logger initialized with file handler for {LOG_FILE}")
    print(f"Logger initialized with file handler for {LOG_FILE}")

    return logger, LOG_FILE

def airtable_setup_logger():
    logger = logging.getLogger("skip_trace_logger")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent logs from propagating to parent loggers

    if not logger.handlers:
        # File handler
        file_handler = logging.FileHandler(AIRTABLE_LOG_FILE)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console (stream) handler
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    logger.debug(f"Logger initialized with file handler for {AIRTABLE_LOG_FILE}")
    print(f"Logger initialized with file handler for {AIRTABLE_LOG_FILE}")

    return logger, AIRTABLE_LOG_FILE

# Configuration for PostgreSQL to Airtable sync mappings
# Each mapping defines how data from a PostgreSQL table should be synced to Airtable tables

TABLE_MAPPINGS = {
    # Sync property_info table (no filtering)
    "property_info_sync": {
        "source_table": "property_info",
        "airtable_table": "property_info",
        "filter": {
            "filter_column": None,
            "filter_value": None
        }
    },
    
    # Sync tax_info table (no filtering)
    "tax_info_sync": {
        "source_table": "tax_info", 
        "airtable_table": "tax_info",
        "filter": {
            "filter_column": None,
            "filter_value": None
        }
    },
    
    # Sync case_intake table where active_indicator = True to case_intake_valid
    "case_intake_valid_sync": {
        "source_table": "case_intake",
        "airtable_table": "case_intake_valid",
        "filter": {
            "filter_column": "active_indicator",
            "filter_value": True
        }
    },
    
    # Sync case_intake table where active_indicator = False to case_intake_invalid
    "case_intake_invalid_sync": {
        "source_table": "case_intake",
        "airtable_table": "case_intake_invalid", 
        "filter": {
            "filter_column": "active_indicator",
            "filter_value": False
        }
    }
}

TABLE_CONFIG = {
    "phone_number_info": {
        "filter_column": "",
        "filter_value": ""
    }
}

