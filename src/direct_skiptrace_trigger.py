import psycopg2
import requests
import json
import os
import uuid
import boto3
import logging
import sys
import utils
from io import StringIO
from datetime import datetime
from psycopg2 import Error as Psycopg2Error
from requests.exceptions import RequestException
from boto3.exceptions import Boto3Error
 
logger, LOG_FILE = utils.setup_logger()

SECRET_ARN = os.environ.get("SecretArn")
print("SECRET_ARN;;",SECRET_ARN)

# DirectSkip API configuration
DIRECTSKIP_API_URL = os.getenv("DIRECTSKIP_API_URL", "https://api0.directskip.com/v2/search_contact.php")
API_KEY = os.getenv('API_KEY', "")
 
# S3 configuration
S3_BUCKET = os.getenv('S3_BUCKET', "")
S3_FOLDER = os.getenv('S3_FOLDER', "")

def store_logs(log_file):
    s3 = boto3.client('s3', region_name="us-east-1")
    log_key_name = f"{S3_FOLDER}/{os.path.basename(log_file)}"
    try:
        if os.path.exists(log_file):
            s3.upload_file(log_file, S3_BUCKET, log_key_name)
            logger.info(f"Log file {log_file} uploaded to s3://{S3_BUCKET}/{log_key_name}")
            print(f"Log file {log_file} uploaded to s3://{S3_BUCKET}/{log_key_name}")
        else:
            logger.warning(f"Log file {log_file} does not exist, cannot upload to S3")
            print(f"Log file {log_file} does not exist, cannot upload to S3")
    except Exception as e:
        logger.error(f"Failed to upload log to S3: {str(e)}")
        print(f"Failed to upload log to S3: {str(e)}")
 
def check_if_column_exists(conn, schema_name, cursor, table_name, column_name):
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            AND column_name = %s
        );
        """
        cursor.execute(query, (schema_name, table_name, column_name))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    except Psycopg2Error as e:
        logger.error(f"check_if_column_exists: Database error while checking column {column_name} in {table_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"check_if_column_exists: Unexpected error while checking column {column_name} in {table_name}: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
 
def add_column(conn, schema_name, cursor, table_name, column_name, column_type):
    cursor = None
    try:
        cursor = conn.cursor()
        query = f"""
        ALTER TABLE "{schema_name}"."{table_name}"
        ADD COLUMN IF NOT EXISTS "{column_name}" {column_type};
        """
        cursor.execute(query)
        conn.commit()
        logger.info(f"add_column: Column {column_name} added successfully to {table_name}.")
        return True
    except Psycopg2Error as e:
        logger.error(f"add_column: Database error while adding column {column_name} to {table_name}: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logger.error(f"add_column: Unexpected error while adding column {column_name} to {table_name}: {e}")
        conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
 
def get_properties(conn, schema_name, cursor, batch_start=None, batch_end=None):
    cursor = None
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT
            id,
            case_number,
            first_name,
            last_name,
            mailing_address,
            mailing_city,
            mailing_state,
            zip_code,
            property_address,
            owner_name
        FROM "{schema_name}"."property_info"
        WHERE equity_status IN ('MID', 'HIGH')
        AND skip_trace_status IS NULL
        ORDER BY case_number, id
        """
 
        if batch_start is not None and batch_end is not None:
            query += f" OFFSET {batch_start - 1} LIMIT {batch_end - batch_start + 1}"
 
        cursor.execute(query)
        properties = cursor.fetchall()
        logger.info(f"get_properties: Retrieved {len(properties)} properties.")
        return properties
    except Psycopg2Error as e:
        logger.error(f"get_properties: Database error while retrieving properties: {e}")
        return []
    except Exception as e:
        logger.error(f"get_properties: Unexpected error while retrieving properties: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
 
def call_skip_trace_api(property_data):
    try:
        id, case_number, first_name, last_name, mailing_address, mailing_city, \
        mailing_state, zip_code, property_address, owner_name = property_data
 
        if not (first_name and last_name) and not mailing_address:
            logger.info(f"call_skip_trace_api: Skipping case number {case_number} (id: {id}) due to missing both name and mailing address.")
            return None
 
        payload = {
            "api_key": API_KEY,
            "first_name": first_name or "",
            "last_name": last_name or "",
            "mailing_address": mailing_address or "",
            "mailing_city": mailing_city or "",
            "mailing_state": mailing_state or "",
            "mailing_zip": zip_code or "",
            "custom_field_1": case_number or "",
            "custom_field_2": property_address or "",
            "custom_field_3": owner_name or "",
            "auto_match_boost": 0,
            "dnc_scrub": 0,
            "owner_fix": 0
        }
 
        if first_name and last_name:
            logger.info(f"call_skip_trace_api: Calling API with name-based information for case number {case_number} (id: {id})")
        else:
            logger.info(f"call_skip_trace_api: Calling API with address-based information for case number {case_number} (id: {id})")
 
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
 
        response = requests.post(DIRECTSKIP_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        api_response = response.json()
        return api_response
    except RequestException as e:
        logger.error(f"call_skip_trace_api: API call failed for case number {case_number} (id: {id}): {e}")
        return None
    except Exception as e:
        logger.error(f"call_skip_trace_api: Unexpected error for case number {case_number} (id: {id}): {e}")
        return None
 
def extract_phone_numbers(api_response):
    try:
        phone_numbers = []
 
        if not api_response or "contacts" not in api_response or not api_response["contacts"]:
            logger.info("extract_phone_numbers: No contacts found in API response.")
            return phone_numbers
 
        contact = api_response["contacts"][0]
        if "phones" in contact and contact["phones"]:
            for phone in contact["phones"]:
                if phone.get("phonenumber") and phone.get("dnc_litigator_scrub") != "DNC":
                    phone_numbers.append(phone["phonenumber"])
                    if len(phone_numbers) >= 3:
                        break
            logger.info(f"extract_phone_numbers: Found {len(phone_numbers)} phone numbers in primary contact.")
        else:
            logger.info("extract_phone_numbers: No phone numbers found in primary contact.")
 
        if len(phone_numbers) < 3 and "relatives" in contact:
            for relative in contact["relatives"]:
                if "phones" in relative:
                    for phone in relative["phones"]:
                        if phone.get("phonenumber") and phone.get("dnc_litigator_scrub") != "DNC":
                            if phone["phonenumber"] not in phone_numbers:
                                phone_numbers.append(phone["phonenumber"])
                                if len(phone_numbers) >= 3:
                                    break
                    if len(phone_numbers) >= 3:
                        break
            logger.info(f"extract_phone_numbers: Found {len(phone_numbers)} total phone numbers after checking relatives.")
        else:
            logger.info("extract_phone_numbers: No relatives or relative phone numbers found.")
 
        return phone_numbers[:3]
    except KeyError as e:
        logger.error(f"extract_phone_numbers: Key error in API response: {e}")
        return []
    except Exception as e:
        logger.error(f"extract_phone_numbers: Unexpected error: {e}")
        return []
 
def update_property_with_phone_numbers(conn, schema_name, cursor, id, case_number, phone_numbers, failure_reason):
    cursor = None
    try:
        cursor = conn.cursor()
        if phone_numbers and len(phone_numbers) > 0:
            cursor.execute(f"""
                SELECT case_number, parcel_or_tax_id, owner_name, first_name, last_name
                FROM "{schema_name}"."property_info"
                WHERE id = %s
            """, (id,))
            row = cursor.fetchone()
 
            if row:
                case_number, parcel_or_tax_id, owner_name, first_name, last_name = row
 
                while len(phone_numbers) < 3:
                    phone_numbers.append(None)
 
                phone_no1, phone_no2, phone_no3 = phone_numbers[:3]
 
                phone_no1_type = 'mobile' if phone_no1 else None
                phone_no2_type = 'home' if phone_no2 else None
                phone_no3_type = 'work' if phone_no3 else None
 
                insert_query = f"""
                INSERT INTO "{schema_name}".phone_number_info (
                    id,
                    case_number,
                    parcel_or_tax_id,
                    owner_name,
                    first_name,
                    last_name,
                    phone_no1,
                    phone_no1_type,
                    phone_no2,
                    phone_no2_type,
                    phone_no3,
                    phone_no3_type,
                    created_at,
                    update_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
 
                new_uuid = str(uuid.uuid4())
                current_time = datetime.now()
 
                cursor.execute(insert_query, (
                    new_uuid,
                    case_number,
                    parcel_or_tax_id,
                    owner_name,
                    first_name,
                    last_name,
                    phone_no1,
                    phone_no1_type,
                    phone_no2,
                    phone_no2_type,
                    phone_no3,
                    phone_no3_type,
                    current_time,
                    current_time
                ))
 
                update_query = f"""
                UPDATE "{schema_name}"."property_info"
                SET
                    skip_trace_status = 'true',
                    skip_trace_failure_reason = NULL,
                    last_updated_at = NOW()
                WHERE id = %s
                """
                cursor.execute(update_query, (id,))
 
                status = "inserted"
                logger.info(f"update_property_with_phone_numbers:  Inserted phone numbers and updated skip_trace_status to 'true' for case {case_number} (id: {id})")
            else:
                update_query = f"""
                UPDATE "{schema_name}"."property_info"
                SET
                    skip_trace_status = 'false',
                    skip_trace_failure_reason = %s,
                    last_updated_at = NOW()
                WHERE id = %s
                """
                cursor.execute(update_query, ("Could not find property details", id))
 
                status = "property_not_found"
                logger.info(f"update_property_with_phone_numbers:  Could not find property details for id: {id}, updated skip_trace_status to 'false'")
        else:
            update_query = f"""
            UPDATE "{schema_name}"."property_info"
            SET
                skip_trace_status = 'false',
                skip_trace_failure_reason = %s,
                last_updated_at = NOW()
            WHERE id = %s
            """
            cursor.execute(update_query, (failure_reason, id))
 
            status = "no_phone_found"
            logger.info(f"update_property_with_phone_numbers:  No phone numbers found for case {case_number} (id: {id}), updated skip_trace_status to 'false' with reason: {failure_reason}")
 
        conn.commit()
        return status
    except Psycopg2Error as e:
        logger.error(f"update_property_with_phone_numbers: Database error for case {case_number} (id: {id}): {e}")
        if cursor:
            try:
                update_query = f"""
                UPDATE "{schema_name}"."property_info"
                SET
                    skip_trace_status = 'false',
                    skip_trace_failure_reason = %s,
                    last_updated_at = NOW()
                WHERE id = %s
                """
                cursor.execute(update_query, (f"Database error: {str(e)}", id))
                conn.commit()
            except Psycopg2Error as commit_e:
                logger.error(f"update_property_with_phone_numbers: Failed to update skip_trace_status for case {case_number} (id: {id}): {commit_e}")
                conn.rollback()
        return "error"
    except Exception as e:
        logger.error(f"update_property_with_phone_numbers: Unexpected error for case {case_number} (id: {id}): {e}")
        if cursor:
            try:
                update_query = f"""
                UPDATE "{schema_name}"."property_info"
                SET
                    skip_trace_status = 'false',
                    skip_trace_failure_reason = %s,
                    last_updated_at = NOW()
                WHERE id = %s
                """
                cursor.execute(update_query, (f"Unexpected error: {str(e)}", id))
                conn.commit()
            except Psycopg2Error as commit_e:
                logger.error(f"update_property_with_phone_numbers: Failed to update skip_trace_status for case {case_number} (id: {id}): {commit_e}")
                conn.rollback()
        return "error"
    finally:
        if cursor:
            cursor.close()
 
def lambda_handler(event, context):
    # Generate unique request ID and log file name
    request_id = str(uuid.uuid4())
    log_file = LOG_FILE  # Use LOG_FILE from setup_logger
    try:
        logger.info(f"skip_trace: Starting request {request_id}")
 
        # Handle batch parameters from event
        batch_start = event.get('batch_start')
        batch_end = event.get('batch_end')
        
        if batch_start is not None and batch_end is not None:
            logger.info(f"skip_trace: Lambda request {request_id} with batch_start={batch_start}, batch_end={batch_end}")
        else:
            logger.info(f"skip_trace: Lambda request {request_id} without batch parameters")
 
        conn, cursor, schema_name = utils.dbConnection(SECRET_ARN)
        if not conn:
            logger.error(f"skip_trace: Failed to connect to database for request {request_id}")
            return {
                "statusCode": 500,
                "body": json.dumps({"status": "error", "message": "Database connection failed"})
            }
 
        # Check and add required columns
        for column, col_type in [
            ("skip_trace_status", "TEXT"),
            ("skip_trace_failure_reason", "TEXT")
        ]:
            if not check_if_column_exists(conn, schema_name, cursor, "property_info", column):
                if not add_column(conn, schema_name, cursor, "property_info", column, col_type):
                    logger.error(f"skip_trace: Failed to add column {column} for request {request_id}")
                    conn.close()
                    return {
                        "statusCode": 500,
                        "body": json.dumps({"status": "error", "message": f"Failed to add column {column}"})
                    }
 
        properties = get_properties(conn, schema_name, cursor,batch_start, batch_end)
        if not properties:
            logger.info(f"skip_trace: No properties to process for request {request_id}")
            conn.close()
            return {
                "statusCode": 200,
                "body": json.dumps({"status": "success", "message": "No properties to process", "log_file": log_file})
            }
 
        report_data = []
        for property_data in properties:
            try:
                id, case_number, first_name, last_name, mailing_address, mailing_city, mailing_state, zip_code, property_address, owner_name = property_data
                logger.info(f"skip_trace: Processing case number: {case_number}, id: {id} for request {request_id}")
 
                # Determine API call type
                if first_name and last_name and mailing_address and mailing_city and mailing_state and zip_code:
                    api_call_type = "complete_info"
                elif first_name and last_name:
                    api_call_type = "name_based"
                elif mailing_address:
                    api_call_type = "address_based"
                else:
                    api_call_type = "skipped"
 
                failure_reason = ""
 
                if first_name and last_name or mailing_address:
                    api_response = call_skip_trace_api(property_data)
                    phone_numbers = extract_phone_numbers(api_response) if api_response else []
 
                    if not phone_numbers:
                        failure_reason = "No valid phone numbers from API"
                    else:
                        failure_reason = "Success"
                else:
                    failure_reason = "Insufficient data (missing name and address)"
                    phone_numbers = []
 
                status = update_property_with_phone_numbers(conn, schema_name, cursor, id, case_number, phone_numbers, failure_reason)
 
                report_data.append({
                    'id': id,
                    'case_number': case_number,
                    'first_name': first_name,
                    'last_name': last_name,
                    'phone_numbers': phone_numbers,
                    'update_status': status,
                    'failure_reason': failure_reason,
                    'api_call_type': api_call_type,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                logger.info(f"skip_trace: Processed case {case_number}, id {id} - {status}, Phone numbers: {phone_numbers}, API Call Type: {api_call_type}, Reason: {failure_reason} for request {request_id}")
            except Exception as e:
                logger.error(f"skip_trace: Error processing case {case_number} (id: {id}) for request {request_id}: {e}")

        conn.close()
        logger.info(f"skip_trace: Request {request_id} completed successfully")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "message": "Process completed",
                "log_file": log_file,
                "processed_count": len(properties),
                "report_data": report_data
            })
        }
 
    except Exception as e:
        logger.error(f"skip_trace: Fatal error in request {request_id}: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": f"Fatal error: {str(e)}"})
        }
    finally:
        store_logs(log_file)