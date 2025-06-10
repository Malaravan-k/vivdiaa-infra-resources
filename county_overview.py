import json
import time
import boto3
import requests
import os
import pandas as pd
from datetime import date, datetime,timedelta
import re
import logging
import logging.handlers
import io
from db_config import get_db_connection, CaseInTakeTable, get_secret

# Configure logging
log_filename = f"county_overview_{date.today().strftime('%Y%m%d')}.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Formatter for log messages
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# StreamHandler for console output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# MemoryHandler to buffer logs in memory
memory_handler = logging.handlers.MemoryHandler(capacity=10000, flushLevel=logging.ERROR)
memory_handler.setFormatter(formatter)
logger.addHandler(memory_handler)

# Global variables
bearer_token_data = {"token": None, "expires_at": 0}
COUNTY_JSON_PATH = "county_info.json"

AWS_REGION = "us-east-1"
s3_client = boto3.client("s3")

APP_CLIENT_ID = os.getenv("APP_CLIENT_ID", "f052f213-a46f-4e3c-8cbe-2c4f6025d23f")
TOKEN_SCOPE = os.getenv("TOKEN_SCOPE", "api://prd-rpa-web-services.nclea.gov/User.ReadAccess")
BUCKET_NAME = os.getenv("BUCKET_NAME", "vivid-dev-county")
BASE_URL = os.getenv("BASE_URL", "https://prdaws.nccourts.org/rpa_web_services/api/v1/partycases/")
CASE_CATEGORY = "CV"
CASE_TYPES = ["CVFCV", "CVFM", "FORSP"]
ALLOWED_CASE_PREFIXES = {"CV", "SP", "M"}
# EXCLUDE_FORSP_COUNTIES = {"Mecklenburg County", "Wake County", "Cabarrus County"}
INCLUDE_FORSP_COUNTIES = {"Mecklenburg County", "Union County", "Cabarrus County"}

secret_Arn = "arn:aws:secretsmanager:us-east-1:491085409841:secret:Vivid-pasword-store-8aMVod"
# Default dates: end date is today, start date is yesterday
default_end_date = date.today()
default_start_date = default_end_date - timedelta(days=1)
start_date_str = os.getenv("START_DATE", default_start_date.strftime("%m/%d/%Y"))
end_date_str = os.getenv("END_DATE", default_end_date.strftime("%m/%d/%Y"))


def fetch_cases_by_date_range(county, odyssey, county_node_ids, bearer_token, start_date, end_date, session):
    """Fetch cases for a given county using date range and countyNodeIDs."""
    try:
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
        all_cases = []

        params = {
            "page": "1",
            "caseCategory": CASE_CATEGORY,
            "caseFiledStartDate": start_date.strftime("%m/%d/%Y"),
            "caseFiledEndDate": end_date.strftime("%m/%d/%Y")
        }
        # case_types = [ct for ct in CASE_TYPES if ct != "FORSP"] if county in EXCLUDE_FORSP_COUNTIES else CASE_TYPES
        case_types = CASE_TYPES if county in INCLUDE_FORSP_COUNTIES else [ct for ct in CASE_TYPES if ct != "FORSP"]
        logger.info(f"Using case types {case_types} for {county}")
        params["caseType"] = case_types
        params["countyNodeID"] = county_node_ids

        try:
            prepared_request = requests.Request('GET', BASE_URL, headers=headers, params=params).prepare()
            logger.info(f"Debug: API request URL: {prepared_request.url}")

            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            if response.status_code == 504:
                raise Exception(f"504 Gateway Time-out for {county} in initial API call. Exiting script.")
            if response.status_code == 404:
                try:
                    error_data = response.json()
                    if error_data.get("status") == 404 and error_data.get("message") == "No cases were found with the search criteria.":
                        logger.info(f"API returned 404: No cases exist for {county} from {start_date} to {end_date}.")
                        return all_cases
                except ValueError:
                    logger.info(f"Non-JSON 404 response for {county}: {response.text}")
                    return all_cases
            elif response.status_code != 200:
                logger.error(f"Error fetching cases for {county}: {response.status_code} - {response.text}")
                return all_cases

            response_headers = response.headers
            link_header = response_headers.get("Link", "")
            cache_key_match = re.search(r'cacheKey=([a-zA-Z0-9]+)', link_header)
            cache_key = cache_key_match.group(1) if cache_key_match else None
            last_page_match = re.search(r'page=(\d+)>;rel="last"', link_header)
            last_page = int(last_page_match.group(1)) if last_page_match else 1

            cases_data = []
            if cache_key:
                for page in range(1, last_page + 1):
                    cache_params = {"cacheKey": cache_key, "page": str(page)}
                    cache_url = f"{BASE_URL}"
                    try:
                        response = requests.get(cache_url, headers=headers, params=cache_params, timeout=30)
                        if response.status_code != 200:
                            logger.error(f"Failed to fetch page {page} for {county}: {response.status_code} - {response.text}")
                            continue
                        page_data = response.json().get("cases", [])
                        logger.info(f"Fetched {len(page_data)} cases for {county} on page {page}")
                        for case in page_data:
                            case["county"] = county
                        cases_data.extend(page_data)
                    except requests.exceptions.JSONDecodeError as json_err:
                        logger.error(f"JSON decode error on page {page} for {county}: {str(json_err)}")
                        continue
                    except Exception as page_err:
                        logger.error(f"Error fetching page {page} for {county}: {str(page_err)}")
                        continue
            else:
                try:
                    page_data = response.json().get("cases", [])
                    logger.info(f"Fetched {len(page_data)} cases for {county} (no pagination)")
                    for case in page_data:
                        case["county"] = county
                    cases_data.extend(page_data)
                except requests.exceptions.JSONDecodeError as json_err:
                    logger.error(f"JSON decode error for {county} (no pagination): {str(json_err)}")
                    return all_cases

            if cases_data:
                all_cases.extend(cases_data)
            else:
                logger.info(f"No cases found for {county} from {start_date} to {end_date}")

            return all_cases

        except requests.exceptions.Timeout:
            raise Exception(f"Timeout occurred for {county} in initial API call. Exiting script.")
        except requests.RequestException as req_err:
            logger.error(f"Request error for {county}: {str(req_err)}")
            return all_cases
        except Exception as e:
            logger.error(f"Unexpected error fetching cases for {county}: {str(e)}")
            return all_cases

    except Exception as e:
        logger.error(f"Error in fetch_cases_by_date_range for {county}: {str(e)}")
        raise

def save_cases_to_rds(cases_data, session):
    """Loads cases and saves only PEND status cases with CV, SP, or M prefixes to RDS."""
    try:
        filtered_cases = 0
        for case in cases_data:
            try:
                case_number = case.get("caseNumber", "")
                prefix_match = re.match(r"(\d{2})(\w+?)\d+-\d+", case_number)
                if not prefix_match:
                    logger.info(f"Skipping case {case_number} due to invalid caseNumber format")
                    continue
                year, prefix = prefix_match.groups()
                if prefix not in ALLOWED_CASE_PREFIXES:
                    logger.info(f"Skipping case {case_number} with unallowed prefix {prefix} (allowed: CV, SP, M)")
                    continue
                if year != "25":
                    logger.info(f"Skipping case {case_number} from year 20{year} (only 2025 cases allowed)")
                    continue

                if case.get("caseStatus") != "PEND":
                    logger.info(f"Skipping case {case_number} with status {case.get('caseStatus', 'unknown')} (only PEND cases saved)")
                    continue

                case_style = case.get("caseStyle", "").upper()
                if "(HOA)" in case_style:
                    logger.info(f"Skipping case {case_number} due to caseStyle containing '(HOA)'")
                    continue

                existing_case = session.query(CaseInTakeTable).filter_by(case_number=case_number).first()
                if existing_case:
                    logger.info(f"Skipping case {case_number} as it already exists in the database")
                    continue

                new_case = CaseInTakeTable(
                    odyssey_id=int(str(case["caseNumber"])[-3:]),
                    node_id=case["nodeID"],
                    case_number=case_number,
                    case_status=case["caseStatus"],
                    case_style=case["caseStyle"],
                    case_type=case["caseType"],
                    county=case["county"]
                )
                session.add(new_case)
                filtered_cases += 1
            except Exception as case_err:
                logger.error(f"Error processing case {case.get('caseNumber', 'unknown')}: {str(case_err)}")
                continue

        if filtered_cases > 0:
            session.commit()
            logger.info(f"Cases saved to AWS RDS in vivid-dev-schema! {filtered_cases} PEND  cases processed.")
        else:
            logger.info("No PEND cases with CV, SP, or M prefixes found to save to RDS.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error in save_cases_to_rds: {str(e)}")
        raise

def validate_bearer_token():
    """Validate and return a valid bearer token."""
    global bearer_token_data
    try:
        current_time = int(time.time())
        if bearer_token_data["token"] and current_time <= bearer_token_data["expires_at"]:
            logger.info("Using existing valid bearer token.")
            return bearer_token_data["token"]
        
        logger.info("Bearer token expired or missing. Generating a new one.")
        new_token, new_expiry = generate_new_token()
        
        if new_token:
            bearer_token_data["token"] = new_token
            bearer_token_data["expires_at"] = new_expiry
            return new_token
        else:
            logger.error("Failed to generate new token.")
            return None
    except Exception as e:
        logger.error(f"Error in validate_bearer_token: {str(e)}")
        return None

def generate_new_token():
    """Generate a new bearer token."""
    try:
        url = "https://prdaws.nccourts.org/authentication_proxy/api/v1/authorize"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        ENCODED_EMAIL, ENCODED_PASSWORD = get_secret(secret_Arn)
        if not ENCODED_EMAIL or not ENCODED_PASSWORD:
            raise ValueError("Failed to retrieve credentials from Secrets Manager")
        
        data = {
            "appClientId": APP_CLIENT_ID,
            "tokenScope": TOKEN_SCOPE,
            "userEmailAddress": ENCODED_EMAIL,
            "userPassword": ENCODED_PASSWORD,
        }

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            token_data = response.json()
            new_token = token_data["accessToken"]
            new_expiry = int(time.time()) + 300
            logger.info("New token generated successfully.")
            return new_token, new_expiry
        else:
            logger.error(f"Failed to get new token: {response.text}")
            return None, None
    except Exception as e:
        logger.error(f"Error in generate_new_token: {str(e)}")
        return None, None

def load_county_data():
    """Load county data including county, odyssey, and codes from a JSON file."""
    try:
        with open(COUNTY_JSON_PATH, "r") as f:
            counties = json.load(f)
            return [(county, data["odyssey"], data["codes"] if isinstance(data["codes"], list) else [data["codes"]]) for county, data in counties.items()]
    except FileNotFoundError:
        logger.error(f"Error: {COUNTY_JSON_PATH} not found")
        return []
    except json.JSONDecodeError as json_err:
        logger.error(f"Error decoding JSON in {COUNTY_JSON_PATH}: {str(json_err)}")
        return []
    except Exception as e:
        logger.error(f"Error in load_county_data: {str(e)}")
        return []

def fetch_cases_for_county(county, odyssey, county_node_ids, bearer_token, session):
    """Fetch cases for a given county using a date range from environment variables."""
    try:
        results = []

        try:
            start_date = datetime.strptime(start_date_str, "%m/%d/%Y").date()
            end_date = datetime.strptime(end_date_str, "%m/%d/%Y").date()
            if start_date > end_date:
                raise ValueError("START_DATE cannot be after END_DATE")
            if start_date.year != 2025 or end_date.year != 2025:
                raise ValueError("Date range must be within 2025")
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid START_DATE/END_DATE: {str(e)}. Using defaults 04/01/2025 to 05/13/2025.")
            start_date = datetime.strptime("04/01/2025", "%m/%d/%Y").date()
            end_date = datetime.strptime("05/13/2025", "%m/%d/%Y").date()

        cases_data = fetch_cases_by_date_range(county, odyssey, county_node_ids, bearer_token, start_date, end_date, session)
        if cases_data:
            result = upload_to_s3(cases_data, county, odyssey, start_date, end_date, session)
            results.append(result)
        else:
            logger.info(f"No cases found for {county} from {start_date} to {end_date}")

        return results if results else {"status": "No new data fetched"}
    except Exception as e:
        logger.error(f"Error in fetch_cases_for_county for {county}: {str(e)}")
        raise

def upload_to_s3(data, county, odyssey, start_date, end_date, session):
    """Uploads case data to S3 in both JSON and Excel formats."""
    try:
        formatted_date = date.today().strftime("%Y_%m_%d")
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        folder_path = f"county_overview/{formatted_date}/{odyssey}/"
        json_filename = f"{folder_path}{odyssey}_{formatted_date}_county_overview_CV_CASES_{start_date_str}_{end_date_str}.json"
        xlsx_filename = f"{folder_path}{odyssey}_{formatted_date}_county_overview_CV_CASES_{start_date_str}_{end_date_str}.xlsx"

        try:
            save_cases_to_rds(data, session)
            # logger.info(f"Cases for {county} successfully saved as table")
        except Exception as rds_err:
            logger.error(f"Failed to save cases for {county} as table in RDS: {str(rds_err)}")
            raise

        json_data = json.dumps(data, indent=4)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=json_filename,
            Body=json_data,
            ContentType="application/json",
        )
        logger.info(f"Successfully uploaded JSON to S3: {json_filename}")

        case_rows = []
        for case in data:
            try:
                base_info = {
                    "nodeID": case.get("nodeID", ""),
                    "caseNumber": case.get("caseNumber", ""),
                    "caseStyle": case.get("caseStyle", ""),
                    "caseType": case.get("caseType", ""),
                    "caseSecurityGroup": case.get("caseSecurityGroup", ""),
                    "charges": json.dumps(case.get("charges", [])),
                    "county": case.get("county", ""),
                }
                case_rows.append(base_info)
            except Exception as row_err:
                logger.error(f"Error processing case row for {case.get('caseNumber', 'unknown')}: {str(row_err)}")
                continue

        df = pd.DataFrame(case_rows)
        tmp_xlsx_path = f"/tmp/data_CV_CASES_{start_date_str}_{end_date_str}.xlsx"
        with pd.ExcelWriter(tmp_xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Cases")

        with open(tmp_xlsx_path, "rb") as f:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=xlsx_filename,
                Body=f.read(),
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        logger.info(f"Successfully uploaded Excel to S3: {xlsx_filename}")

        return {"status": "Success", "json_file": json_filename, "xlsx_file": xlsx_filename}
    except Exception as e:
        logger.error(f"Error in upload_to_s3 for {county}: {str(e)}")
        return {"status": "Failure", "error": str(e)}

def county_overview():
    """Main function."""
    global bearer_token_data
    try:
        engine, Session = get_db_connection()
        session = Session()
        if not engine:
            logger.error("Error: Failed to establish database connection")
            return {"statusCode": 500, "body": "Database connection failed!"}
        
        logger.info("Scripts started execution.")
    
        bearer_token = validate_bearer_token()
        if not bearer_token:
            logger.error("Error: Failed to obtain a valid bearer token.")
            return {"statusCode": 401, "body": "Data fetching failed!"}
    
        counties = load_county_data()
        logger.info(f"Loaded {len(counties)} counties for processing.")

        for county, odyssey, county_node_ids in counties:
            try:
                response = fetch_cases_for_county(county, odyssey, county_node_ids, bearer_token, session)
                logger.info(f"Processed {county}: {response}")
            except Exception as county_err:
                logger.error(f"Error processing {county}: {str(county_err)}")
                raise

        logger.info("Scripts execution completed.")

        try:
            s3_log_key = f"logs/{log_filename}"
            log_buffer = io.StringIO()
            for record in memory_handler.buffer:
                log_buffer.write(formatter.format(record) + "\n")
            log_content = log_buffer.getvalue().encode('utf-8')
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_log_key,
                Body=log_content,
                ContentType="text/plain",
            )
            logger.info(f"Successfully uploaded log file to S3: {s3_log_key}")
            log_buffer.close()
        except Exception as log_err:
            logger.error(f"Failed to upload log file to S3: {str(log_err)}")

        return {"statusCode": 200, "body": "Data fetching completed!"}
    except Exception as e:
        logger.error(f"Error in county_overview: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error."}
    finally:
        if 'engine' in locals() and engine:
            session.close()
            engine.dispose()
            logger.info("Database connection closed.")
        memory_handler.flush()
        memory_handler.close()

if __name__ == "__main__":
    county_overview()