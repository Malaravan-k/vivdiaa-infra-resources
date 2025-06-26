import requests
import time
import urllib.parse
from playwright.sync_api import sync_playwright
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import fitz  # PyMuPDF
import os
import re
import glob
import uuid
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, Column, update, String, MetaData, Date, Integer, Boolean, func, select, and_, or_, literal_column
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, date
import copy

from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.runnables import RunnableWithMessageHistory
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.messages import BaseMessage
from langchain.text_splitter import CharacterTextSplitter
from pydantic import BaseModel, Field
from typing import List
from logger_config import setup_logger

SITE_URL = "https://portal-nc.tylertech.cloud/Portal/Home/Dashboard/29"
BUCKET_NAME = os.getenv("BUCKET_NAME", "") 
print(f"BUCKET_NAME: {BUCKET_NAME}")    
if not BUCKET_NAME:
    raise ValueError("BUCKET_NAME environment variable is not set. Please set it to the S3 bucket name where you want to store the logs.")
# Database Configuration
RDS_HOST = os.getenv("RDS_HOST", "vivid-dev-database.ccn2i0geapl8.us-east-1.rds.amazonaws.com")
RDS_PORT = "5432"
RDS_DBNAME = "vivid"
RDS_USER = "vivid"
RDS_PASSWORD = "vivdiaa#4321"
DATABASE_URL = f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DBNAME}"
SCHEMA_NAME = "vivid-dev-schema"

# AWS SES Configuration
SENDER_EMAIL = "kalimalaravan0103@gmail.com"
RECIPIENT_EMAIL = "vigneshkumar@meyicloud.com"

# AWS Secrets Manager Configuration
secrets_manager_session = boto3.client("secretsmanager", region_name="us-east-1")
SECRET_ARN = os.getenv("SecretArn", "arn:aws:secretsmanager:us-east-1:491085409841:secret:vivid/dev/secret-credentials-v1eKvA")

FILE_KEYWORDS = [
    "affidavit of service", "service affidavit", "note", "promissory note", "aos",
    "foreclosure notice of hearing", "notice of hearing", "notice of foreclosure sale", "nos",
    "notice of foreclosure", "noh", "deed of trust", "trust deed",
    "guardian ad litem", "gal",
    "lien", "statement of account", "soa", "notice of sale", "service aff", "loan", "loan mod", "loan modification",
    "return of service", "service returns", "complaint", "lis pendens", "lp", "legacy complete case scan",
]

def get_secret_data(secret_arn):
    try:
        response = secrets_manager_session.get_secret_value(SecretId=secret_arn)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        return None

def load_api_keys(secret_arn):
    secrets = get_secret_data(secret_arn)
    if not secrets:
        raise ValueError("Failed to retrieve secrets from AWS Secrets Manager")
    return (
        secrets.get("OPENAI_API_KEY", ""),
        secrets.get("API_KEY", ""),
        secrets.get("CAPTCHA_SITE_KEY", "")
    )

OPENAI_API_KEY, API_KEY, CAPTCHA_SITE_KEY = load_api_keys(SECRET_ARN)

# Setup logger
logger, LOG_FILE = setup_logger()

def store_logs(LOG_FILE):
    s3 = boto3.client('s3', region_name="us-east-1")
    log_key_name = f"pdf_extraction_logs/{os.path.basename(LOG_FILE)}"
    try:
        s3.upload_file(LOG_FILE, BUCKET_NAME, log_key_name)
        logger.info(f"Log file {LOG_FILE} uploaded to s3://{BUCKET_NAME}/{log_key_name}")
    except Exception as e:
        logger.error(f"Failed to upload log to S3: {str(e)}")

def solve_captcha():
    try:
        response = requests.get(
            f"http://2captcha.com/in.php?key={API_KEY}&method=userrecaptcha&googlekey={CAPTCHA_SITE_KEY}&pageurl={SITE_URL}&json=1"
        )
        captcha_request = response.json()
        if captcha_request.get("status") != 1:
            logger.info(f"Failed to request CAPTCHA solving:{captcha_request}")
            raise ValueError(captcha_request)
        request_id = captcha_request.get("request")
        logger.info("Waiting for CAPTCHA solution...")
        for _ in range(50):
            time.sleep(3)
            solution_response = requests.get(f"http://2captcha.com/res.php?key={API_KEY}&action=get&id={request_id}&json=1")
            solution_data = solution_response.json()
            if solution_data.get("status") == 1:
                captcha_token = solution_data.get("request")
                return captcha_token, "Captcha Solved"
            elif solution_data.get("request") == "CAPCHA_NOT_READY":
                continue
            else:
                exit()
        return False, f"Failed to solve captcha: {solution_data}"
    except Exception as e:
        logger.error(f"Have issue in solve_captcha function...! {e}")
        return False, e

def upload_to_s3(file_content, case_number, file_name, date):
    s3_client = boto3.client('s3', region_name="us-east-1")
    current_date = datetime.now().strftime("%Y_%m_%d")
    document_date = date.replace("/", "_")
    odyssey_id = case_number[-3:]
    folder_path = f"case_details/{current_date}/{odyssey_id}/{case_number}/{document_date}_{file_name}.pdf"
    try:
        s3_client.put_object(
            Body=file_content,
            Bucket=BUCKET_NAME,
            Key=folder_path,
            ContentType='application/pdf'
        )
        logger.info(f"Successfully uploaded {file_name} to S3")
        return folder_path
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")

def construct_download_url(case_number, event_data):
    base_url = "https://portal-nc.tylertech.cloud/Portal/DocumentViewer/DisplayDoc"
    encoded_doc_name = urllib.parse.quote(event_data['documentName'][0]) if event_data['documentName'] else ""
    params = {
        'documentID': event_data['documentFragmentId'][0] if event_data['documentFragmentId'] else "",
        'caseNum': case_number,
        'locationId': event_data['nodeId'][0] if event_data['nodeId'] else "",
        'caseId': event_data['case_id'],
        'docTypeId': '1418',
        'isVersionId': 'false',
        'docType': event_data['description1'] if event_data['description1'] else "",
        'docName': encoded_doc_name,
        'eventName': event_data['description2']
    }
    return f"{base_url}?{urllib.parse.urlencode(params)}"

def extract_event_details(response_data, case_number):
    case_id = response_data['CaseId']
    events = response_data['Events']
    extracted_data = []
    for event in events:
        event_data = {
            'case_id': case_id,
            'description1': None,
            'description2': event['Event']['TypeId']['Description'],
            'documentFragmentId': [],
            'nodeId': [],
            'documentName': [],
            'date': event['Event']['Date']
        }
        for doc in event['Event']['Documents']:
            event_data['documentName'].append(doc['DocumentName'])
            event_data['description1'] = doc['DocumentTypeID']['Description']
            for version in doc['DocumentVersions']:
                for fragment in version['DocumentFragments']:
                    event_data['documentFragmentId'].append(fragment['DocumentFragmentID'])
            for parent in doc['ParentLinks']:
                if parent['NodeID']:
                    event_data['nodeId'].append(parent['NodeID'])
        if event_data['documentFragmentId']:
            extracted_data.append(event_data)
    return extracted_data

def inject_captcha(page, token):
    page.evaluate(f"""
    document.querySelector('[name="g-recaptcha-response"]').value = '{token}';
    document.querySelector('[name="g-recaptcha-response"]').dispatchEvent(new Event('change', {{ bubbles: true }}));
    """)
    time.sleep(3)
    injected_value = page.evaluate("document.querySelector('[name=\"g-recaptcha-response\"]').value")
    if injected_value:
        logger.info(f"CAPTCHA successfully injected: {injected_value[:20]}...")
    else:
        logger.warning("CAPTCHA injection failed. Retrying...")
        return False
    return True

def pdf_has_images(pdf_path):
    doc = fitz.open(pdf_path)
    for page_num in range(len(doc)):
        page = doc[page_num]
        images = page.get_images(full=True)
        if images:
            return True
    return False

def extract_pdf_text(pdf_path, s3_key):
    if pdf_has_images(pdf_path):
        logger.info("PDF contains images.")
        return extract_text_with_textract(s3_key)
    else:
        logger.info("PDF does not contain images.")
        return extract_text_with_fitz(pdf_path)

def extract_text_with_fitz(pdf_path):
    doc = fitz.open(pdf_path)
    full_text = ""
    for page in doc:
        full_text += page.get_text()
    return full_text

def extract_text_with_textract(s3_key):
    try:
        logger.info(f"Starting Textract job for: {s3_key}")
        textract = boto3.client("textract", region_name="us-east-1")
        response = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": BUCKET_NAME, "Name": s3_key}}
        )
        job_id = response["JobId"]
        logger.info(f"Textract job started with Job ID: {job_id}")
        while True:
            result = textract.get_document_text_detection(JobId=job_id)
            status = result["JobStatus"]
            logger.info(f"Textract Job Status: {status}")
            if status == "SUCCEEDED":
                break
            elif status == "FAILED":
                logger.error("Textract text detection job failed")
                raise Exception("Textract text detection job failed")
            time.sleep(3)
        all_blocks = []
        next_token = None
        while True:
            if next_token:
                result = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
            else:
                result = textract.get_document_text_detection(JobId=job_id)
            all_blocks.extend(result["Blocks"])
            next_token = result.get("NextToken")
            if not next_token:
                break
        extracted_text = ""
        for block in all_blocks:
            if block["BlockType"] == "LINE":
                extracted_text += block["Text"] + "\n"
        logger.info(f"Textract completed for {s3_key}")
        return extracted_text
    except Exception as e:
        logger.error(f"Error in extract_text_with_textract for {s3_key}: {str(e)}", exc_info=True)

class InMemoryHistory(BaseChatMessageHistory, BaseModel):
    messages: List[BaseMessage] = Field(default_factory=list)
    def add_messages(self, messages: List[BaseMessage]) -> None:
        self.messages.extend(messages)
    def clear(self) -> None:
        self.messages = []

session_memory_store = {}
def get_session_history(session_id: str) -> BaseChatMessageHistory:
    if session_id not in session_memory_store:
        session_memory_store[session_id] = InMemoryHistory()
    return session_memory_store[session_id]

def dict_to_structured_text(data, indent=0):
    lines = []
    spacing = " " * indent
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                lines.append(f"{spacing}- {key}:")
                lines.append(dict_to_structured_text(value, indent + 4))
            elif isinstance(value, list):
                lines.append(f"{spacing}- {key}:")
                for item in value:
                    if isinstance(item, dict):
                        lines.append(f"{' ' * (indent + 4)}-")
                        lines.append(dict_to_structured_text(item, indent + 8))
                    else:
                        lines.append(f"{' ' * (indent + 4)}- {item}")
            else:
                lines.append(f"{spacing}- {key}: {value}")
    else:
        lines.append(f"{spacing}{data}")
    return "\n".join(lines)

def process_text_with_chatgpt(text, chatgpt_summary=""):
    try:
        if isinstance(chatgpt_summary, dict):
            previous_json = "Previous extracted data:\n" + dict_to_structured_text(chatgpt_summary)
        else:
            previous_json = ""
        logger.info(f"Total counts of extracted text: {len(text)}")
        base_prompt = """  
                You are a foreclosure PDF analysis assistant. Extract structured info from legal documents into clean double-quoted JSON. Never guess missing values. Leave blank if not found. Always follow this logic:   
                ==========  
                FILTERING  
                ==========  
                - Before any further processing, scan the entire document for red flag conditions.  
                - If ANY of the following are found:  
                   - "Guardian ad Litem"  
                   - "HOA" or "Homeowners Association"  
                   - >10 heirs  
                   - Homestead exemption  
                   - Reverse mortgage, HUD/USDA/federal lien  
                   - Mobile home / trailer  
                   - Bankruptcy, guardianship, conservatorship  
                   - Government owner / grantee  
                   - Owner is an LLC, Trust, Irrevocable Trust  
                   - Ignore cases where the phrase 'Deed of Trust executed by' is followed by individual names. Only flag for Trust ownership if the owner is explicitly identified as a Trust, Trustee, or the Trust itself (e.g., 'The Lunsford Family Trust'). Do not misinterpret 'Deed of Trust executed by John Smith' as indicating trust ownership.  
                   - Joint tenant survivor  
                   - Mentions of a will, executor, or testate succession  
                   - Keywords: "partition", "quiet title", "motion to intervene"  
                   - Do NOT treat a case as a red flag just because the owner is deceased or heirs are mentioned. These are informational fields. Only flag if there is an explicit mention of:  
                a Will, Executor, Testate succession, or  
                court terms like ‘Probate Court’, ‘Letters of Administration’.  
                   - Setting Deceased = true or Heir_Flag = Yes alone should NOT trigger red_flag = Yes. Only flag if red flag keywords listed above are explicitly found in the document.  

                → Then immediately STOP processing further and return only the following output:  
                {{{{  
                  "active_indicator": false,  
                  "red_flag": "Yes",  
                  "red_flag_reason": "[Insert detected issue here, e.g., 'HOA foreclosure', 'Reverse mortgage mentioned', etc.]",  
                   “case_summary”: 10 lines → case purpose, parties, debt type, status, legal steps  
                }}}}  

                Ignore formatting issues (e.g., case # mid-sentence). Only flag “Trust” if the owner is actually a trust—not from “Deed of Trust executed by X”.  
                ===============  
                DEFENDANT INFO  
                ===============  
                - Extract ALL individual human defendants (entire doc: headers, service certs, exhibits).  
                - For each:  
                  - Full_Name (as printed), First_Name, Last_Name (if separable),  
                  - Mailing address (only if labeled: “mailing”, “sent to”, “last known”, etc),  
                  - Deceased = true only if explicitly stated.  
                - Do NOT include companies, banks, LLCs, trusts, or law firms.  
                - If vague (e.g., “heirs of”), fill Full_Name only.  
                - Do not assume they live at the property.  
                ============================  
                PROPERTY & CASE IDENTIFIERS  
                ============================  
                - Case Number: format like 25SP001130-090, 25CV007269-400.  
                - Parcel ID: look for “Parcel ID”, “Tax ID”, “PIN”, or patterns near legal descriptions (e.g., 12/3456, 55384830370000, 150-05-138).   
                - Address: use physical/legal/tax description fields only (not mailing address).  
                - Deed Book/Page: use oldest original Deed of Trust if multiple listed.  
                - Accept address variants like “commonly known as”, “Property Address”.  
                =======================  
                AMOUNT EXTRACTION RULES  
                =======================  
                - Total_Tax_Amount: sum ALL taxes/fees: “amount due”, “penalty”, “interest”, “filing fee”, “service fee”, etc.  
                - Total_Lien_Amount: non-tax liens (e.g., cleanup, fines, municipal charges).  
                - Mortgage_Balance:  
                  → Extract the total due, payoff, or amount secured by deed of trust. If not available, return the principal/note amount. Include all mortgage-type debts (1st, 2nd, HELOC) using terms like “payoff”, “secured debt”, “HELOC”. Exclude taxes, municipal fines, and judgment liens.  
                - Mortgage_Year:  
                   →Extract the earliest year tied to the original deed of trust from phrases like “Deed of Trust dated”, “executed on”, or “recorded on”.  
                =================  
                RED FLAG DETECTION  
                =================  
                If any below appears, set red_flag = "Yes", active_indicator = false:  
                - >10 heirs  
                - Homestead exemption  
                - Reverse mortgage, HUD/USDA/fed lien  
                - Mobile home/trailer  
                - Bankruptcy, guardianship, conservatorship  
                - Govt owner / grantee  
                - LLC, Trust, Irrevocable Trust  
                - Joint tenant survivor  
                - Will / testate succession  
                - Keywords: “partition”, “quiet title”, “motion to intervene”  
                Else, red_flag = "No", active_indicator = true.  
                ===================  
                DEAL EVALUATION INFO  
                ===================  
                - case_type: One of [Tax Foreclosure, Mortgage, HOA, Other]  
                - complexity_score: 1–5 (null if unclear)  
                - flag_manual_review = "Yes" if any field missing or ambiguous  
                - classification_reason: Triggering keywords  
                - case_summary: 10 lines → case purpose, parties, debt type, status, legal steps  
                - Filed_date: e.g., "04/10/2023"  
                - Status: Open, Closed, Dismissed, Other  
                - Court_type: Trial, Superior, etc.  
                ===================  
                HEIRS & PROBATE INFO  
                ===================  
                - Heir_Flag: "Yes" if any "heirs of", "heirs at law", “devisees”, etc.  
                  → Heir_Count_Estimated = “Unknown” (if vague), or numeric count if named  
                - Probate_Clue = "Yes" if terms like: “Estate of”, “Executor”, “Probate Court”, “Letters of Administration”, etc.   
                - Only set "Probate_Clue": "Yes" if terms like “Estate of”, “Executor”, “Probate Court”, or “Letters of Administration” appear. Do not set just because "Deceased" is listed.  
                ===========================  
                MERGE INSTRUCTION (if used)  
                ===========================  
                If {previous_json} is passed, update only if the new value is better or previously blank.  
                ==================  
                RETURN FORMAT (JSON)  
                ==================  
                {{{{  
                  "Property_Info": {{{{  
                    "Property_address": "", "Parcel_ID": "", "County": "",  
                    "Deed_Book_Number": "", "Deed_Page_Number": "", "Mortgage_Balance": "", "Mortgage_Year": "", "Heir_Flag": "", "Heir_Count_Estimated": "", "Probate_Clue": "",  
                    "Plaintiff_Name": "",  
                    "Defendants": [  
                      {{{{  
                        "Name": {{{{ "Full_Name": "", "First_Name": "", "Last_Name": "" }}}},  
                        "Address": {{{{ "Mailing_Address": "", "Mailing_City": "", "Mailing_State": "", "Zip_Code": "" }}}},  
                        "Deceased_Info": {{{{ "Deceased": true/false, "Deceased_Info": "" }}}}  
                      }}}}  
                    ],  
                    "Property_Use_Type": ""  
                  }}}},  
                  "Tax_Info": {{{{ "Total_Tax_Amount": "", "Total_Lien_Amount": "" }}}},  
                  "Deal_Evaluation": {{{{  
                    "case_type": "", "complexity_score": 1–5 or null, "flag_manual_review": "Yes/No",  
                    "classification_reason": "", "case_summary": "", "Filed_date": "",  
                    "Status": "", "Court_type": ""  
                  }}}},  
                  "Owner_Other_Case_Numbers": [],  
                  "red_flag": "Yes/No",  
                  "red_flag_reason": "",  
                  "active_indicator": true/false  
                }}}}  
                """ 
        full_prompt = base_prompt.format(previous_json=previous_json)
        prompt = PromptTemplate(
            input_variables=["text"],
            template=full_prompt + "\n{text}"
        )
        chat_model = ChatOpenAI(model_name="gpt-4-turbo", openai_api_key=OPENAI_API_KEY)
        chain = prompt | chat_model
        chain_with_memory = RunnableWithMessageHistory(
            chain,
            get_session_history,
            input_messages_key="text",
            history_messages_key="chat_history"
        )
        response = chain_with_memory.invoke(
            {"text": text},
            config={"configurable": {"session_id": "default-session"}}
        )
        logger.info(f"ChatGPT Response:, {response.content}")
        if response.content:
            json_match = re.search(r'```json\n(.*?)\n```', response.content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = response.content
            try:
                json_output = json.loads(json_str)
                return json.dumps(json_output)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {str(e)}")
        else:
            logger.error("Received empty response from OpenAI API.")
            return None
    except Exception as e:
        logger.error(f"Langchain error: {e}")
        return None

def get_case_numbers(engine):
    try:
        """Fetch case_number where document_pull_recommended is True"""
        metadata = MetaData(schema="vivid-dev-schema")
        
        # Reflect the specific table
        case_intake = Table("case_intake", metadata, autoload_with=engine)

        #CURRENT DATE QUERY
        query = (
            select(case_intake.c.case_number,case_intake.c.created_at)
            .where(or_(func.date(case_intake.c.created_at) == func.current_date(),or_(case_intake.c.parse_failed.is_(None))))
            .order_by(case_intake.c.case_number.asc())
        )

        #PARSE FAILED NONE QUERY
        # query = select(case_intake.c.case_number).where(or_(case_intake.c.parse_failed == True,case_intake.c.parse_failed.is_(None) )).order_by(case_intake.c.case_number.asc())

        # Execute the query
        with engine.connect() as conn:
            result = conn.execute(query)
            case_numbers_list = [row[0] for row in result.fetchall()]
            logger.info(f"\nTOTAL CASE NUMBERS RETRIEVED FROM DATABASE: {len(case_numbers_list)}")

        # case_numbers_list = ['25SP000313-120']
        return case_numbers_list
    
    except Exception as e:
        logger.error(f"Error fetching case numbers: {e}")
        return None

def get_db_connection():
    try:
        engine = create_engine(DATABASE_URL)
        logger.info("Connected to AWS RDS PostgreSQL!")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to DB: {e}")
        return None

def normalize(text):
    return text.lower().strip()

def filter_documents(events, case_number):
    def is_valid_doc(doc_name_list):
        return bool(doc_name_list and doc_name_list[0])
    if case_number[2:4] == "SP":
        matched_docs = [
            doc for doc in events
            if any(normalize(keyword) in normalize(doc['documentName'][0]) for keyword in FILE_KEYWORDS if doc.get('documentName') and doc['documentName'][0] is not None)
        ]
    if case_number[2:4] in ["CV", "M0"]:
        matched_docs = [
            doc for doc in events
            if any(normalize(keyword) in normalize(doc['documentName'][0]) for keyword in FILE_KEYWORDS if doc.get('documentName') and doc['documentName'][0] is not None)
        ]
    sorted_docs = sorted(
        matched_docs,
        key=lambda x: datetime.strptime(x['date'], "%m/%d/%Y"),
        reverse=True
    )
    return sorted_docs

def exclude_hoa_cases(classification_reason, red_flag_reason):
    patterns = ['HOA', 'Homeowner', 'Condo', 'Guardian', 'company']
    if classification_reason or red_flag_reason:
        if any(p.lower() in classification_reason.lower() for p in patterns) or any(p.lower() in red_flag_reason.lower() for p in patterns):
            return True
        else:
            return False

def update_case_intake_red_flag(engine, final_results, case_number):
    try:
        metadata = MetaData(schema=SCHEMA_NAME)
        case_intake = Table(
            "case_intake", metadata,
            Column("case_number", String, primary_key=True),
            Column("extracted_case_status", String),
            Column("filing_date", Date),
            Column("court_type", String),
            Column("complexity_score", Integer),
            Column("manual_flag", Boolean),
            Column("classification_reason", String),
            Column("extracted_case_type", String),
            Column("parse_failed", Boolean),
            Column("active_indicator", Boolean),
            Column("last_updated_at", Date),
            Column("red_flag", String),
            Column("red_flag_reason", String),
            autoload_with=engine
        )
        with engine.connect() as conn:
            update_stmt = update(case_intake).where(case_intake.c.case_number == case_number).values(
                red_flag=final_results.get("red_flag"),
                red_flag_reason=final_results.get("red_flag_reason"),
                active_indicator=final_results.get("active_indicator"),
                classification_reason=final_results.get("case_summary"),
                parse_failed=False,
                last_updated_at=datetime.now()
            )
            conn.execute(update_stmt)
            conn.commit()
        logger.info(f"Updated case_intake table with red_flag details for {case_number}")
        return True, "Red flag details updated successfully in case_intake"
    except Exception as e:
        logger.error(f"Failed to update case_intake for {case_number}: {e}")
        return False, f"Failed to update case_intake: {str(e)}"

def extract_data():
    engine = get_db_connection()
    metadata = MetaData(schema=SCHEMA_NAME)
    case_intake = Table(
        "case_intake", metadata,
        Column("case_number", String, primary_key=True),
        Column("parse_failed", Boolean),
        Column("last_updated_at", Date),
        Column("parse_failed_reason", String),
        autoload_with=engine
    )
    Failed_cases = []
    case_numbers = None
    try:
        case_numbers = get_case_numbers(engine)
        if not case_numbers:
            raise ValueError(f"[CASE NUMBERS NOT EXISTED] FOR THE CURRENT DATE {date.today()}!")
        for i in range(2):
            captcha_token, captcha_msg = solve_captcha()
            if captcha_token:
                break
        if captcha_token == False:
            raise ValueError(captcha_msg)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto(SITE_URL)
            page.evaluate(f"""
            document.querySelector('[name="g-recaptcha-response"]').value = '{captcha_token}';
            document.querySelector('[name="g-recaptcha-response"]').dispatchEvent(new Event('change', {{ bubbles: true }}));
            """)
            time.sleep(3)
            for case_number in case_numbers:
                try:
                    logger.info(f"Processing Case: {case_number}")
                    time.sleep(3)
                    captcha_token, captcha_msg = solve_captcha()
                    inject_captcha(page, captcha_token)
                    page.fill("#caseCriteria_SearchCriteria", case_number)
                    time.sleep(2)
                    page.click("#btnSSSubmit")
                    time.sleep(3)
                    try:
                        case_link = page.locator("a.caseLink").first
                        case_link.wait_for(timeout=30000)
                        logger.info("CASE LINK FOUND!")
                        page.click("a.caseLink")
                    except Exception as e:
                        logger.info("[CASE LINK NOT FOUND], Debugging page content...")
                        logger.info(f"Exception in caseLink Click via Playwright: {e}")
                        logger.info("Trying to solve CAPTCHA again...")
                        captcha_token, captcha_msg = solve_captcha()
                        if not inject_captcha(page, captcha_token):
                            logger.info("[CAPTCHA EXPIRED]! Retrying new captcha...")
                            continue
                        page.fill("#caseCriteria_SearchCriteria", case_number)
                        time.sleep(2)
                        page.click("#btnSSSubmit")
                        time.sleep(3)
                        try:
                            case_link = page.locator("a.caseLink").first
                            case_link.wait_for(timeout=40000)
                            logger.info("[CASE LINK NOT FOUND!] Clicking...")
                            case_link.click()
                        except Exception as e:
                            logger.error(f" [COULD NOT FIND THE CASE LINK FOR] : {case_number}. Skipping...")
                            raise ValueError(f"Still couldn't find case link for this {case_number}.")
                    data_url = page.get_attribute("a.caseLink", "data-url")
                    parsed_url = urllib.parse.parse_qs(urllib.parse.urlparse(data_url).query)
                    case_id = parsed_url.get("id", [""])[0]
                    if case_id:
                        api_url = f"https://portal-nc.tylertech.cloud/app/RegisterOfActionsService/CaseEvents('{case_id}')?mode=portalembed&$top=50&$skip=0"
                        response = requests.get(api_url)
                        if response.status_code == 200:
                            response_data = response.json()
                            events = extract_event_details(response_data, case_number)
                            logger.info(f"[PDF EVENTS]: {events}")
                            logger.info(f"[PDF EVENTS COUNTS]: {len(events)}")
                            if not events:
                                raise ValueError(f"PDF DOCUMENTS NOT EXIST")
                            filtered_events = filter_documents(events, case_number)
                            logger.info("\n[FILTERED DOCUMENT NAMES]:")
                            logger.info(f"{filtered_events} '\n','[FILTERED DOCUMENT COUNTS]:' {len(filtered_events)}")
                            if not filtered_events:
                                raise ValueError(f"REQUIRED PDF DOCUMENTS NOT EXIST")
                            all_pdfs = []
                            final_results = {}
                            chatgpt_summary = ""
                            for event in reversed(filtered_events):
                                download_url = construct_download_url(case_number, event)
                                pdf_response = requests.get(download_url)
                                if pdf_response.status_code == 200:
                                    file_name = event['documentName'][0]
                                    s3_key = upload_to_s3(pdf_response.content, case_number, file_name, event['date'])
                                    safe_filename = re.sub(r'[\\/:"*?<>|]+', '_', event['documentName'][0])
                                    file_name = f"{case_number}_{safe_filename}".replace(".pdf", "") + ".pdf"
                                    file_path = f"./{file_name}"
                                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                                    with open(file_path, "wb") as pdf_file:
                                        pdf_file.write(pdf_response.content)
                                    all_pdfs.append(file_path)
                                    pdf_text = extract_pdf_text(file_path, s3_key)
                                    if pdf_text is None:
                                        raise ValueError("CANNOT EXTRACT TEXT WITH TEXTRACT")
                                    if pdf_text.strip():
                                        chatgpt_summary = json.loads(process_text_with_chatgpt(pdf_text, chatgpt_summary))
                                        if chatgpt_summary is None:
                                            raise ValueError("CANNOT EXTRACT DETAILS FROM ChatGPT")
                                        if chatgpt_summary.get("red_flag") == "Yes":
                                            db_success, msg = update_case_intake_red_flag(engine, chatgpt_summary, case_number)
                                            if not db_success:
                                                raise ValueError(str(msg))
                                            logger.info(f"Red flag detected, stopping further PDF processing for {case_number}")
                                            break
                                    else:
                                        logger.error(f"Failed to extract text from PDF: {file_name}")
                                        raise ValueError(f"FAILED TO EXTRACT TEXT FROM PDF")
                                else:
                                    logger.error(f"Failed to download PDF: {pdf_response.status_code}")
                                    raise ValueError(f"FAILED TO DOWNLOAD PDF")
                            if chatgpt_summary.get("red_flag") != "Yes":
                                final_results = chatgpt_summary
                                db_success, msg = insert_final_result(engine, final_results, case_number)
                                if not db_success:
                                    raise ValueError(str(msg))
                                if db_success:
                                    logger.info(msg)
                            for pdf_file in glob.glob("*.pdf"):
                                try:
                                    os.remove(pdf_file)
                                    logger.info(f"\n[PDF REMOVED]: {pdf_file}")
                                except Exception as e:
                                    logger.error(f"[ERROR IN REMOVING PDF] {pdf_file}: {e}")
                            time.sleep(2)
                            page.go_back()
                            logger.info(f"\nEXTRACTION SUCCESSFUL! FOR THE CASE NUMBER {case_number}")
                        else:
                            logger.error(f"[API REQUEST FAILED]: {response.status_code}")
                            raise ValueError(f"PDF API REQUEST FAILED")
                except Exception as e:
                    logger.error(f"[EXTRACTION FAILED FOR THE CASE NUMBER:] {case_number}: {e}")
                    Failed_cases.append({"case_number": case_number, "error": str(e)})
                    try:
                        update_stmt = update(case_intake).where(
                            case_intake.c.case_number == case_number).values(
                            parse_failed=True, last_updated_at=datetime.now(), parse_failed_reason=str(e))
                        with engine.connect() as conn:
                            conn.execute(update_stmt)
                            conn.commit()
                            logger.info(f" 'parse_failed' updated to True in DB for {case_number}")
                    except Exception as db_err:
                        logger.error(f" Failed to update 'parse_failed' in DB for {case_number}: {db_err}")
                    for pdf_file in glob.glob("*.pdf"):
                        try:
                            os.remove(pdf_file)
                            logger.info(f"\n[PDF REMOVED]: {pdf_file}")
                        except Exception as e:
                            logger.error(f"[ERROR IN REMOVING PDF] {pdf_file}: {e}")
                    time.sleep(2)
                    page.go_back()
    except Exception as e:
        logger.error(f"[EXTRACTION FAILED IN extract_data FUNCTION]: {e}")
    return {"failed_cases": Failed_cases}

def insert_final_result(engine, final_results, case_number):
    try:
        metadata = MetaData(schema=SCHEMA_NAME)
        if not isinstance(engine, sqlalchemy.engine.Engine):
            raise TypeError("get_db_connection() did not return a valid SQLAlchemy Engine instance")
        case_intake = Table(
            "case_intake", metadata,
            Column("case_number", String, primary_key=True),
            Column("extracted_case_status", String),
            Column("filing_date", Date),
            Column("court_type", String),
            Column("complexity_score", Integer),
            Column("manual_flag", Boolean),
            Column("classification_reason", String),
            Column("extracted_case_type", String),
            Column("parse_failed", Boolean),
            Column("active_indicator", Boolean),
            Column("last_updated_at", Date),
            Column("red_flag", String),
            Column("red_flag_reason", String),
            autoload_with=engine
        )
        property_info = Table(
            "property_info", metadata,
            Column("id", UUID(as_uuid=True), primary_key=True),
            Column("case_number", String),
            Column("property_address", String),
            Column("parcel_or_tax_id", String),
            Column("land_use", String),
            Column("owner_name", String),
            Column("first_name", String),
            Column("last_name", String),
            Column("owner_mailing_address", String),
            Column("mailing_address", String),
            Column("mailing_city", String),
            Column("mailing_state", String),
            Column("zip_code", String),
            Column("owner_deceased", Boolean),
            Column("created_at", Date),
            Column("owner_other_case_numbers", String),
            Column("deed_book_number", String),
            Column("deed_page_number", String),
            Column("mortgage_balance", Integer),
            Column("number_of_heirs", String),
            Column("owner_deceased_reason", String),
            Column("property_use_type", String),
            Column("manual_review", Boolean),
            Column("manual_review_reason", String),
            autoload_with=engine
        )
        tax_info = Table(
            "tax_info", metadata,
            Column("id", UUID(as_uuid=True), primary_key=True),
            Column("case_number", String),
            Column("amount_owed", Integer),
            Column("parcel_or_tax_id", String),
            Column("created_at", Date),
            Column("total_tax_value", Integer),
            Column("tax_due_or_lien_amount", Integer),
            Column("manual_review", Boolean),
            Column("manual_review_reason", String),
            autoload_with=engine
        )
        result_property_info = final_results.get("Property_Info")
        result_tax_info = final_results.get("Tax_Info")
        result_deal_evaluation = final_results.get("Deal_Evaluation")
        if result_deal_evaluation["Filed_date"] not in ['', None, "None"]:
            try:
                filed_date = datetime.strptime(result_deal_evaluation["Filed_date"], "%B %d, %Y").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    filed_date = datetime.strptime(result_deal_evaluation["Filed_date"], "%m/%d/%Y").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        filed_date = datetime.strptime(result_deal_evaluation["Filed_date"], "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError as e:
                        raise ValueError("Error parsing in filed_date:", str(e))
        else:
            filed_date = None
        logger.info(f"[FILED DATE]: {filed_date} {type(filed_date)}")
        mortgage_balance = result_property_info.get("Mortgage_Balance")
        logger.info(f"[MORTGAGE BALANCE]: {mortgage_balance}")
        if mortgage_balance not in ['', None, "None"]:
            try:
                mortgage_cleaned_value = mortgage_balance.strip("$").replace(",", "").strip()
                if mortgage_cleaned_value in ['', None, "None"]:
                    mortgage_balance = None
                else:
                    mortgage_balance = int(float(mortgage_cleaned_value))
            except Exception as e:
                logger.error(f"Error parsing Mortgage Balance: {e}")
                mortgage_balance = None
                raise ValueError(f"Error parsing Mortgage Balance: {str(e)}")
        else:
            mortgage_balance = None
        total_tax_value = result_tax_info.get("Total_Tax_Amount")
        logger.info(f"total tax value: {total_tax_value}")
        if total_tax_value not in ['', None, "None"]:
            try:
                tax_cleaned_value = total_tax_value.strip("$").replace(",", "").strip()
                if tax_cleaned_value in ['', None, "None"]:
                    total_tax_value = None
                else:
                    total_tax_value = int(float(tax_cleaned_value))
            except Exception as e:
                logger.error(f"Error parsing Total Tax Value: {e}")
                total_tax_value = None
                raise ValueError(f"Error parsing Total Tax Value: {str(e)}")
        else:
            total_tax_value = None
        tax_due_or_lien_amount = result_tax_info.get("Total_Lien_Amount")
        logger.info(f"tax due or lien amount: {tax_due_or_lien_amount}")
        if tax_due_or_lien_amount not in ['', None, "None"]:
            try:
                tax_due_cleaned_value = tax_due_or_lien_amount.strip("$").replace(",", "").strip()
                if tax_due_cleaned_value in ['', None, "None"]:
                    tax_due_or_lien_amount = None
                else:
                    tax_due_or_lien_amount = int(float(tax_due_cleaned_value))
            except Exception as e:
                logger.error(f"Error parsing Tax Due or Lien Amount: {e}")
                tax_due_or_lien_amount = None
                raise ValueError(f"Error parsing Tax Due or Lien Amount: {str(e)}")
        else:
            tax_due_or_lien_amount = None
        if mortgage_balance and total_tax_value:
            if total_tax_value > mortgage_balance:
                total_tax_value = total_tax_value - mortgage_balance
        if mortgage_balance and tax_due_or_lien_amount:
            if tax_due_or_lien_amount > mortgage_balance:
                tax_due_or_lien_amount = tax_due_or_lien_amount - mortgage_balance
        mortgage_bal = mortgage_balance if mortgage_balance else 0
        total_tax_val = total_tax_value if total_tax_value else 0
        tax_due_or_lien = tax_due_or_lien_amount if tax_due_or_lien_amount else 0
        amount_owed = mortgage_bal + total_tax_val + tax_due_or_lien
        print("Mortgage Bal:", mortgage_bal)
        print("total tax val:", total_tax_val)
        print("tax_due_or_lien:", tax_due_or_lien)
        print("amount owed", amount_owed)
        parcel_no = result_property_info.get("Parcel_ID")
        if parcel_no:
            parcel_no = parcel_no.replace("-", "")
        if not amount_owed:
            tax_manual_review = True
            tax_manual_review_reason = 'AMOUNT OWED IS EMPTY'
        else:
            tax_manual_review = False
            tax_manual_review_reason = ''
        if not result_property_info.get("Property_address") and not parcel_no:
            property_manual_review = True
            prop_manual_review_reason = 'EMPTY PROPERTY ADDRESS AND PARCEL NO'
        else:
            property_manual_review = False
            prop_manual_review_reason = ''
        tax_id = uuid.uuid4()
        invalid_case = exclude_hoa_cases(result_deal_evaluation.get("classification_reason"), final_results.get("red_flag_reason"))
        with engine.connect() as conn:
            update_stmt = update(case_intake).where(case_intake.c.case_number == case_number).values(
                extracted_case_status=result_deal_evaluation.get("Status"),
                filing_date=filed_date,
                court_type=result_deal_evaluation.get("Court_type"),
                extracted_case_type=result_deal_evaluation.get("case_type"),
                complexity_score=result_deal_evaluation.get("complexity_score"),
                manual_flag=result_deal_evaluation.get("flag_manual_review").strip().lower() == "yes",
                classification_reason=result_deal_evaluation.get("case_summary"),
                parse_failed=False,
                last_updated_at=datetime.now(),
                red_flag=final_results.get("red_flag"),
                red_flag_reason=final_results.get("red_flag_reason"),
                active_indicator=final_results.get("active_indicator")
            )
            conn.execute(update_stmt)
            defendants = result_property_info.get('Defendants') #[value for key, value in result_property_info.items() if key.startswith("Defendants")]
            for defendant in defendants:
                address = defendant.get("Address")
                address_parts = [
                    address.get("Mailing_Address"),
                    address.get("Mailing_City"),
                    address.get("Mailing_State"),
                    address.get("Zip_Code")
                ]
                new_property_id = str(uuid.uuid4())
                combined_address = ", ".join(filter(None, address_parts)) if any(address_parts) else None
                insert_property = property_info.insert().values(
                    id=new_property_id,
                    case_number=case_number,
                    property_address=result_property_info.get("Property_address"),
                    parcel_or_tax_id=parcel_no,
                    owner_name=defendant["Name"]['Full_Name'].upper() if defendant["Name"]['Full_Name'] else "",
                    first_name=defendant["Name"]["First_Name"].upper() if defendant["Name"]["First_Name"] else "",
                    last_name=defendant["Name"]["Last_Name"].upper() if defendant["Name"]["Last_Name"] else "",
                    owner_mailing_address=combined_address.upper() if combined_address else "",
                    mailing_address=address.get('Mailing_Address', '').upper(),
                    mailing_city=address.get('Mailing_City', '').upper(),
                    mailing_state=address.get('Mailing_State', '').upper(),
                    zip_code=address.get('Zip_Code', ''),
                    owner_deceased=None if defendant["Deceased_Info"]["Deceased"] == "None" else bool(defendant["Deceased_Info"]["Deceased"]),
                    owner_other_case_numbers=str(final_results.get('Owner_Other_Case_Numbers')),
                    deed_book_number=result_property_info.get("Deed_Book_Number"),
                    deed_page_number=result_property_info.get("Deed_Page_Number"),
                    mortgage_balance=mortgage_balance,
                    number_of_heirs=result_property_info.get("Heir_Count_Estimated"),
                    owner_deceased_reason=defendant["Deceased_Info"]["Deceased_Info"],
                    property_use_type=result_property_info.get("Property_Use_Type"),
                    manual_review=property_manual_review,
                    manual_review_reason=prop_manual_review_reason,
                    created_at=datetime.now(),
                )
                conn.execute(insert_property)
            insert_tax = tax_info.insert().values(
                id=tax_id,
                case_number=case_number,
                amount_owed=amount_owed,
                mortgage_balance=mortgage_balance,
                created_at=datetime.now(),
                parcel_or_tax_id=parcel_no,
                tax_due_or_lien_amount=tax_due_or_lien_amount,
                total_tax_value=total_tax_value,
                manual_review=tax_manual_review,
                manual_review_reason=tax_manual_review_reason,
            )
            conn.execute(insert_tax)
            
            conn.commit()
        return True, "[DATA UPDATED SUCCESSFULLY IN DB!]"
    except Exception as e:
        logger.error(f"\n[UPDATION FAILED] for this case number {case_number} \n Error inserting data: {e}")
        return False, "DB UPDATE FAILED"

def send_email_notification(failed_cases):
    ses_client = boto3.client('ses', region_name='us-east-1')
    subject = "Notification: Failed Case Parsing"
    if not failed_cases:
        logger.info("No failed cases to report via email.")
        return
    failed_cases_list = "\n".join([f"Case Number: {case['case_number']}, Error: {case['error']}" for case in failed_cases])
    body_text = f"The following cases failed parsing:\n\n{failed_cases_list}"
    body_html = f"<p>The following cases failed parsing:</p><ul>{failed_cases_list}</ul>"
    email_params = {
        'Source': SENDER_EMAIL,
        'Destination': {
            'ToAddresses': [RECIPIENT_EMAIL]
        },
        'Message': {
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': body_text,
                    'Charset': 'UTF-8'
                },
                'Html': {
                    'Data': body_html,
                    'Charset': 'UTF-8'
                }
            }
        }
    }
    try:
        response = ses_client.send_email(**email_params)
        logger.info(f"Email sent successfully! Message ID: {response['MessageId']}")
    except ClientError as e:
        logger.info(f"Error sending email: {e.response['Error']['Message']}")

def send_extraction_email_notification(msg):
    ses_client = boto3.client('ses', region_name='us-east-1')
    subject = "Notification: EXTRACTION FAILED"
    body_text = f"[EXTRACTION FAILED]\n{msg}"
    body_html = f"<p>[EXTRACTION FAILED]</p><ul>{msg}</ul>"
    email_params = {
        'Source': SENDER_EMAIL,
        'Destination': {
            'ToAddresses': [RECIPIENT_EMAIL]
        },
        'Message': {
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': body_text,
                    'Charset': 'UTF-8'
                },
                'Html': {
                    'Data': body_html,
                    'Charset': 'UTF-8'
                }
            }
        }
    }
    try:
        response = ses_client.send_email(**email_params)
        logger.info(f"Email sent successfully! Message ID: {response['MessageId']}")
    except ClientError as e:
        logger.error(f"Error sending email: {e.response['Error']['Message']}")

if __name__ == "__main__":
    try:
        response = extract_data()
        logger.info(f"\n Failed cases: {response['failed_cases']}")
        failed_cases = response["failed_cases"]
        if len(failed_cases) >= 1:
            send_email_notification(failed_cases)
        else:
            logger.warning("No failed cases")
    except Exception as e:
        logger.error(e)
    finally:
        store_logs(LOG_FILE)