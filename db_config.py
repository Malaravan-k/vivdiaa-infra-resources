import uuid
from sqlalchemy import create_engine, Column, Integer, String, Boolean, JSON, MetaData, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
import boto3
import json
import base64
import boto3
import os


# Database Configuration
RDS_HOST = "vivid-dev-database.ccn2i0geapl8.us-east-1.rds.amazonaws.com"
RDS_PORT = "5432"
RDS_DBNAME = "vivid"
RDS_USER = "vivid"
RDS_PASSWORD = "vivdiaa#4321"

DATABASE_URL = f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DBNAME}"
SCHEMA_NAME = "vivid-dev-schema"

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

session = boto3.Session(region_name=AWS_REGION)
secrets_manager_session = session.client('secretsmanager')

# Initialize DB Connection
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
meta_data = MetaData(schema=SCHEMA_NAME)
Base = declarative_base(metadata=meta_data)

class CaseInTakeTable(Base):  # Changed class name to avoid spaces
    __tablename__ = "case_intake"  # Changed table name to remove spaces
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    odyssey_id = Column(Integer, nullable=False)
    node_id = Column(Integer, nullable=False)
    case_number = Column(String(50), nullable=False)
    case_status = Column(String(20), nullable=False)
    case_style = Column(String, nullable=False)
    case_type = Column(String(50), nullable=False)
    county = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
# Create tables in the schema
Base.metadata.create_all(engine)

def get_db_connection():
    """Returns DB session and engine"""
    try:
        print("Connected to AWS RDS PostgreSQL!")
        return engine, SessionLocal
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None, None

def get_secret(secret_Arn):
    try:
        # Retrieve and parse secret data
        secret_value = secrets_manager_session.get_secret_value(SecretId=secret_Arn)['SecretString']
        secret_data = json.loads(secret_value)
        # Encoding Email & Password
        ENCODED_EMAIL = base64.b64encode(secret_data['Email'].encode()).decode()
        ENCODED_PASSWORD = base64.b64encode(secret_data['Password'].encode()).decode()
        # Establish database connection
        return ENCODED_EMAIL, ENCODED_PASSWORD
    except Exception as e:
        print(f"Error in get_secret: {e}")
        return None

def split_PLA_DEF(case_parties_json):
    try:
        case_parties = json.loads(case_parties_json) if isinstance(case_parties_json, str) else case_parties_json
        formatted_output = {}
        for party in case_parties:
            formattedPartyName = party.get("formattedPartyName")
            connectionType = party.get("connectionType")
    
            if connectionType in ["DEF", "PLA"]:
                formatted_output.setdefault(connectionType, []).append(formattedPartyName)
            else:
                formatted_output = {"DEF":[None] ,"PLA":[None]}
    
        # print(f"Extracted parties: {formatted_output}")
        return formatted_output
    except Exception as error:
        print(f"Error in split_PLA_DEF: {error}")
        return None

