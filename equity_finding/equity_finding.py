import sqlalchemy
from sqlalchemy import create_engine,Table, select, Column, update, String, MetaData, Date, Integer,Numeric, Boolean,func, select, and_,or_, literal_column,union_all, null,cast,text
# from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import distinct
import json
import boto3
import os
from logger_config_equity import setup_logger
import time
import re

# Database Configuration
RDS_HOST = "vivid-dev-database.ccn2i0geapl8.us-east-1.rds.amazonaws.com"
RDS_PORT = "5432"
RDS_DBNAME = "vivid"
RDS_USER = "vivid"
RDS_PASSWORD = "vivdiaa#4321"

DATABASE_URL = f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DBNAME}"
SCHEMA_NAME = "vivid-dev-schema"

BUCKET_NAME = "vivid-cleaned-geojson"

COUNTIES = {
    '910': 'wake',
    '890': 'union',
    '120': 'cabarrus',
    '670': 'orange',
    '180': 'chatham',
    '400': 'guilford',
    '500': 'johnston',
    '640': 'newhanover',
    '090': 'brunswick',
    '000': 'alamance',
    '750': 'randolph',
    '590': 'mecklenburg',
    '310': 'durham'
}

def get_db_connection():
    """Returns DB engine"""
    try:
        engine = create_engine(DATABASE_URL)
        logger.info("Connected to AWS RDS PostgreSQL!")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to DB: {e}")
        return None

# def get_owner_names():
#     return

logger, LOG_FILE = setup_logger()

def store_logs(LOG_FILE):
    s3 = boto3.client('s3',region_name="us-east-1")
    LOG_BUCKET_NAME = "vivid-dev-county"
    log_key_name = f"equity_finding_logs/{os.path.basename(LOG_FILE)}"
    try:
        s3.upload_file(LOG_FILE, LOG_BUCKET_NAME, log_key_name)
        logger.info(f"Log file {LOG_FILE} uploaded to s3://{LOG_BUCKET_NAME}/{log_key_name}")
    except Exception as e:
        logger.error(f"Failed to upload log to S3: {str(e)}")

def get_parcel_numbers(engine):

    try:
        """Fetch case_number where document_pull_recommended is True"""
        
        metadata = MetaData(schema=SCHEMA_NAME)
        # Reflect the table
        tax_info = Table("tax_info", metadata, autoload_with=engine)
        property_info = Table("property_info",metadata,autoload_with=engine)
        
        # p = property_info.alias("p")
        # t = tax_info.alias("t")

        query = (
            select(
                property_info.c.case_number,
                property_info.c.parcel_or_tax_id,
                property_info.c.property_address,
                tax_info.c.amount_owed,
                property_info.c.deed_book_number,
                property_info.c.deed_page_number,
            )
            .distinct(property_info.c.case_number)
            .select_from(
                property_info.join(tax_info, property_info.c.case_number == tax_info.c.case_number)
            )
            .where(
                and_(
                    property_info.c.manual_review == False,
                    tax_info.c.manual_review == False,
                    property_info.c.equity.is_(None),
                    tax_info.c.equity.is_(None),
                    property_info.c.created_at >= '2025-05-13'
                )
            )
            .order_by(property_info.c.case_number.asc())
        )
        print("QUERY: ",query)
        # query = (
        #     select(
        #         property_info.c.case_number,
        #         property_info.c.parcel_or_tax_id,
        #         property_info.c.property_address,
        #         tax_info.c.amount_owed,
        #     )
        #     .distinct(property_info.c.case_number)  # DISTINCT ON (p.case_number)
        #     .select_from(
        #         property_info.join(tax_info, property_info.c.case_number == tax_info.c.case_number)
        #     )
        #     .where(
        #         and_(
        #             or_(
        #                 and_(
        #                     property_info.c.parcel_or_tax_id.isnot(None),
        #                     property_info.c.parcel_or_tax_id != ''
        #                 ),
        #                 property_info.c.property_address.isnot(None)
        #             ),
        #             property_info.c.ncmap_updated.is_(None)
        #         )
        #     )
        #     .order_by(property_info.c.case_number.asc())
        # )

        # Execute the query
        with engine.connect() as conn:
            result = conn.execute(query)
            # case_numbers_list = [row[0] for row in result.fetchall()]
            data = [{"case_number": row[0], "parcel_id": row[1],"amount_owed":row[3], "property_address":row[2], "deed_book_number": row[4], "deed_page_number": row[5]} for row in result]

        logger.info(f"Parcel Data: {data} Length: {len(data)}")
        return data
    

    
    except Exception as e:
        logger.error(f"Error fetching parcel numbers: {e}")
        return None

def get_geojson(county_id):
    s3 = boto3.client('s3',region_name="us-east-1")
    county_name = COUNTIES.get(county_id, '')

    if not county_name:
        return {
            'success': False,
            'msg': f"County name not found for ID {county_id}",
            'data': None
        }

    s3_key = f"{county_name}_{county_id}_geojson/nc_{county_name}_parcels_poly.geojson"
    print(f"S3 Key: {s3_key}")

    # Ensure local directory exists
    local_dir = "geojson_cache"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, f"{county_id}_polygon.geojson")

    if os.path.exists(local_path):
        logger.info("Geojson existed in local path!")
        with open(local_path, 'r') as f:
            geojson_data = json.load(f)
            time.sleep(2)
        return geojson_data
    else:
        try:
            logger.info("Downloading from S3...")
            response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            geojson_data = json.loads(response['Body'].read())

            # Save locally for future use
            with open(local_path, 'w') as f:
                json.dump(geojson_data, f)
                logger.info("GEOJSON SAVED IN LOCAL FOLDER")
            time.sleep(1)
            with open(local_path, 'r') as r:
                geojson_dict_data = json.load(r)
                time.sleep(2)
            return geojson_dict_data
        except s3.exceptions.NoSuchKey:
            return None
        

# def map_with_parcel_id(geojson_file):
#     parcel_lookup = {}
#     features = geojson_file.get("features", [])
#     for i, feature in enumerate(features):
#         try:
#             properties = feature.get("properties", {})
#             parcel_id = properties.get("PARNO")
#             if not parcel_id:
#                 continue
#             parcel_lookup[parcel_id] = {
#                     "PARVAL": properties.get("PARVAL"),
#                     "SOURCEREF": properties.get("SOURCEREF"),
#                     "SADDNO": properties.get("SADDNO"),
#                     "SADDPREF": properties.get("SADDPREF"),
#                     "SADDSTNAME": properties.get("SADDSTNAME"),
#                     "SADDSTR": properties.get("SADDSTR"),
#                     "SADDSTSUF": properties.get("SADDSTSUF"),
#                     "SADDSTTYP": properties.get("SADDSTTYP"),
#                     "SALEDATE": properties.get("SALEDATE"),
#                     "SALEDATETX": properties.get("SALEDATETX"),
#                     "SCITY": properties.get("SCITY"),
#                     "SITEADD": properties.get("SITEADD"),
#                     "OWNFRST": properties.get("OWNFRST"),
#                     "OWNLAST": properties.get("OWNLAST"),
#                     "OWNNAME": properties.get("OWNNAME"),
#                     "MAILADD": properties.get("MAILADD"),
#                 }
#         except Exception as e:
#             print(f"[SKIP FEATURE {i}] Error: {e}")
#     return parcel_lookup


def map_with_parcel_id(geojson_file):
    try:
        parcel_lookup = {
            feature["properties"]["PARNO"]: {
                "PARVAL": feature["properties"].get("PARVAL"),
                "SOURCEREF": feature["properties"].get("SOURCEREF"),
                "SADDNO": feature["properties"].get("SADDNO"),
                "SADDSTNAME": feature["properties"].get("SADDSTNAME"),
                "SADDSTR": feature["properties"].get("SADDSTR"),
                "SADDSTTYP": feature["properties"].get("SADDSTTYP"),
                "SCITY": feature["properties"].get("SCITY"),
                "SITEADD": feature["properties"].get("SITEADD"),
                "OWNFRST": feature["properties"].get("OWNFRST"),
                "OWNLAST": feature["properties"].get("OWNLAST"),
                "OWNNAME": feature["properties"].get("OWNNAME"),
                "MAILADD": feature["properties"].get("MAILADD"),
            }
            for feature in geojson_file.get("features", [])
            if feature["properties"].get("PARNO")
        }

        alt_parcel_lookup = {
            feature["properties"]["ALTPARNO"]: {
                "PARVAL": feature["properties"].get("PARVAL"),
                "SOURCEREF": feature["properties"].get("SOURCEREF"),
                "SADDNO": feature["properties"].get("SADDNO"),
                "SADDSTNAME": feature["properties"].get("SADDSTNAME"),
                "SADDSTR": feature["properties"].get("SADDSTR"),
                "SADDSTTYP": feature["properties"].get("SADDSTTYP"),
                "SCITY": feature["properties"].get("SCITY"),
                "SITEADD": feature["properties"].get("SITEADD"),
                "OWNFRST": feature["properties"].get("OWNFRST"),
                "OWNLAST": feature["properties"].get("OWNLAST"),
                "OWNNAME": feature["properties"].get("OWNNAME"),
                "MAILADD": feature["properties"].get("MAILADD"),
            }
            for feature in geojson_file.get("features", [])
            if feature["properties"].get("ALTPARNO")
        }
        time.sleep(2)

        return {
            'success': True,
            'msg': 'Successfully extracted geojson',
            'data': {'parcel_lookup': parcel_lookup, 'alt_parcel_lookup': alt_parcel_lookup}
        }
    except Exception as e:
        return {
            'success': False,
            'msg': f"Error loading GeoJSON: {e}",
            'data': None
        }

def map_with_book_page_no(geojson_file):
    try:
        book_page_lookup = {}
        for feature in geojson_file.get("features", []):
            source_ref = feature["properties"].get("SOURCEREF")
            if not source_ref:
                continue
            
            # Split SOURCEREF into book and page (e.g., "33649/924" -> ["33649", "924"])
            try:
                book, page = source_ref.split('/')
                book = book.strip()
                page = page.strip()
                # Create a normalized key for comparison (e.g., "33649-924")
                book_page_key = f"{book}-{page}"
                
                book_page_lookup[book_page_key] = {
                    "PARNO": feature["properties"].get("PARNO"),
                    "PARVAL": feature["properties"].get("PARVAL"),
                    "SOURCEREF": source_ref,
                    "SADDNO": feature["properties"].get("SADDNO"),
                    "SADDSTNAME": feature["properties"].get("SADDSTNAME"),
                    "SADDSTR": feature["properties"].get("SADDSTR"),
                    "SADDSTTYP": feature["properties"].get("SADDSTTYP"),
                    "SCITY": feature["properties"].get("SCITY"),
                    "SITEADD": feature["properties"].get("SITEADD"),
                    "OWNFRST": feature["properties"].get("OWNFRST"),
                    "OWNLAST": feature["properties"].get("OWNLAST"),
                    "OWNNAME": feature["properties"].get("OWNNAME"),
                    "MAILADD": feature["properties"].get("MAILADD"),
                }
            except ValueError:
                logger.warning(f"Invalid SOURCEREF format: {source_ref}")
                continue

        time.sleep(2)
        return book_page_lookup
    except Exception as e:
        logger.info(f"[ERROR IN BOOK NO MAPPING:] {str(e)}")
        return None

def map_with_mailing_address(geojson_file):
    try:
        mailing_address_lookup = {
            feature["properties"]["MAILADD"]: {
                "PARNO":feature["properties"].get("PARNO"),
                "PARVAL": feature["properties"].get("PARVAL"),
                "SOURCEREF": feature["properties"].get("SOURCEREF"),
                "SADDNO": feature["properties"].get("SADDNO"),
                "SADDSTNAME": feature["properties"].get("SADDSTNAME"),
                "SADDSTTYP": feature["properties"].get("SADDSTTYP"),
                "SADDSTR": feature["properties"].get("SADDSTR"),
                "SCITY": feature["properties"].get("SCITY"),
                "SITEADD": feature["properties"].get("SITEADD"),
                "OWNFRST": feature["properties"].get("OWNFRST"),
                "OWNLAST": feature["properties"].get("OWNLAST"),
                "OWNNAME": feature["properties"].get("OWNNAME"),
                "MAILADD": feature["properties"].get("MAILADD"),
            }
            for feature in geojson_file.get("features", [])
            if feature["properties"].get("MAILADD")
        }
        return mailing_address_lookup
    except Exception as e:
        return None

def map_with_site_address(geojson_file):
    try:
        mailing_address_lookup = {
            feature["properties"]["SITEADD"]: {
                "PARNO":feature["properties"].get("PARNO"),
                "PARVAL": feature["properties"].get("PARVAL"),
                "SOURCEREF": feature["properties"].get("SOURCEREF"),
                "SADDNO": feature["properties"].get("SADDNO"),
                "SADDSTNAME": feature["properties"].get("SADDSTNAME"),
                "SADDSTTYP": feature["properties"].get("SADDSTTYP"),
                "SADDSTR": feature["properties"].get("SADDSTR"),
                "SCITY": feature["properties"].get("SCITY"),
                "SITEADD": feature["properties"].get("SITEADD"),
                "OWNFRST": feature["properties"].get("OWNFRST"),
                "OWNLAST": feature["properties"].get("OWNLAST"),
                "OWNNAME": feature["properties"].get("OWNNAME"),
                "MAILADD": feature["properties"].get("MAILADD"),
            }
            for feature in geojson_file.get("features", [])
            if feature["properties"].get("SITEADD")
        }
        return mailing_address_lookup
    except Exception as e:
        return None

import re

def extract_mailing_address(address):
    # # Normalize address by removing commas and trimming
    # address = address.replace(",", "").strip()

    # Directional keywords
    directional_keywords = [
        "Northeast", "North East", "NE",
        "Northwest", "North West", "NW",
        "Southeast", "South East", "SE",
        "Southwest", "South West", "SW",
    ]

    # Street type keywords
    street_keywords = [
        "Drive", "Dr", "Avenue", "Ave", "Road", "Rd", "Parkway", "Pkwy",
        "Court", "Ct", "Street", "St", "Boulevard", "Blvd", "Lane", "Ln",
        "Highway", "Hwy", "Apartment", "Apt", "Unit", "Suite", "Ste","Circle","Cir",
        "Extension", "EXT", "Ext","Place","Pl",
    ]
    
    if not ',' in address or address.count(',')==1:
        # 1. Check for directional keywords
        for keyword in directional_keywords:
            if keyword in address:
                parts = address.split()
                for i in range(len(parts)):
                    if parts[i] == keyword:
                        return " ".join(parts[:i+1])  # Include the keyword
                # If partial match (like "SW" attached to street), fallback to regex
                match = re.search(rf"(.*?\b{re.escape(keyword)}\b)", address)
                if match:
                    return match.group(1).strip()
    
        # 2. Check for street type keywords using regex
        for keyword in street_keywords:
            pattern = rf"(.*?\b{keyword}\b)"
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    
    
    # Case 1: If comma exists, split at first comma
    if ',' in address and not '.' in address:
        return address.split(',')[0].strip()
    
    if '.' in address and address.count(',') > 1:
        return address.split(',')[0].strip()
    if '.' in address:
        return address.split('.')[0].strip()
    # Fallback: return full address


    # 3. Fallback to full address if nothing matched
    return address.strip()

def normalize_address_keywords(address):
    replacements = {
        r'\bDrive\b': 'Dr',
        r'\bAvenue\b': 'Ave',
        r'\bRoad\b': 'Rd',
        r'\bParkway\b': 'Pkwy',
        r'\bCourt\b': 'Ct',
        r'\bStreet\b': 'St',
        r'\bBoulevard\b': 'Blvd',
        r'\bLane\b': 'Ln',
        r'\bHighway\b': 'Hwy',
        r'\bApartment\b': 'Apt',
        r'\bUnit\b': 'Unit',
        r'\bSuite\b': 'Ste',
        r'\bNortheast\b': 'NE',
        r'\bNorth East\b': 'NE',
        r'\bNorthwest\b': 'NW',
        r'\bNorth West\b': 'NW',
        r'\bSoutheast\b': 'SE',
        r'\bSouth East\b': 'SE',
        r'\bSouthwest\b': 'SW',
        r'\bSouth West\b': 'SW',
        r'\bPlace\b': 'PL',
        r'\bExtension\b': 'EXT',
        r'\bCircle\b': 'CIR',
        r'\bNorth\b': 'N',
        r'\bSouth\b': 'S',
        r'\bWest\b': 'W',
        r'\bEast\b': 'E'
    }

    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    address.replace('.','')
    return address.upper()

def get_property_info():

    engine = get_db_connection()
    # Convert result to a dictionary list
    parcel_details = get_parcel_numbers(engine)
    logger.info(f"COUNTS: {len(parcel_details)}")

    c=1
    failed_cases = []
    # --- Match and Extract LANDVAL ---
    for entry in parcel_details:

        try:

            # if not entry["parcel_id"]:
            #     raise ValueError(f"\n{c}.Parcel ID not exist for this case number {entry["case_number"]}")

            parcel_id = entry["parcel_id"]#.replace("-", "")
            case_number = entry["case_number"]
            amount_owed = entry["amount_owed"]
            property_address = entry["property_address"]
            deed_book_number = entry["deed_book_number"]
            deed_page_number = entry["deed_page_number"]
            # owner_name = ""
            logger.info(f"\n{c}.Processing case no {case_number}, parcel id {parcel_id}, property address: {property_address}, amount owed: {amount_owed}, deed book: {deed_book_number}, deed page: {deed_page_number}")
            county_id = case_number[-3:]

            logger.info(f"COUNTY ID: {county_id}")

            geojson_file = get_geojson(county_id)

            updated = False

            # First: Lookup with property address
            if property_address:
                logger.info("[MAPPING] with PROPERTY ADDRESS...")

                extracted_mailing_address = extract_mailing_address(property_address)
                logger.info(f"EXTRACTED STREET ADDRESS: {extracted_mailing_address}")
                normalized_address = normalize_address_keywords(extracted_mailing_address)

                if county_id in ['590','640','180']:
                    normalized_address = re.sub(r'^(\d+)(\s+)', r'\1  ', normalized_address)

                logger.info(f"NORMALIZED ADDRESS: {normalized_address}")

                #Step1: Try with Site Address
                site_address_lookup_data = map_with_site_address(geojson_file)
                if not isinstance(site_address_lookup_data, dict):
                    logger.warning("[LOOKUP FAILED] site_address_lookup_data is not a valid dictionary")

                logger.info("1.STARTED SITE ADD MAPPING....")
                siteadd_property_val = site_address_lookup_data.get(normalized_address)
                logger.info("ENDED SITE MAPPING!")
                
                if siteadd_property_val:
                    logger.info("SITE ADDRESS MATCHED!")

                    try:
                        updated = True
                        db_insert = update_db(siteadd_property_val, entry, engine)
                        if db_insert:
                            updated = True
                        else:
                            raise ValueError("DB UPDATE FAILED")
                        # Stop at first successful match
                    except Exception as e:
                        logger.error(f"Exception during DB update: {e}")
                else:
                    logger.warning("SITE ADDRESS NOT MATCHED")
                #Step2: Try with Mailing Address
                if not updated:
                    mailing_address_lookup_data = map_with_mailing_address(geojson_file)
                    
                    if not isinstance(mailing_address_lookup_data, dict):
                        logger.warning("[LOOKUP FAILED] mailing_address_lookup_data is not a valid dictionary")
                    
                    logger.info("2.MAPPING WITH MAILING ADDRESS....")
                    mailadd_property_val = mailing_address_lookup_data.get(normalized_address)
                    logger.info("ENDED MAIL MAPPING!")
                    
                    if mailadd_property_val:
                        logger.info("MAILING ADDRESS MATCHED!")
                        try:
                            updated = True
                            db_insert = update_db(mailadd_property_val, entry, engine)
                            if db_insert:
                                updated = True
                            else:
                                raise ValueError("DB UPDATE FAILED")
                            # Stop at first successful match
                        except Exception as e:
                            logger.error(f"Exception during DB update: {e}")
                if not updated:
                        logger.warning("[PROPERTY ADDRESS NOT MATCHED] in GeoJSON.")
            else:
                logger.warning("[PROPERTY ADDRESS IS EMPTY]")

            if not updated:
                # Second: Lookup with Parcel ID
                if parcel_id:
                    logger.info("3.[MAPPING] with Parcel ID...")
                    parcel_lookup_data = map_with_parcel_id(geojson_file)
                    time.sleep(2)

                    geojson_data = parcel_lookup_data.get('data','')
                    parcel_lookup = geojson_data.get('parcel_lookup','')
                    alt_parcel_lookup = geojson_data.get('alt_parcel_lookup','')

                    if parcel_id in parcel_lookup or parcel_id in alt_parcel_lookup:
                        property_val = parcel_lookup.get(parcel_id) or alt_parcel_lookup.get(parcel_id)
                        logger.info("PARCEL ID MATCHED!")

                        if property_val:
                            updated = True
                            db_insert = update_db(property_val, entry, engine)
                            if db_insert:   
                                updated = True
                            if not db_insert:
                                raise ValueError("DB UPDATE FAILED")

                    else:
                        logger.warning(f"\n[NOT FOUND]Parcel ID {parcel_id} in JSON")
                else:
                    logger.warning("PARCEL ID is Empty")

            if not updated:
                # Third: Lookup with Deed Book and Page Number
                if deed_book_number and deed_page_number:
                    logger.info("4.[MAPPING] with Deed Book and Page Number...")
                    book_page_lookup_data = map_with_book_page_no(geojson_file)
                    time.sleep(2)

                    if not isinstance(book_page_lookup_data, dict):
                        logger.warning("[LOOKUP FAILED] book_page_lookup_data is not a valid dictionary")

                    # Combine book and page number to match SOURCEREF format (e.g., "33649-924")
                    source_ref = f"{deed_book_number}-{deed_page_number}"
                    logger.info(f"Constructed SOURCEREF: {source_ref}")

                    book_page_property_val = book_page_lookup_data.get(source_ref)

                    if book_page_property_val:
                        logger.info("DEED BOOK AND PAGE NUMBER MATCHED!")
                        try:
                            updated = True
                            db_insert = update_db(book_page_property_val, entry, engine)
                            if db_insert:
                                updated = True
                            else:
                                raise ValueError("DB UPDATE FAILED")
                        except Exception as e:
                            logger.error(f"Exception during DB update: {e}")
                    else:
                        logger.warning(f"\n[NOT FOUND] Deed Book {deed_book_number} Page {deed_page_number} in JSON")
                else:
                    logger.warning("DEED BOOK OR PAGE NUMBER IS EMPTY")

            if not updated:
                logger.warning("PROPERTY ADDRESS, PARCEL ID, AND DEED BOOK/PAGE NOT MATCHED")
                failed_cases.append({"case_number": case_number, "failed_reason": 'PROPERTY ADDRESS, PARCEL ID, AND DEED BOOK/PAGE NOT MATCHED'})
            # if not updated:
            #     #Third: Lookup with Owner name
            #     if owner_name:
            #         print("Mapping with Owner name...")
            #         owner_name_lookup_data = map_with_owner_name(geojson_file)

            #         if owner_name in owner_name_lookup_data:
            #             owner_property_val = owner_name_lookup_data.get(owner_name)

            #             update_db(owner_property_val,entry,engine)
            #             return {"success":True}
            #     else:
        except Exception as e:
            logger.error(f"[PROPERTY EXTRACTION FAILED]: {str(e)}")

        c+=1
    
    logger.info(f"FAILED CASES: {failed_cases}")
    # Updating Failed Cases
    for case in failed_cases:
        metadata = MetaData(schema=SCHEMA_NAME)
        property_info = Table(
            "property_info", metadata,
            Column("case_number", String),
            Column("ncmap_updated", Boolean),
            Column("manual_review", Boolean),
            Column("manual_review_reason", String)
        )
        try:
            if case.get('case_number') and case.get('failed_reason'):
                with engine.begin() as conn:  # engine.begin() ensures commit at the end
                    updt_stmt = (
                        update(property_info)
                        .where(property_info.c.case_number == case['case_number'])
                        .values(
                            ncmap_updated=False,
                            manual_review=True,
                            manual_review_reason=case['failed_reason']
                        )
                    )
                    conn.execute(updt_stmt)
        except Exception as e:
            logger.error(f"Error updating failed case {case.get('case_number')}: {str(e)}")
    logger.info("FAILED EQUITY CASES UPDATED IN DB")


def update_db(property_val,entry,engine):

    metadata = MetaData(schema=SCHEMA_NAME)

    property_info = Table(
            "property_info", metadata,
            Column("case_number", String),
            Column("assessed_value", Integer),
            Column("parcel_or_tax_id", String),
            Column("ncmap_owner_name",String),
            Column("ncmap_owner_first_name",String),
            Column("ncmap_owner_last_name",String),
            Column("ncmap_owner_mailing_address",String),
            Column("equity",Integer),
            Column("skip_trace_status",String),
            Column("last_updated_at", Date),
            Column("ncmap_updated",Boolean),
            Column("equity_status",String),
            Column("ncmap_parcel_number",String),
            autoload_with=engine
    )

    tax_info = Table(
        "tax_info",metadata,
        Column("case_number",String),
        Column("equity",Integer),
        Column("last_updated_at", Date),
        Column("equity_status",String),
        Column("manual_review",String),
        autoload_with=engine
    )

    parcel_id = entry["parcel_id"]#.replace("-", "")
    case_number = entry["case_number"]
    amount_owed = entry["amount_owed"]

    extracted_parcel_id = property_val.get('PARNO','')
    assessed_value = property_val["PARVAL"]
    book_and_page_no = property_val["SOURCEREF"]
    st_addr_no = property_val["SADDNO"]
    st_name = property_val["SADDSTNAME"]
    st_addr_type = property_val["SADDSTTYP"]
    st_name_full = property_val["SADDSTR"]
    city = property_val["SCITY"]
    full_address = property_val["SITEADD"]
    owner_first_name =property_val["OWNFRST"]
    owner_last_name =property_val["OWNLAST"]
    owner_full_name =property_val["OWNNAME"]
    mailing_address = property_val["MAILADD"]
    
    logger.info(f"Case Number: {case_number}, Parcel ID: {parcel_id}")
    logger.info(f"ASSESSED VALUE: {assessed_value}")
    logger.info(f"Extracted parcel No: {extracted_parcel_id}")
    logger.info(f"BOOK NO/PAGE NO: {book_and_page_no}")
    logger.info(f"Street No         : {st_addr_no}")
    logger.info(f"Street Name       : {st_name}")
    logger.info(f"Street Full Name  : {st_name_full}")
    logger.info(f"Street Type       : {st_addr_type}")
    logger.info(f"Mailing Address   : {mailing_address}")
    logger.info(f"City              : {city}")
    logger.info(f"Full Address      : {full_address}")
    logger.info(f"Owner First Name  : {owner_first_name}")
    logger.info(f"Owner Last Name   : {owner_last_name}")
    logger.info(f"Owner Full Name   : {owner_full_name}")
    #Case type
    # with engine.connect() as conn:
    #     case_intake = Table("case_intake", metadata, autoload_with=engine)
    #     case_type_query =select(case_intake.c.extracted_case_type).where(case_intake.c.case_number==case_number)
    #     result = conn.execute(case_type_query).fetchone()

    #     # Extract the value into a single variable
    #     case_type = result[0] if result else None

    #     logger.info(f"[CASE TYPE] {case_type}")

    #     if case_number[2:4] == "SP":
    #         property_info_mortgage = Table("property_info",metadata,autoload_with=engine)
    #         mortgage_query = select(property_info_mortgage.c.mortgage_balance).where(property_info_mortgage.c.case_number==case_number)
    #         mortgage_result = conn.execute(mortgage_query).fetchone()
    #         mortgage_balance = mortgage_result[0] if mortgage_result else None

    #         logger.info(f"[MORTGAGE BALANCE]: {mortgage_balance}")

    #Equity Calculation
    equity = 0
    try:
            
        if not amount_owed or amount_owed == 0:
            raise ValueError(f"AMOUNT OWED NOT EXIST TO CALCULATE EQUITY")
        
        equity = assessed_value - amount_owed
        logger.info(f"[ASSESSED VALUE] : {assessed_value} [AMOUNT OWED]: {amount_owed} [EQUITY] : {equity}")
        
    except Exception as e:
        logger.info(f"[EQUITY CALCULATON FAILED] {str(e)}")

    #Categorizing Equity Status
    equity_status = None
    try:
        if equity is not None:
            if equity >= 100000:
                equity_status = "HIGH"
            elif equity >= 50000 and equity < 100000:
                equity_status = "MID"
            elif equity > 1000 and equity < 50000:
                equity_status = "LOW"
            else:
                equity_status = None
    except Exception as e:
        logger.info(f"[FAILED TO CATEGORIZE EQUITY VALUE] : {str(e)}")
                        
    logger.info(f"[EQUITY STATUS]: {equity_status}")

    #Update DB
    try:
        with engine.connect() as conn:
            update_property_info = update(property_info).where(property_info.c.case_number == case_number).values(
                assessed_value= assessed_value,
                ncmap_owner_name = owner_full_name,
                ncmap_owner_first_name = owner_first_name,
                ncmap_owner_last_name = owner_last_name,
                ncmap_owner_mailing_address = mailing_address,
                equity = equity,
                ncmap_updated = True,
                skip_trace_status = None,
                equity_status = equity_status,
                ncmap_parcel_number = extracted_parcel_id,
                last_updated_at = datetime.now()
            )

            conn.execute(update_property_info)

            update_tax_info = update(tax_info).where(tax_info.c.case_number == case_number).values(assessed_value=assessed_value,manual_review=False,equity=equity,equity_status = equity_status,last_updated_at = datetime.now())

            conn.execute(update_tax_info)

            conn.commit()

            logger.info("[DB UPDATED SUCCESSFULLY!]")
            return True
    except Exception as e:
        logger.error(f"[DB UPDATE FAILED]: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        get_property_info()
    except Exception as e:
        logger.error(f"[MAIN FUNCTION ERROR]: {str(e)}")
    finally:
        store_logs(LOG_FILE)