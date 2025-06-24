#!/usr/bin/env python3
import os
import psycopg2
import requests  
import json
import concurrent.futures
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Optional, Tuple, Set
import utils  
import configs 

# Airtable Configuration
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", " ")
AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT", " ")
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json"
}
AIRTABLE_API_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}" 

SECRET_ARN = os.environ.get("SecretArn")

# Concurrency settings
MAX_WORKERS = 5  # Number of concurrent threads for API calls
BATCH_SIZE = 10  # Airtable API batch size limit

def get_primary_key(conn, schema_name, cursor, table_name: str) -> str:
    """Get the primary key column name for a table"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
                LIMIT 1
            """, (schema_name, table_name))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                # Fallback to 'id' if no primary key is found
                print(f"No primary key found for {table_name}, using 'id' as default")
                return 'id'
    except Exception as e:
        print(f"Error finding primary key for {table_name}: {e}")
        return 'id'  # Default to 'id' column as fallback

def get_table_data(conn, schema_name, cursor, table_name: str) -> Tuple[List[Dict], List[Dict], str, List]:
    """Fetch data from PostgreSQL table with filter if specified"""
    TABLE_CONFIG = configs.TABLE_CONFIG.get(table_name, {})
    print("TABLE_CONFIG;;", TABLE_CONFIG)
    filter_column = TABLE_CONFIG.get("filter_column")
    filter_value = TABLE_CONFIG.get("filter_value")

    # Get primary key for the table
    primary_key = get_primary_key(conn, schema_name, cursor, table_name)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Get column information
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema_name, table_name))
        columns_info = cursor.fetchall()
        
        # Build query with filter if specified
        query = f'SELECT * FROM "{schema_name}".{table_name}'
        if filter_column and filter_value is not None:
            print(f"Applying filter: {filter_column} = {filter_value}")
            cursor.execute(f"{query} WHERE {filter_column} = %s", (filter_value,))
        else:
            print(f"No filter specified for {table_name}, fetching all rows")
            cursor.execute(query)
            
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} rows from {table_name}")
        
        # Get IDs of rows that should be deleted from Airtable (where filter condition is not met)
        rows_to_delete = []
        if filter_column:
            cursor.execute(f"""
                SELECT {primary_key} FROM "{schema_name}".{table_name} 
                WHERE {filter_column} IS NULL OR {filter_column} = %s
            """, (not filter_value,))
            rows_to_delete = cursor.fetchall()
            print(f"Found {len(rows_to_delete)} rows to delete based on filter condition")
        else:
            print(f"No filter condition for {table_name}, no rows will be deleted from Airtable")
            
        return list(rows), columns_info, primary_key, [row[primary_key] for row in rows_to_delete]

def get_airtable_tables() -> List[str]:
    """Get list of existing tables in Airtable"""
    try:
        response = requests.get(
            f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables",
            headers=AIRTABLE_HEADERS
        )
        response.raise_for_status()
        tables = response.json().get('tables', [])
        return [table.get('name') for table in tables]
    except Exception as e:
        print(f"Error fetching Airtable tables: {e}")
        return []

def get_airtable_fields(table_name: str) -> List[Dict]:
    """Get existing fields (columns) in an Airtable table"""
    try:
        response = requests.get(
            f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables",
            headers=AIRTABLE_HEADERS
        )
        response.raise_for_status()
        
        tables = response.json().get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                return table.get('fields', [])
        return []
    except Exception as e:
        print(f"Error fetching Airtable fields for table {table_name}: {e}")
        return []

def get_airtable_table_id(table_name: str) -> Optional[str]:
    """Get the Airtable table ID for a given table name"""
    try:
        response = requests.get(
            f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables",
            headers=AIRTABLE_HEADERS
        )
        response.raise_for_status()
        
        tables = response.json().get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                return table.get('id')
        return None
    except Exception as e:
        print(f"Error fetching Airtable table ID for {table_name}: {e}")
        return None

def map_pg_type_to_airtable(pg_type: str) -> Dict:
    """Map PostgreSQL data types to Airtable field types"""
    pg_type = pg_type.lower()
    if pg_type in ['boolean']:
        field_type = "checkbox"
        options = {
            "icon": "check",
            "color": "greenBright"
        }
    elif pg_type.startswith('character varying'):
        try:
            length = int(pg_type.split('(')[1].split(')')[0])
            if length > 100:
                field_type = "multilineText"
            else:
                field_type = "singleLineText"
        except (IndexError, ValueError):
            field_type = "singleLineText"  # Default if length not provided
        options = None
    elif pg_type in ['text']:
        field_type = "multilineText"
        options = None
    else:
        # Default to singleLineText for most other types
        field_type = "singleLineText"
        options = None
    result = {"type": field_type}
    if options is not None:
        result["options"] = options
    return result


def create_airtable_table(table_name: str, columns_info: List[Dict]) -> bool:
    """Create a new table in Airtable with matching schema"""
    fields = []
    
    for col in columns_info:
        col_name = col['column_name']
        pg_type = col['data_type']
        
        airtable_type = map_pg_type_to_airtable(pg_type)
        
        field = {
            "name": col_name,
            "type": airtable_type["type"],
        }
        
        if airtable_type["options"]:
            field["options"] = airtable_type["options"]
            
        fields.append(field)
    
    payload = {
        "tables": [
            {
                "name": table_name,
                "fields": fields
            }
        ]
    }
    
    try:
        response = requests.post(
            f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables",
            headers=AIRTABLE_HEADERS,
            json=payload
        )
        response.raise_for_status()
        print(f"Created table {table_name} in Airtable")
        return True
    except Exception as e:
        print(f"Error creating Airtable table {table_name}: {e}")
        if hasattr(e, 'response'):
            print(f"Response: {e.response.text}")
        return False

def add_missing_columns(table_name: str, existing_fields: List[Dict], columns_info: List[Dict]) -> bool:
    """Add missing columns to existing Airtable table"""
    existing_field_names = [field.get('name') for field in existing_fields]
    fields_to_add = []
    # Reserved field names that cannot be used in Airtable
    # reserved_names = ['id', 'createdTime', 'lastModifiedTime', 'Created Time', 'Last Modified Time']
    reserved_names = ['id']
    for col in columns_info:
        col_name = col['column_name']
        # Skip reserved field names
        if col_name in reserved_names:
            print(f"Skipping reserved field name: {col_name}")
            continue
            
        if col_name not in existing_field_names:
            pg_type = col['data_type']
            airtable_type = map_pg_type_to_airtable(pg_type)
            field = {
                "name": col_name,
                "type": airtable_type["type"],
            }
           
            # Only add options if they exist and are not empty
            if "options" in airtable_type and airtable_type["options"]:
                field["options"] = airtable_type["options"]
               
            fields_to_add.append(field)
            print("fields_to_add;;", fields_to_add)
   
    if not fields_to_add:
        print(f"No new columns to add for table {table_name}")
        return True
   
    # Find the table ID
    table_id = get_airtable_table_id(table_name)
    if not table_id:
        print(f"Could not find table ID for {table_name}")
        return False

    updated_fields = fields_to_add
    payload = {"fields": updated_fields}
    
    print(f"Number of fields in payload: {len(payload['fields'])}")
    print(f"New fields being added: {[f['name'] for f in fields_to_add]}")
    
    success_count = 0
    
    for field_to_add in updated_fields:
        try:
            response = requests.post(
                f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables/{table_id}/fields",
                headers=AIRTABLE_HEADERS,
                json=field_to_add
            )
            
            print(f"Response status for field '{field_to_add['name']}': {response.status_code}")
            
            if response.status_code == 200:
                print(f"Successfully added field: {field_to_add['name']}")
                success_count += 1
            else:
                print(f"Failed to add field '{field_to_add['name']}': {response.text}")
                
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error adding field '{field_to_add['name']}': {e}")
            print(f"Response: {e.response.text}")
            try:
                error_details = e.response.json()
                print(f"Error details: {error_details}")
            except:
                pass
        except Exception as e:
            print(f"Error adding field '{field_to_add['name']}': {e}")
    
    if success_count == len(fields_to_add):
        print(f"Successfully added all {success_count} new columns to table {table_name}")
        return True
    elif success_count > 0:
        print(f"Partially successful: added {success_count} out of {len(fields_to_add)} columns to table {table_name}")
        return True
    else:
        print(f"Failed to add any new columns to table {table_name}")
        return False

def clean_record_for_airtable(record: Dict) -> Dict:
    """Clean PostgreSQL record for Airtable compatibility"""
    clean_record = {}
    
    for key, value in record.items():
        # Skip None values
        if value is None:
            continue
            
        # Convert booleans
        if isinstance(value, bool):
            clean_record[key] = value
        # Convert lists and dicts to JSON strings
        elif isinstance(value, (list, dict)):
            clean_record[key] = json.dumps(value)
        # Handle date and datetime objects
        elif hasattr(value, 'isoformat'):
            clean_record[key] = value.isoformat()
        # Everything else as string
        else:
            clean_record[key] = str(value)
            
    return clean_record

def get_existing_records_from_airtable(table_name: str, primary_key: str) -> Dict[str, str]:
    """Get existing records from Airtable with their IDs mapped to their record IDs"""
    records_map = {}
    offset = None
    
    try:
        while True:
            url = f"{AIRTABLE_API_URL}/{table_name}"
            params = {}
            if offset:
                params['offset'] = offset
                
            response = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            
            for record in response_data.get('records', []):
                pg_id = record.get('fields', {}).get(primary_key)
                if pg_id:
                    records_map[str(pg_id)] = record.get('id')
            
            offset = response_data.get('offset')
            if not offset:
                break
                
        print(f"Found {len(records_map)} existing records in Airtable table {table_name}")
        return records_map
    except Exception as e:
        print(f"Error fetching existing records from Airtable: {e}")
        if hasattr(e, 'response'):
            print(f"Response: {e.response.text}")
        return {}

def process_batch(operation: str, table_name: str, batch: List, primary_key: str = None, existing_records: Dict = None) -> bool:
    """Process a batch of records for insertion, update, or deletion"""
    try:
        url = f"{AIRTABLE_API_URL}/{table_name}"
        
        if operation == "create":
            payload = {"records": batch}
            response = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
        elif operation == "update":
            payload = {"records": batch}
            response = requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
        elif operation == "delete":
            record_ids = [record_id for record_id in batch if record_id]
            if not record_ids:
                return True
                
            params = {"records[]": record_ids}
            response = requests.delete(url, headers=AIRTABLE_HEADERS, params=params)
        else:
            print(f"Unknown operation: {operation}")
            return False
            
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Error processing {operation} batch for {table_name}: {e}")
        if hasattr(e, 'response'):
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
        return False

def sync_data_to_airtable(table_name: str, pg_data: List[Dict], primary_key: str, ids_to_delete: List) -> bool:
    """Sync PostgreSQL data to Airtable with update and delete capabilities"""
    # Get existing records from Airtable to determine create vs update
    existing_records = get_existing_records_from_airtable(table_name, primary_key)
    
    # Prepare records for create, update, and delete
    records_to_create = []
    records_to_update = []
    
    for record in pg_data:
        clean_record = clean_record_for_airtable(record)
        if not clean_record:  # Skip empty records
            continue
            
        # Check if this record exists in Airtable
        record_key = str(record[primary_key])
        if record_key in existing_records:
            # Update existing record
            records_to_update.append({
                "id": existing_records[record_key],
                "fields": clean_record
            })
        else:
            # Create new record
            records_to_create.append({
                "fields": clean_record
            })
    
    # Get the IDs of records to delete
    records_to_delete = []
    for pg_id in ids_to_delete:
        airtable_id = existing_records.get(str(pg_id))
        if airtable_id:
            records_to_delete.append(airtable_id)
    
    # Process in batches using ThreadPoolExecutor for concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        # Process create batches
        for i in range(0, len(records_to_create), BATCH_SIZE):
            batch = records_to_create[i:i+BATCH_SIZE]
            futures.append(
                executor.submit(process_batch, "create", table_name, batch)
            )
        
        # Process update batches
        for i in range(0, len(records_to_update), BATCH_SIZE):
            batch = records_to_update[i:i+BATCH_SIZE]
            futures.append(
                executor.submit(process_batch, "update", table_name, batch)
            )
        
        # Process delete batches
        for i in range(0, len(records_to_delete), BATCH_SIZE):
            batch = records_to_delete[i:i+BATCH_SIZE]
            futures.append(
                executor.submit(process_batch, "delete", table_name, batch)
            )
        
        # Wait for all futures to complete
        results = [future.result() for future in futures]
        
        print(f"Created {len(records_to_create)} records in {table_name}")
        print(f"Updated {len(records_to_update)} records in {table_name}")
        print(f"Deleted {len(records_to_delete)} records from {table_name}")
        
        return all(results)

def process_table(table_name: str):
    """Process a single table from PostgreSQL to Airtable"""
    try:
        print(f"\n=== Processing table: {table_name} ===")
        # Connect to PostgreSQL
        conn, cursor, schema_name = utils.dbConnection(SECRET_ARN)
        
        # Get data and column info from PostgreSQL
        pg_data, columns_info, primary_key, ids_to_delete = get_table_data(conn, schema_name, cursor, table_name)
        print(f"Found {len(pg_data)} records in PostgreSQL table {table_name}")
        
        # Get existing tables in Airtable
        airtable_tables = get_airtable_tables()
        
        # Create table if it doesn't exist
        if table_name not in airtable_tables:
            print(f"Table {table_name} not found in Airtable. Creating...")
            create_airtable_table(table_name, columns_info)
        else:
            print(f"Table {table_name} already exists in Airtable")
            
            # Get existing fields and add any missing ones
            existing_fields = get_airtable_fields(table_name)
            add_missing_columns(table_name, existing_fields, columns_info)
        
        # Sync data (create, update, delete)
        print(f"Syncing data for {table_name}...")
        sync_success = sync_data_to_airtable(table_name, pg_data, primary_key, ids_to_delete)
        
        if sync_success:
            print(f"Sync completed successfully for {table_name}!")
        else:
            print(f"Sync completed with some errors for {table_name}")
        
        return sync_success
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def lambda_handler(event,context):
    """Main function to sync PostgreSQL data to Airtable for all configured tables"""
    print("Starting PostgreSQL to Airtable sync process...")
    # Process all tables in the config
    results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(configs.TABLE_CONFIG)) as executor:
        # Submit jobs for each table
        future_to_table = {
            executor.submit(process_table, table_name): table_name
            for table_name in configs.TABLE_CONFIG.keys()
        }
        
        # Collect results
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                success = future.result()
                results[table_name] = "Success" if success else "Partial failure"
            except Exception as e:
                print(f"Error processing table {table_name}: {e}")
                results[table_name] = f"Failed: {str(e)}"
    
    # Print summary
    print("\n=== Sync Summary ===")
    for table, result in results.items():
        print(f"{table}: {result}")
    print("Sync process completed!") 