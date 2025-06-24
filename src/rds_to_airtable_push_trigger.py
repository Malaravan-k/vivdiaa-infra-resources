import requests
import json
import concurrent.futures
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Optional, Tuple, Set
import utils 
import configs
import os

# Airtable Configuration
AIRTABLE_BASE_ID = "appanUA6rMslh7laz"
AIRTABLE_PAT = "patCZax01Ca7ZKPbX.3938b0d6fcfacae03f6e88bc75ac42e119f5feb226dd1465828d86f1ba4c4443"
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

def get_table_data_with_filter(conn, schema_name, cursor, source_table: str, airtable_table: str, filter_config: Dict) -> Tuple[List[Dict], List[Dict], str, List]:
    """Fetch data from PostgreSQL table with filter based on configuration"""
    filter_column = filter_config.get("filter_column")
    filter_value = filter_config.get("filter_value")
    
    print(f"Processing {source_table} -> {airtable_table} with filter: {filter_column} = {filter_value}")
    
    # Get primary key for the source table
    primary_key = get_primary_key(conn, schema_name, cursor, source_table)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Get column information from source table
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (schema_name, source_table))
        columns_info = cursor.fetchall()
        
        # Build query with filter
        query = f'SELECT * FROM "{schema_name}".{source_table}'
        if filter_column and filter_value is not None:
            print(f"Applying filter: {filter_column} = {filter_value}")
            cursor.execute(f"{query} WHERE {filter_column} = %s", (filter_value,))
        else:
            print(f"No filter specified for {airtable_table}, fetching all rows from {source_table}")
            cursor.execute(query)
            
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} rows from {source_table} for {airtable_table}")
        
        # Get IDs of rows that should be deleted from Airtable 
        # (where filter condition is not met or records no longer exist)
        rows_to_delete = []
        if filter_column and filter_value is not None:
            # Find records that don't match the current filter
            cursor.execute(f"""
                SELECT {primary_key} FROM "{schema_name}".{source_table} 
                WHERE {filter_column} IS NULL OR {filter_column} != %s
            """, (filter_value,))
            rows_to_delete = cursor.fetchall()
            print(f"Found {len(rows_to_delete)} rows to delete from {airtable_table} based on filter condition")
        
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
        
        if "options" in airtable_type and airtable_type["options"]:
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
            print(f"Adding field: {field_to_add}")
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

def process_table_mapping(source_table: str, airtable_table: str, filter_config: Dict):
    """Process mapping from a single PostgreSQL table to an Airtable table with filtering"""
    try:
        print(f"\n=== Processing mapping: {source_table} -> {airtable_table} ===")
        # Connect to PostgreSQL
        conn, cursor, schema_name = utils.dbConnection(SECRET_ARN)
        
        # Get data and column info from PostgreSQL with filter
        pg_data, columns_info, primary_key, ids_to_delete = get_table_data_with_filter(
            conn, schema_name, cursor, source_table, airtable_table, filter_config
        )
        print(f"Found {len(pg_data)} records in PostgreSQL table {source_table} for {airtable_table}")
        
        # Get existing tables in Airtable
        airtable_tables = get_airtable_tables()
        
        # Create table if it doesn't exist
        if airtable_table not in airtable_tables:
            print(f"Table {airtable_table} not found in Airtable. Creating...")
            create_airtable_table(airtable_table, columns_info)
        else:
            print(f"Table {airtable_table} already exists in Airtable")
            
            # Get existing fields and add any missing ones
            existing_fields = get_airtable_fields(airtable_table)
            add_missing_columns(airtable_table, existing_fields, columns_info)
        
        # Sync data (create, update, delete)
        print(f"Syncing data for {airtable_table}...")
        sync_success = sync_data_to_airtable(airtable_table, pg_data, primary_key, ids_to_delete)
        
        if sync_success:
            print(f"Sync completed successfully for {airtable_table}!")
        else:
            print(f"Sync completed with some errors for {airtable_table}")
        
        return sync_success
    except Exception as e:
        print(f"Error processing mapping {source_table} -> {airtable_table}: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def sync_postgres_to_airtable(event, context):
    """Main function to sync PostgreSQL data to Airtable for all configured table mappings"""
    print("Starting PostgreSQL to Airtable sync process...")
    
    # Process all table mappings in the config
    results = {}
    
    # Create a list of all mappings to process
    mappings_to_process = []
    for mapping_key, mapping_config in configs.TABLE_MAPPINGS.items():
        source_table = mapping_config["source_table"]
        airtable_table = mapping_config["airtable_table"]
        filter_config = mapping_config.get("filter", {})
        mappings_to_process.append((mapping_key, source_table, airtable_table, filter_config))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(mappings_to_process)) as executor:
        # Submit jobs for each mapping
        future_to_mapping = {
            executor.submit(process_table_mapping, source_table, airtable_table, filter_config): mapping_key
            for mapping_key, source_table, airtable_table, filter_config in mappings_to_process
        }
        
        # Collect results
        for future in concurrent.futures.as_completed(future_to_mapping):
            mapping_key = future_to_mapping[future]
            try:
                success = future.result()
                results[mapping_key] = "Success" if success else "Partial failure"
            except Exception as e:
                print(f"Error processing mapping {mapping_key}: {e}")
                results[mapping_key] = f"Failed: {str(e)}"
    
    # Print summary
    print("\n=== Sync Summary ===")
    for mapping, result in results.items():
        print(f"{mapping}: {result}")
    print("Sync process completed!")


