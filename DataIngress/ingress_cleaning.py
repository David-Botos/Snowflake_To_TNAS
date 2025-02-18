import uuid
import pandas as pd
import sqlalchemy
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime
import logging

# Get logger instance
detail_log = logging.getLogger('detail')

class CleaningOperation:
    """Base class for cleaning operations"""
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        
    def clean(self, value: Any, context: Dict) -> Tuple[Any, Optional[Dict]]:
        """
        Clean a value and return cleaning record if changed
        
        Args:
            value: The value to clean
            context: Dict containing schema_name, table_name, row_identifier
            
        Returns:
            Tuple of (cleaned_value, cleaning_record or None)
        """
        raise NotImplementedError("Subclasses must implement clean method")

class UUIDCleaner(CleaningOperation):
    """Clean and validate UUID values"""
    
    def is_valid_uuid(self, val: str) -> bool:
        """Check if a string is a valid UUID v4"""
        if not val:
            return False
        
        try:
            parsed = uuid.UUID(val)
            return parsed.version == 4
        except (ValueError, AttributeError, TypeError):
            return False
    
    def clean(self, value: Any, context: Dict) -> Tuple[str, Optional[Dict]]:
        is_primary_key = context.get('is_primary_key', False)
        
        # Handle null values
        if pd.isna(value) or value is None:
            new_uuid = str(uuid.uuid4())
            return new_uuid, {
                'schema_name': context['schema_name'],
                'table_name': context['table_name'],
                'original_value': None,
                'new_value': new_uuid,
                'row_identifier': context.get('row_identifier', new_uuid),
                'cleaning_operation': 'uuid_replacement',
                'cleaning_reason': 'null_uuid_primary_key' if is_primary_key else 'null_uuid'
            }
        
        # Convert to string if not already
        val_str = str(value).strip().lower()
        
        # Check if valid UUID v4
        if self.is_valid_uuid(val_str):
            return val_str, None
        
        # Generate new UUID v4
        new_uuid = str(uuid.uuid4())
        return new_uuid, {
            'schema_name': context['schema_name'],
            'table_name': context['table_name'],
            'original_value': val_str,
            'new_value': new_uuid,
            'row_identifier': context['row_identifier'],
            'cleaning_operation': 'uuid_replacement',
            'cleaning_reason': 'invalid_uuid_format'
        }

class DataCleaner:
    """Manage data cleaning operations during ingress"""
    
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.setup_cleaning_table()
        
        # Register cleaning operations
        self.uuid_cleaner = UUIDCleaner(pg_conn)
        
    def setup_cleaning_table(self):
        """Create the cleaned_on_ingress tracking table if it doesn't exist"""
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS public.cleaned_on_ingress (
                id SERIAL PRIMARY KEY,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                original_value TEXT,
                new_value TEXT,
                row_identifier TEXT NOT NULL,
                cleaning_operation TEXT NOT NULL,
                cleaning_reason TEXT NOT NULL,
                cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """
        with self.pg_conn.begin() as conn:
            conn.execute(sqlalchemy.text(create_table_sql))
            detail_log.info("Ensured cleaned_on_ingress table exists")
    
    def clean_dataframe(self, df: pd.DataFrame, metadata: Dict, schema: str, table: str) -> Tuple[pd.DataFrame, List[Dict]]:
        """Clean a dataframe and track changes"""
        cleaning_records = []
        primary_keys = [pk.lower() for pk in metadata['primary_keys']]
        
        # First pass: Clean primary key columns to ensure we have valid IDs
        for pk in primary_keys:
            col_meta = next(c for c in metadata['columns'] if c['name'].lower() == pk.lower())
            
            # Clean primary key column
            for idx, row in df.iterrows():
                context = {
                    'schema_name': schema,
                    'table_name': table,
                    'row_identifier': str(row[pk]) if not pd.isna(row[pk]) else None,
                    'is_primary_key': True
                }
                
                new_val, cleaning_record = self.uuid_cleaner.clean(row[pk], context)
                df.at[idx, pk] = new_val
                if cleaning_record:
                    cleaning_records.append(cleaning_record)
        
        # Second pass: Clean other UUID columns
        for col in df.columns:
            if col.lower() in primary_keys:
                continue
                
            col_meta = next(c for c in metadata['columns'] if c['name'].upper() == col.upper())
            
            if (col.lower().endswith('_uuid') or col.lower() == 'uuid' or 
                'uuid' in col_meta['data_type'].lower()):
                
                for idx, row in df.iterrows():
                    context = {
                        'schema_name': schema,
                        'table_name': table,
                        'row_identifier': str(row[primary_keys[0]]),
                        'is_primary_key': False
                    }
                    
                    new_val, cleaning_record = self.uuid_cleaner.clean(row[col], context)
                    df.at[idx, col] = new_val
                    if cleaning_record:
                        cleaning_records.append(cleaning_record)
        
        return df, cleaning_records
    
    def record_cleaning(self, cleaning_records: List[Dict]):
        """Record cleaning operations in the tracking table"""
        if not cleaning_records:
            return
            
        insert_sql = """
            INSERT INTO public.cleaned_on_ingress 
            (schema_name, table_name, original_value, new_value, 
             row_identifier, cleaning_operation, cleaning_reason)
            VALUES 
            (:schema_name, :table_name, :original_value, :new_value,
             :row_identifier, :cleaning_operation, :cleaning_reason)
        """
        
        with self.pg_conn.begin() as conn:
            conn.execute(sqlalchemy.text(insert_sql), cleaning_records)
            detail_log.info(f"Recorded {len(cleaning_records)} cleaning operations")