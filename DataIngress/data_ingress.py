import os
from dotenv import load_dotenv
import snowflake.connector
import sqlalchemy
import pandas as pd
from tqdm import tqdm
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import re
import traceback
from ingress_cleaning import DataCleaner 


# Load environment variables
load_dotenv()

# Configure logging
def setup_logging():
    """Configure separate loggers for summary and detailed logging"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Summary logger
    summary_logger = logging.getLogger('summary')
    summary_logger.setLevel(logging.INFO)
    summary_handler = logging.FileHandler(f'logs/transfer_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    summary_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    summary_logger.addHandler(summary_handler)
    summary_logger.addHandler(logging.StreamHandler())
    
    # Detailed logger
    detail_logger = logging.getLogger('detail')
    detail_logger.setLevel(logging.DEBUG)
    detail_handler = logging.FileHandler(f'logs/transfer_detail_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    detail_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    detail_logger.addHandler(detail_handler)
    
    return summary_logger, detail_logger

# Global loggers
summary_log, detail_log = setup_logging()

class DatabaseConnector:
    """Handle database connections and connection verification"""
    def __init__(self):
        self.snow_conn = self._create_snowflake_connection()
        self.pg_conn = self._create_postgres_connection()
    
    def _create_snowflake_connection(self):
        """Create and verify Snowflake connection"""
        try:
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database='NORSE_STAGING', 
                authenticator='snowflake'
            )
            self._verify_snowflake_access(conn)
            return conn
        except Exception as e:
            summary_log.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def _create_postgres_connection(self):
        """Create PostgreSQL connection"""
        try:
            return sqlalchemy.create_engine(
                f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
                f"@localhost:{os.getenv('TUNNEL_PORT')}/{os.getenv('POSTGRES_DB')}",
                connect_args={'connect_timeout': 10}
            )
        except Exception as e:
            summary_log.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    
    def _verify_snowflake_access(self, conn):
        """Verify Snowflake connection and access rights"""
        cur = conn.cursor()
        try:
            cur.execute("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
            role, database, warehouse = cur.fetchone()
            detail_log.info(f"Connected to Snowflake as Role: {role}, Database: {database}, Warehouse: {warehouse}")
            
            # Verify we're connected to the correct database
            if database != 'NORSE_STAGING':
                raise Exception(f"Connected to wrong database: {database}. Expected: NORSE_STAGING")
                
            # Verify schemas exist in NORSE_STAGING
            cur.execute("SHOW SCHEMAS IN DATABASE NORSE_STAGING")
            schemas = [row[1] for row in cur.fetchall()]
            for schema in ['WA211', 'WITHINREAC', 'WHATCOMCOU']:
                if schema not in schemas:
                    detail_log.warning(f"Schema {schema} not found in NORSE_STAGING: {', '.join(schemas)}")
        finally:
            cur.close()

class TypeConverter:
    """Handle type conversions between Snowflake and PostgreSQL"""
    
    @staticmethod
    def convert_snowflake_to_pg_type(snow_type: str) -> str:
        """Convert Snowflake data type to PostgreSQL type"""
        base_type = snow_type.split('(')[0].upper()
        
        # Handle VARCHAR types
        if 'VARCHAR' in snow_type.upper():
            return 'TEXT'
        
        # Handle NUMBER types with specific precision handling
        if base_type == 'NUMBER':
            if '(' in snow_type:
                precision_scale = re.search(r'\((\d+),?(\d+)?\)', snow_type)
                if precision_scale:
                    precision = int(precision_scale.group(1))
                    scale = int(precision_scale.group(2) or 0)
                    # For integers (scale = 0)
                    if scale == 0:
                        if precision <= 4:
                            return 'SMALLINT'
                        elif precision <= 9:
                            return 'INTEGER'
                        elif precision <= 18:
                            return 'BIGINT'
                        else:
                            return f'NUMERIC({precision})'
                    # For decimals
                    return f'NUMERIC({precision},{scale})'
            return 'NUMERIC'  # Default when no precision specified
        
        type_mapping = {
            'TEXT': 'TEXT',
            'INTEGER': 'INTEGER',
            'FLOAT': 'DOUBLE PRECISION',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'TIMESTAMP_NTZ': 'TIMESTAMP',
            'TIMESTAMP_TZ': 'TIMESTAMP WITH TIME ZONE',
            'TIMESTAMP_LTZ': 'TIMESTAMP WITH TIME ZONE',
            'VARIANT': 'JSONB',
            'ARRAY': 'JSONB',
            'OBJECT': 'JSONB'
        }
        
        return type_mapping.get(base_type, 'TEXT')
    
    @staticmethod
    def handle_null_value(value: Any, target_type: str) -> Optional[Any]:
        """Convert NULL/None values based on target PostgreSQL type"""
        # Handle NULL values
        if pd.isna(value) or value in {'None', 'NULL', 'null', '', 'NaN'}:
            return None
        
        try:
            # Handle boolean values
            if target_type == 'boolean':
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    value_lower = value.lower().strip()
                    if value_lower in {'true', 't', 'yes', 'y', '1'}:
                        return True
                    if value_lower in {'false', 'f', 'no', 'n', '0'}:
                        return False
                    return None
                if isinstance(value, (int, float)):
                    return bool(value)
                return None
            
            # Handle numeric values
            if any(t in target_type.lower() for t in ['numeric', 'integer', 'smallint', 'bigint']):
                if isinstance(value, (int, float)):
                    return value
                try:
                    if 'integer' in target_type.lower() or 'smallint' in target_type.lower() or 'bigint' in target_type.lower():
                        return int(float(value))
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            # Handle timestamp values
            if 'timestamp' in target_type.lower():
                if isinstance(value, (datetime, pd.Timestamp)):
                    return value
                if isinstance(value, str):
                    try:
                        return pd.to_datetime(value)
                    except (ValueError, TypeError):
                        return None
                return None
            
            # Handle JSON/JSONB values
            if target_type.lower() in ('json', 'jsonb'):
                if isinstance(value, (dict, list)):
                    return value
                if isinstance(value, str):
                    try:
                        import json
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return None
                return None
            
            # Default string conversion
            return str(value) if value is not None else None
            
        except Exception as e:
            detail_log.warning(f"Error converting value '{value}' to type {target_type}: {str(e)}")
            return None
        
class TableManager:
    """Handle table operations including metadata and creation"""
    def __init__(self, snow_conn, pg_conn):
        self.snow_conn = snow_conn
        self.pg_conn = pg_conn
        self.type_converter = TypeConverter()
    
    def get_table_metadata(self, schema: str, table: str) -> Dict:
        """Get table metadata using Snowflake's SHOW commands"""
        cur = self.snow_conn.cursor()
        try:
            # Get column information using SHOW COLUMNS
            cur.execute(f"SHOW COLUMNS IN TABLE {schema}.{table}")
            columns = []
            column_names = set()
            
            for row in cur.fetchall():
                name = row[2]  # Column name
                data_type = row[3]  
                nullable = row[7].upper() == 'Y'
                
                if name in column_names:
                    detail_log.warning(f"Duplicate column name found: {name} in {schema}.{table}")
                    continue
                column_names.add(name)
                
                pg_type = self.type_converter.convert_snowflake_to_pg_type(data_type)
                columns.append({
                    'name': name,
                    'data_type': data_type,
                    'nullable': nullable,
                    'pg_type': pg_type
                })

            # Check for ID column
            if 'ID' not in column_names:
                raise ValueError(f"Table {schema}.{table} does not have an ID column")
                
            # Set ID as primary key
            primary_keys = ['ID']
            detail_log.info(f"Set ID as primary key for {schema}.{table}")
                
            return {'columns': columns, 'primary_keys': primary_keys}
            
        except Exception as e:
            detail_log.error(f"Error getting metadata for {schema}.{table}: {str(e)}")
            raise
        finally:
            cur.close()
    
    def create_table(self, schema: str, table: str, metadata: Dict):
        """Create table in PostgreSQL with improved data quality"""
        columns = []
        for col in metadata['columns']:
            # Make ID column non-nullable and a primary key, all other columns nullable
            if col['name'] == 'ID':
                columns.append(f"{col['name']} {col['pg_type']} NOT NULL")
            else:
                columns.append(f"{col['name']} {col['pg_type']} NULL")
        
        # Add primary key constraint on ID
        pk_clause = ", PRIMARY KEY (ID)"
        
        create_table_sql = f"""
            CREATE TABLE {schema.lower()}.{table.lower()} (
                {', '.join(columns)}
                {pk_clause}
            )
        """
        
        with self.pg_conn.begin() as trans:
            trans.execute(sqlalchemy.text(f"CREATE SCHEMA IF NOT EXISTS {schema.lower()}"))
            trans.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {schema.lower()}.{table.lower()}"))
            trans.execute(sqlalchemy.text(create_table_sql))
            detail_log.info(f"Created table {schema.lower()}.{table.lower()} with ID as primary key")

class DataTransfer:
    """Handle the data transfer process"""
    def __init__(self, snow_conn, pg_conn):
        self.snow_conn = snow_conn
        self.pg_conn = pg_conn
        self.table_manager = TableManager(snow_conn, pg_conn)
        self.type_converter = TypeConverter()
        self.data_cleaner = DataCleaner(pg_conn)
        self.retry_attempts = 3
        self.chunk_size = 5000
    
    def _process_chunk(self, df: pd.DataFrame, metadata: Dict, schema: str, table: str) -> Tuple[pd.DataFrame, List[Dict]]:
            """Process a chunk of data with proper type conversion and cleaning"""
            try:
                # First apply type conversion
                for col in df.columns:
                    col_meta = next(c for c in metadata['columns'] if c['name'].upper() == col.upper())
                    if not any(t in col.lower() for t in ['uuid']):  # Skip UUID columns for type conversion
                        df[col] = df[col].apply(lambda x: self.type_converter.handle_null_value(x, col_meta['pg_type']))
                
                # Then apply data cleaning
                df, cleaning_records = self.data_cleaner.clean_dataframe(df, metadata, schema, table)
                
                return df, cleaning_records
            except Exception as e:
                # Enhanced error logging
                detail_log.error(f"""
                    Error processing chunk for {schema}.{table}:
                    Error type: {type(e).__name__}
                    Error message: {str(e)}
                    DataFrame info:
                    {df.info()}
                    DataFrame head:
                    {df.head()}
                    Metadata:
                    {metadata}
                """)
                raise

    def transfer_table(self, schema: str, table: str) -> bool:
        """Transfer table data from Snowflake to PostgreSQL"""
        schema_name = schema.split('.')[-1]
        table_key = f"{schema_name}.{table}"
        start_time = datetime.now()
        
        try:
            # Get metadata and create table
            metadata = self.table_manager.get_table_metadata(schema_name, table)
            self.table_manager.create_table(schema_name, table, metadata)
            
            # Get row count and prepare for transfer
            snow_cur = self.snow_conn.cursor()
            snow_cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table}")
            total_rows = snow_cur.fetchone()[0]
            
            if total_rows == 0:
                summary_log.info(f"Table {table_key} is empty, skipping transfer")
                return True
            
            # Get column names and prepare query
            columns = [col['name'] for col in metadata['columns']]
            select_query = f"SELECT {', '.join(columns)} FROM {schema_name}.{table}"
            
            # Transfer data in chunks
            snow_cur.execute(select_query)
            
            with tqdm(total=total_rows, desc=f"Transferring {table_key}") as pbar:
                while True:
                    chunk = snow_cur.fetchmany(self.chunk_size)
                    if not chunk:
                        break
                    
                    # Convert chunk to DataFrame
                    df = pd.DataFrame(chunk, columns=columns)
                    
                    try:
                        processed_df, cleaning_records = self._process_chunk(
                            df.copy(), 
                            metadata,
                            schema,
                            table
                        )
                        
                        insert_sql = f"""
                            INSERT INTO {schema.lower()}.{table.lower()} 
                            ({', '.join(col.lower() for col in columns)})
                            VALUES ({', '.join([':' + col for col in columns])})
                        """
                        
                        with self.pg_conn.begin() as conn:
                            try:
                                conn.execute(
                                    sqlalchemy.text(insert_sql),
                                    processed_df.to_dict('records')
                                )
                            except Exception as e:
                                detail_log.error(f"""
                                    SQL execution failed for {table_key}:
                                    Error type: {type(e).__name__}
                                    Error message: {str(e)}
                                    SQL: {insert_sql}
                                    First row sample:
                                    {processed_df.iloc[0].to_dict() if not processed_df.empty else 'Empty DataFrame'}
                                    DataFrame info:
                                    {processed_df.info()}
                                """)
                                raise
                            
                            # Record any cleaning operations
                            if cleaning_records:
                                self.data_cleaner.record_cleaning(cleaning_records)
                                detail_log.info(f"Recorded {len(cleaning_records)} cleaning operations for chunk in {table_key}")
                    
                    except Exception as e:
                        detail_log.error(f"""
                            Failed to process chunk for {table_key}:
                            Error type: {type(e).__name__}
                            Error message: {str(e)}
                            Traceback:
                            {traceback.format_exc()}
                        """)
                        raise
                    
                    pbar.update(len(chunk))
            
            # Verify transfer
            with self.pg_conn.connect() as conn:
                result = conn.execute(sqlalchemy.text(
                    f"SELECT COUNT(*) FROM {schema.lower()}.{table.lower()}"
                ))
                pg_count = result.scalar()
            
            if total_rows != pg_count:
                raise ValueError(f"Row count mismatch in {table_key}! Snowflake: {total_rows}, PostgreSQL: {pg_count}")
            
            duration = (datetime.now() - start_time).total_seconds()
            summary_log.info(f"Successfully transferred {table_key}: {total_rows} rows in {duration:.2f} seconds")
            return True
            
        except Exception as e:
            detail_log.error(f"Failed to transfer {table_key}: {str(e)}")
            summary_log.error(f"Failed to transfer {table_key}")
            return False
        finally:
            snow_cur.close()
   
def main():
    """Main execution function"""
    start_time = datetime.now()
    summary_log.info("Starting data transfer process")
    
    try:
        # Initialize connections
        db_connector = DatabaseConnector()
        transfer_manager = DataTransfer(db_connector.snow_conn, db_connector.pg_conn)
        
        # Get schemas to process
        schemas = ['WA211', 'WITHINREAC', 'WHATCOMCOU']
        failed_tables = []
        successful_tables = []
        
        # Process each schema
        for schema in schemas:
            summary_log.info(f"Processing schema: {schema}")
            
            # Get tables in schema using SHOW TABLES command
            snow_cur = db_connector.snow_conn.cursor()
            snow_cur.execute(f"SHOW TABLES IN SCHEMA {schema}")
            tables = [row[1] for row in snow_cur.fetchall()]
            snow_cur.close()
            
            summary_log.info(f"Found {len(tables)} tables in {schema}")
            
            # Transfer each table
            for table in tables:
                if transfer_manager.transfer_table(f"{schema}", table):
                    successful_tables.append(f"{schema}.{table}")
                else:
                    failed_tables.append(f"{schema}.{table}")
        
        # Print summary
        duration = (datetime.now() - start_time).total_seconds()
        summary_log.info("\nTransfer Summary:")
        summary_log.info("-" * 80)
        summary_log.info(f"Total duration: {duration:.2f} seconds")
        summary_log.info(f"Total tables attempted: {len(successful_tables) + len(failed_tables)}")
        summary_log.info(f"Successfully transferred: {len(successful_tables)}")
        summary_log.info(f"Failed transfers: {len(failed_tables)}")
        
        if failed_tables:
            summary_log.info("\nFailed Tables:")
            for table in failed_tables:
                summary_log.info(f"- {table}")
        
    except Exception as e:
        summary_log.error(f"Transfer process failed: {str(e)}")
        raise
    finally:
        # Close connections
        db_connector.snow_conn.close()

if __name__ == "__main__":
    main()