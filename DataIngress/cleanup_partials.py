import os
from dotenv import load_dotenv
import snowflake.connector
import sqlalchemy
import pandas as pd

# Load environment variables
load_dotenv()

print("TNAS PostgreSQL Cleanup Script")
print("------------------------------")
print("This script will identify and optionally remove incomplete tables from the TNAS PostgreSQL database.")
print("It will NOT modify any data in Snowflake - Snowflake is only used for row count comparison.")
print("------------------------------\n")

# PostgreSQL connection (TNAS)
pg_conn = sqlalchemy.create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:{os.getenv('TUNNEL_PORT')}/{os.getenv('POSTGRES_DB')}",
    connect_args={'connect_timeout': 10}
)

# Snowflake connection (read-only comparison)
snow_conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    authenticator='snowflake'
)

def get_schema_tables(schema):
    """Get all tables in a schema from both Snowflake and PostgreSQL"""
    # Get Snowflake tables (for comparison only)
    snow_cur = snow_conn.cursor()
    snow_cur.execute(f"SHOW TABLES IN SCHEMA {schema}")
    snowflake_tables = set(row[1].lower() for row in snow_cur.fetchall())
    snow_cur.close()
    
    # Get PostgreSQL tables (these are the ones we'll potentially drop)
    with pg_conn.connect() as conn:
        result = conn.execute(sqlalchemy.text(
            f"""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = :schema
            """
        ), {'schema': schema.lower()})
        postgres_tables = set(row[0].lower() for row in result)
    
    return snowflake_tables, postgres_tables

def get_row_counts(schema, table):
    """Get row counts from both databases for comparison"""
    # Get Snowflake count (for comparison only)
    snow_cur = snow_conn.cursor()
    snow_cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    snowflake_count = snow_cur.fetchone()[0]
    snow_cur.close()
    
    # Get PostgreSQL count (from TNAS)
    with pg_conn.connect() as conn:
        result = conn.execute(sqlalchemy.text(
            f"SELECT COUNT(*) FROM {schema.lower()}.{table.lower()}"
        ))
        postgres_count = result.scalar()
    
    return snowflake_count, postgres_count

def cleanup_schema(schema):
    """Clean up partially transferred tables in a schema on TNAS PostgreSQL"""
    print(f"\nAnalyzing schema: {schema}")
    
    try:
        snowflake_tables, postgres_tables = get_schema_tables(schema)
        
        # Tables that exist in both databases
        common_tables = snowflake_tables.intersection(postgres_tables)
        print(f"Found {len(common_tables)} tables to check in schema {schema}")
        
        tables_to_drop = []
        
        # Check each table for row count mismatches
        for table in sorted(common_tables):
            try:
                snow_count, pg_count = get_row_counts(schema, table)
                print(f"\n{schema}.{table}:")
                print(f"  Snowflake (source): {snow_count:,} rows")
                print(f"  TNAS PostgreSQL: {pg_count:,} rows")
                
                if snow_count != pg_count:
                    print(f"  ⚠️  Row count mismatch - marking TNAS table for deletion")
                    tables_to_drop.append(table)
            except Exception as e:
                print(f"  ⚠️  Error checking {schema}.{table}: {str(e)}")
                tables_to_drop.append(table)
        
        # Drop tables with confirmation
        if tables_to_drop:
            print(f"\nFound {len(tables_to_drop)} incomplete tables to drop from TNAS in schema {schema}:")
            for table in tables_to_drop:
                print(f"  - {table}")
            
            confirm = input(f"\nDo you want to drop these tables from the TNAS PostgreSQL database? (yes/no): ")
            if confirm.lower() == 'yes':
                with pg_conn.begin() as conn:
                    for table in tables_to_drop:
                        print(f"Dropping {schema}.{table} from TNAS...")
                        conn.execute(sqlalchemy.text(
                            f"DROP TABLE IF EXISTS {schema.lower()}.{table.lower()}"
                        ))
                print(f"\nSuccessfully dropped {len(tables_to_drop)} tables from TNAS")
            else:
                print("\nNo tables were dropped")
        else:
            print(f"\nNo mismatched tables found in schema {schema}")
            
    except Exception as e:
        print(f"Error processing schema {schema}: {str(e)}")

def main():
    schemas = ['WA211', 'WITHINREAC', 'WHATCOMCOU']
    
    print("Starting TNAS PostgreSQL cleanup process...")
    
    # Check if schemas exist in PostgreSQL
    existing_schemas = []
    with pg_conn.connect() as conn:
        for schema in schemas:
            result = conn.execute(sqlalchemy.text(
                "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema)"
            ), {'schema': schema.lower()})
            if result.scalar():
                existing_schemas.append(schema)
    
    if not existing_schemas:
        print("No target schemas found in TNAS PostgreSQL")
        return
    
    print(f"Found {len(existing_schemas)} schemas to process in TNAS")
    
    # Process each schema
    for schema in existing_schemas:
        cleanup_schema(schema)
    
    print("\nTNAS cleanup process completed!")

if __name__ == "__main__":
    main()