import os
from dotenv import load_dotenv
import snowflake.connector
import sqlalchemy 
import pandas as pd
from tqdm import tqdm
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('snowflake.connector')
logger.setLevel(logging.DEBUG)

# Debug print
print(f"Attempting to connect with account: {os.getenv('SNOWFLAKE_ACCOUNT')}")

# Snowflake connection
snow_conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    authenticator='snowflake', 
    client_request_id=None,
    protocol='https'
)

# PostgreSQL connection string
pg_conn = sqlalchemy.create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:{os.getenv('TUNNEL_PORT')}/{os.getenv('POSTGRES_DB')}"
)

def diagnose_postgres_connection():
    """Comprehensive PostgreSQL connection diagnostic function"""
    import socket
    import psycopg2
    from contextlib import closing
    import os
    
    def check_port_status(host, port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(3)
            try:
                result = sock.connect_ex((host, port))
                return result == 0
            except socket.error:
                return False
    
    # 1. Check if tunnel port is actually listening
    tunnel_port = int(os.getenv('TUNNEL_PORT', 54321))
    print(f"\n1. Checking if tunnel port {tunnel_port} is listening...")
    if check_port_status('localhost', tunnel_port):
        print(f"✓ Port {tunnel_port} is open and listening")
    else:
        print(f"✗ Port {tunnel_port} is not listening!")
        
    # 2. Try raw psycopg2 connection
    print("\n2. Attempting direct psycopg2 connection...")
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host='localhost',
            port=tunnel_port
        )
        print("✓ Direct psycopg2 connection successful!")
        
        # 3. Check PostgreSQL server information
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"\n3. PostgreSQL Server Information:")
        print(f"Version: {version}")
        
        # Check listen_addresses
        cur.execute("SHOW listen_addresses;")
        listen_addresses = cur.fetchone()[0]
        print(f"listen_addresses: {listen_addresses}")
        
        # Check max_connections
        cur.execute("SHOW max_connections;")
        max_connections = cur.fetchone()[0]
        print(f"max_connections: {max_connections}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"✗ Direct connection failed: {str(e)}")
        
    # 4. Try to connect to the actual container port through TNAS
    remote_port = int(os.getenv('POSTGRES_LOCAL_PORT', 2345))
    print(f"\n4. Checking if container port {remote_port} is reachable through tunnel...")
    if check_port_status('localhost', remote_port):
        print(f"✓ Container port {remote_port} is reachable")
    else:
        print(f"✗ Container port {remote_port} is not reachable!")

def verify_postgres_connection():
    """Verify PostgreSQL connection through SSH tunnel"""
    import socket
    import time
    import psycopg2
    
    port = int(os.getenv('TUNNEL_PORT', 54321))
    max_attempts = 5  # Increased from 3
    delay = 3  # Increased from 2
    
    for attempt in range(max_attempts):
        try:
            # First check if port is listening
            with socket.create_connection(('localhost', port), timeout=5):
                print(f"✓ Port {port} is listening")
                
            # Then try actual database connection
            conn = psycopg2.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host='localhost',
                port=port,
                connect_timeout=5
            )
            conn.close()
            print("✓ PostgreSQL connection verified")
            return True
                
        except (socket.timeout, ConnectionRefusedError, psycopg2.OperationalError) as e:
            if attempt < max_attempts - 1:
                print(f"Attempt {attempt + 1}: Connection not ready, waiting... ({str(e)})")
                time.sleep(delay)
            else:
                raise Exception(
                    f"\nERROR: Could not connect to PostgreSQL through SSH tunnel.\n"
                    f"Error details: {str(e)}\n"
                    "Please check:\n"
                    "1. SSH tunnel is established\n"
                    "2. PostgreSQL container is running\n"
                    "3. Container port mapping is correct\n"
                    f"4. PostgreSQL is configured to accept connections on port {port}\n"
                )
            
def get_tables_in_schema(snow_cur, schema):
    snow_cur.execute(f"SHOW TABLES IN SCHEMA {schema}")
    return [row[1] for row in snow_cur.fetchall()]

def transfer_table(snow_cur, schema, table):
    try:
        # Get row count for progress bar
        snow_cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        total_rows = snow_cur.fetchone()[0]
        
        # Read from Snowflake in chunks
        snow_cur.execute(f"SELECT * FROM {schema}.{table}")
        columns = [desc[0] for desc in snow_cur.description]
        
        # Create schema if it doesn't exist
        with pg_conn.connect() as conn:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS {schema.lower()}')
        
        # Process in chunks of 10000 rows
        chunk_size = 10000
        with tqdm(total=total_rows, desc=f"Transferring {schema}.{table}") as pbar:
            while True:
                chunk = snow_cur.fetchmany(chunk_size)
                if not chunk:
                    break
                    
                df = pd.DataFrame(chunk, columns=columns)
                df.to_sql(
                    table.lower(), 
                    pg_conn, 
                    schema=schema.lower(), 
                    if_exists='append' if pbar.n > 0 else 'replace',
                    index=False
                )
                pbar.update(len(chunk))
                
        # Verify row count
        with pg_conn.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {schema.lower()}.{table.lower()}")
            pg_count = result.scalar()
            
        print(f"Row count verification for {schema}.{table}:")
        print(f"Snowflake: {total_rows}, PostgreSQL: {pg_count}")
        assert total_rows == pg_count, "Row count mismatch!"
        
    except Exception as e:
        print(f"Error transferring {schema}.{table}: {str(e)}")
        
        # Run diagnostics when we hit a Postgres connection error
        if isinstance(e, sqlalchemy.exc.OperationalError) and "connection" in str(e).lower():
            print("\nRunning PostgreSQL connection diagnostics...")
            diagnose_postgres_connection()

        raise

def main():
    schemas = ['WA211', 'WITHINREAC', 'WHATCOMCOU']
    
    # Verify connections
    verify_postgres_connection()
    diagnose_postgres_connection()

    snow_cur = snow_conn.cursor()
    
    for schema in schemas:
        print(f"\nProcessing schema: {schema}")
        tables = get_tables_in_schema(snow_cur, schema)
        print(f"Found {len(tables)} tables")
        
        for table in tables:
            transfer_table(snow_cur, schema, table)

if __name__ == "__main__":
    main()