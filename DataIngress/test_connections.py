import os
from dotenv import load_dotenv
import socket
import psycopg2
import snowflake.connector
from time import sleep

# Load environment variables
load_dotenv()

def test_port_connection(port, description):
    """Test if a specific port is accessible"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', port))
        if result == 0:
            print(f"✓ Successfully connected to {description} (port {port})")
            return True
        else:
            print(f"✗ Could not connect to {description} (port {port})")
            return False
    except Exception as e:
        print(f"✗ Error testing {description} (port {port}): {str(e)}")
        return False
    finally:
        sock.close()

def test_ssh_tunnel():
    """Test if SSH tunnel is properly forwarding PostgreSQL port"""
    print("\n1. Testing SSH Tunnel...")
    
    tunnel_port = int(os.getenv('TUNNEL_PORT', 54321))
    
    # Test tunnel port
    tunnel_success = test_port_connection(tunnel_port, "SSH tunnel to PostgreSQL")
    
    if not tunnel_success:
        print("\nTroubleshooting tips:")
        print(f"- Check if SSH tunnel is running: ps aux | grep {tunnel_port}")
        print("- Verify SSH tunnel command in run_transfer.sh")
        print(f"- Make sure no other process is using port {tunnel_port}")
        print("- Verify PostgreSQL is running on TNAS port 5432")
    
    return tunnel_success

def test_postgres_connection():
    """Test PostgreSQL connection through tunnel"""
    print("\n2. Testing PostgreSQL Connection...")
    
    tunnel_port = int(os.getenv('TUNNEL_PORT', 54321))
    
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host='localhost',
            port=tunnel_port,
            connect_timeout=10
        )
        
        # Test query
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print("\n✓ PostgreSQL Connection Successful!")
        print(f"✓ Server Version: {version}")
        
        # Get additional server information
        cur.execute("SHOW listen_addresses;")
        listen_addresses = cur.fetchone()[0]
        print(f"✓ listen_addresses: {listen_addresses}")
        
        cur.execute("SHOW port;")
        db_port = cur.fetchone()[0]
        print(f"✓ configured port: {db_port}")
        
        # Test schema creation permission
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS test_schema;")
            conn.commit()
            print("✓ Schema creation permission verified")
            
            # Clean up test schema
            cur.execute("DROP SCHEMA test_schema;")
            conn.commit()
        except Exception as e:
            print(f"✗ Schema creation test failed: {str(e)}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ PostgreSQL Connection Failed: {str(e)}")
        print("\nTroubleshooting tips:")
        print("- Check if PostgreSQL is running on TNAS port 5432")
        print("- Verify credentials in .env file")
        print("- Check PostgreSQL logs for connection attempts")
        print("- Verify pg_hba.conf allows external connections")
        print("- Make sure PostgreSQL is listening on all interfaces (0.0.0.0)")
        return False

def test_snowflake_connection():
    """Test Snowflake connection"""
    print("\n3. Testing Snowflake Connection...")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            authenticator='snowflake'
        )
        
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        print("✓ Snowflake Connection Successful!")
        print(f"✓ Snowflake Version: {version}")
        
        # Test database access
        cur.execute("SHOW DATABASES")
        print("✓ Database access verified")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Snowflake Connection Failed: {str(e)}")
        return False

def main():
    print("Starting Connection Tests...")
    print("---------------------------")
    
    # Test SSH Tunnel
    tunnel_success = test_ssh_tunnel()
    if not tunnel_success:
        print("\n⚠️  SSH Tunnel test failed. Please review the troubleshooting tips above.")
        return
    
    # Test PostgreSQL
    postgres_success = test_postgres_connection()
    if not postgres_success:
        print("\n⚠️  PostgreSQL test failed. Please review the troubleshooting tips above.")
        return
    
    # Test Snowflake
    snowflake_success = test_snowflake_connection()
    if not snowflake_success:
        print("\n⚠️  Snowflake test failed. Please check:")
        print("1. Are the Snowflake credentials correct?")
        print("2. Is 2FA required?")
        print("3. Is the VPN required?")
        return
    
    if all([tunnel_success, postgres_success, snowflake_success]):
        print("\n✅ All connection tests passed! You can proceed with the data transfer.")
        print("\nConnection Summary:")
        print(f"- SSH Tunnel Port ({os.getenv('TUNNEL_PORT')}): OK")
        print("- PostgreSQL Connection (through tunnel): OK")
        print("- Snowflake Connection: OK")

if __name__ == "__main__":
    main()