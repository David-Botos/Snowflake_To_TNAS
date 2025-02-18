#!/bin/bash

# Load environment variables from .env
set -a
source .env
set +a

# Debug output
echo "Configuration:"
echo "Tunnel Port: $TUNNEL_PORT"
echo "TNAS Host: $TNAS_HOST"
echo "TNAS Port: $TNAS_PORT"
echo "TNAS User: $TNAS_USER"
echo

# Function to check if port is in use
check_port() {
    nc -z localhost $1 2>/dev/null
    return $?
}

# Kill existing tunnel if exists
if check_port $TUNNEL_PORT; then
    echo "Killing existing tunnel on port $TUNNEL_PORT"
    kill $(lsof -t -i :$TUNNEL_PORT) 2>/dev/null
fi

# Start SSH tunnel (now using port 5432 directly)
echo "Establishing SSH tunnel..."
ssh -f -N -L "$TUNNEL_PORT:localhost:5432" "$TNAS_USER@$TNAS_HOST" -p "$TNAS_PORT"

# Wait for tunnel to be established
echo "Waiting for tunnel to be ready..."
max_attempts=5
attempt=1
while [ $attempt -le $max_attempts ]; do
    if check_port $TUNNEL_PORT; then
        echo "✓ Tunnel established successfully"
        
        # Test PostgreSQL connection
        if PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p $TUNNEL_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "\l" >/dev/null 2>&1; then
            echo "✓ PostgreSQL connection verified"
            break
        else
            echo "✗ PostgreSQL connection failed"
            exit 1
        fi
    fi
    
    if [ $attempt -eq $max_attempts ]; then
        echo "Failed to establish tunnel after $max_attempts attempts"
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep 1
done

# Test the connections
# source venv/bin/activate
# python test_connections.py

# Check for cleanup flag
if [ "$1" == "--cleanup" ]; then
    echo "Running cleanup script..."
    source venv/bin/activate
    python cleanup_partials.py
    deactivate
    exit 0
fi

# Send the data
source venv/bin/activate
python data_ingress.py

# Clean up tunnel after script completes
echo "Cleaning up SSH tunnel..."
kill $(lsof -t -i :$TUNNEL_PORT) 2>/dev/null