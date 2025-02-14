#!/bin/bash

# Load environment variables from .env
set -a  # automatically export all variables
source .env
set +a  # stop automatically exporting

# Validate required variables
required_vars=("TUNNEL_PORT" "TNAS_HOST" "TNAS_PORT" "TNAS_USER" "POSTGRES_LOCAL_PORT")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: Required variable $var is not set"
        exit 1
    fi
done

# Debug output
echo "Configuration:"
echo "Tunnel Port: $TUNNEL_PORT"
echo "TNAS Host: $TNAS_HOST"
echo "TNAS Port: $TNAS_PORT"
echo "TNAS User: $TNAS_USER"
echo "PostgreSQL Local Port: $POSTGRES_LOCAL_PORT"
echo

# Function to check if port is in use
check_port() {
    lsof -i :$1 > /dev/null 2>&1
    return $?
}

# Function to check if tunnel is working
check_tunnel() {
    nc -z localhost $TUNNEL_PORT > /dev/null 2>&1
    return $?
}

# Kill existing tunnel if exists
if check_port $TUNNEL_PORT; then
    echo "Killing existing tunnel on port $TUNNEL_PORT"
    kill $(lsof -t -i :$TUNNEL_PORT) 2>/dev/null
fi

# Start SSH tunnel in background
echo "Establishing SSH tunnel..."
ssh -f -N -L "$TUNNEL_PORT:localhost:$POSTGRES_LOCAL_PORT" "$TNAS_USER@$TNAS_HOST" -p "$TNAS_PORT"

# Wait for tunnel to be established
echo "Waiting for tunnel to be ready..."
for i in {1..5}; do
    if check_tunnel; then
        echo "Tunnel established successfully"
        break
    fi
    if [ $i -eq 5 ]; then
        echo "Failed to establish tunnel"
        exit 1
    fi
    sleep 1
done

# Activate virtual environment and run script
source venv/bin/activate
python dataIngress.py

# Clean up tunnel after script completes
echo "Cleaning up SSH tunnel..."
kill $(lsof -t -i :$TUNNEL_PORT) 2>/dev/null