#!/bin/bash

# Function to find an available port
find_port() {
    local port=$1
    while ! nc -z localhost $port 2>/dev/null; do
        return $port
    done
    find_port $((port + 1))
}

# Start the backend
echo "Starting backend..."
cd backend
export FLASK_ENV=development
BACKEND_PORT=$(find_port 5001)
python3 src/app.py --port $BACKEND_PORT &
BACKEND_PID=$!

# Start the frontend
echo "Starting frontend..."
cd ../frontend
FRONTEND_PORT=$(find_port 8000)
python3 -m http.server $FRONTEND_PORT &
FRONTEND_PID=$!

# Function to kill both servers
cleanup() {
    echo "Shutting down servers..."
    kill $BACKEND_PID
    kill $FRONTEND_PID
    exit 0
}

# Set up trap for cleanup
trap cleanup INT

echo "Servers started!"
echo "Frontend running at: http://localhost:$FRONTEND_PORT"
echo "Backend running at: http://localhost:$BACKEND_PORT"
echo "Press Ctrl+C to stop both servers"

# Wait for Ctrl+C
wait
