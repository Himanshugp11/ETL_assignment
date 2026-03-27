#!/bin/bash

# 1. Activate the Virtual Environment
echo "--- Activating Virtual Environment ---"
source virtulenv/bin/activate || source virtulenv/Scripts/activate

# 2. Infrastructure Check (Force Refresh)
echo "--- Starting LocalStack ---"
# Remove if exists to fix port/name conflicts
docker rm -f localstack-main 2>/dev/null 
docker run -d --name localstack-main -p 4566:4566 -p 4510-4560:4510-4560 localstack/localstack

echo "--- Starting Postgres ---"
docker start local-datastore 2>/dev/null || \
docker run -d --name local-datastore -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=password123 -e POSTGRES_DB=events_db -p 5432:5432 postgres:16-alpine

# Wait for LocalStack to actually be ready
echo "--- Waiting for LocalStack to initialize (10s) ---"
sleep 10

echo "--- Current Infrastructure Status ---"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 3. Run Generator
echo "--- Generating Mock Data in SQS ---"
python src/mock_generator.py

# 4. Final Processor
echo "--- Waiting for SQS/DB Sync (20s) ---"
sleep 20
python src/final_main.py

# 5. Verify Results
echo "--- Verifying Data in PostgreSQL ---"
docker exec -it local-datastore psql -U admin -d events_db -c "SELECT * FROM trip_events;"