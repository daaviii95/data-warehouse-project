#!/bin/bash
# Reset ShopZada database and re-run ingestion
# This script clears all staging tables and re-ingests data

set -e

echo "🔄 ShopZada Reset and Re-ingestion"
echo "===================================="

# Find the Postgres container
DB_CONTAINER="shopzada-db"

if ! docker ps --format "{{.Names}}" | grep -q "^${DB_CONTAINER}$"; then
    echo "❌ Container ${DB_CONTAINER} not running. Starting services..."
    docker compose -f docker/docker-compose.yml up -d shopzada-db
    echo "⏳ Waiting for database to be ready..."
    sleep 5
fi

# Step 1: Reset database (drop all staging tables)
echo ""
echo "📋 Step 1: Clearing staging tables..."
if [ -f "./sql/reset.sql" ]; then
    docker exec -i "${DB_CONTAINER}" psql -U postgres -d shopzada < ./sql/reset.sql
    echo "✅ Staging tables cleared"
else
    echo "⚠️  reset.sql not found, skipping database reset"
fi

# Step 2: Clear Parquet export directory (optional - only if using Parquet export DAGs)
# echo ""
# echo "📋 Step 2: Clearing Parquet export directory..."
# if [ -d "./data/staging_parquet" ]; then
#     rm -f ./data/staging_parquet/*.parquet
#     echo "✅ Parquet files cleared"
# fi

# Step 3: Re-run ingestion
echo ""
echo "📋 Step 3: Re-running ingestion..."
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest

echo ""
echo "✅ Reset and re-ingestion completed!"
echo "📊 Check ingestion_log table for details:"
echo "   docker exec -it ${DB_CONTAINER} psql -U postgres -d shopzada -c 'SELECT * FROM ingestion_log ORDER BY ts DESC LIMIT 10;'"

