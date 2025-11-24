#!/bin/bash
# ShopZada Data Warehouse - Verification Script
# Checks if all components are working correctly

echo "🔍 ShopZada Data Warehouse Verification"
echo "============================================================"
echo ""

errors=0

# 1. Check Docker Services
echo "1️⃣ Checking Docker Services..."
services=("shopzada-db" "shopzada-airflow-webserver" "shopzada-airflow-scheduler")
for service in "${services[@]}"; do
    if docker ps --format "{{.Names}}" | grep -q "^${service}$"; then
        echo "  ✓ $service is running"
    else
        echo "  ✗ $service is NOT running"
        ((errors++))
    fi
done
echo ""

# 2. Check Database Connection
echo "2️⃣ Checking Database Connection..."
if docker exec shopzada-db psql -U postgres -d shopzada -c "SELECT version();" > /dev/null 2>&1; then
    echo "  ✓ Database connection successful"
else
    echo "  ✗ Database connection failed"
    ((errors++))
fi
echo ""

# 3. Check Staging Tables
echo "3️⃣ Checking Staging Tables..."
staging_count=$(docker exec shopzada-db psql -U postgres -d shopzada -t -c "
    SELECT COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name LIKE 'stg_%';
" | tr -d ' ')

if [ "$staging_count" -gt 0 ]; then
    echo "  ✓ Found $staging_count staging tables"
    
    echo "  Sample tables:"
    docker exec shopzada-db psql -U postgres -d shopzada -t -c "
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'stg_%'
        ORDER BY table_name LIMIT 5;
    " | sed 's/^/    - /'
else
    echo "  ⚠ No staging tables found. Run ingestion first."
    ((errors++))
fi
echo ""

# 4. Check Ingestion Log
echo "4️⃣ Checking Ingestion Log..."
log_count=$(docker exec shopzada-db psql -U postgres -d shopzada -t -c "
    SELECT COUNT(*) FROM ingestion_log;
" 2>/dev/null | tr -d ' ')

if [ "$log_count" -gt 0 ]; then
    echo "  ✓ Found $log_count ingestion log entries"
    
    echo "  Recent ingestion status:"
    docker exec shopzada-db psql -U postgres -d shopzada -c "
        SELECT file_path, status, rows_ingested, ts 
        FROM ingestion_log 
        ORDER BY ts DESC 
        LIMIT 5;
    " | grep -E "success|failed" | while read line; do
        if echo "$line" | grep -q "success"; then
            echo "    ✓ $line"
        elif echo "$line" | grep -q "failed"; then
            echo "    ✗ $line"
            ((errors++))
        fi
    done
else
    echo "  ⚠ No ingestion logs found. Run ingestion first."
fi
echo ""

# 5. Check Dimensional Model
echo "5️⃣ Checking Dimensional Model..."
dim_count=$(docker exec shopzada-db psql -U postgres -d shopzada -t -c "
    SELECT COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND (table_name LIKE 'dim_%' OR table_name LIKE 'fact_%');
" 2>/dev/null | tr -d ' ')

if [ "$dim_count" -gt 0 ]; then
    echo "  ✓ Found $dim_count dimensional model tables"
    
    echo "  Dimensions:"
    docker exec shopzada-db psql -U postgres -d shopzada -t -c "
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'dim_%'
        ORDER BY table_name;
    " | while read dim; do
        if [ -n "$dim" ]; then
            row_count=$(docker exec shopzada-db psql -U postgres -d shopzada -t -c "SELECT COUNT(*) FROM $dim;" | tr -d ' ')
            echo "    - $dim : $row_count rows"
        fi
    done
    
    echo "  Facts:"
    docker exec shopzada-db psql -U postgres -d shopzada -t -c "
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'fact_%'
        ORDER BY table_name;
    " | while read fact; do
        if [ -n "$fact" ]; then
            row_count=$(docker exec shopzada-db psql -U postgres -d shopzada -t -c "SELECT COUNT(*) FROM $fact;" | tr -d ' ')
            echo "    - $fact : $row_count rows"
        fi
    done
else
    echo "  ⚠ Dimensional model not created yet. Run ETL pipeline."
fi
echo ""

# 6. Check Airflow
echo "6️⃣ Checking Airflow..."
if docker exec shopzada-airflow-webserver curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "  ✓ Airflow webserver is accessible"
    echo "    URL: http://localhost:8080"
    echo "    Login: admin / admin"
else
    echo "  ⚠ Airflow webserver may not be ready yet"
fi
echo ""

# Summary
echo "============================================================"
if [ $errors -eq 0 ]; then
    echo "✅ Verification Complete - All checks passed!"
else
    echo "⚠️  Verification Complete - Found $errors issue(s)"
    echo ""
    echo "Next steps:"
    echo "  1. If no staging tables: Run ingestion"
    echo "     docker compose -f docker/docker-compose.yml run --rm shopzada-ingest"
    echo ""
    echo "  2. If dimensional model missing: Run ETL pipeline"
    echo "     Trigger 'shopzada_etl_pipeline' DAG in Airflow"
fi
echo ""

