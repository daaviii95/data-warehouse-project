# ShopZada Data Warehouse - Verification Script
# Checks if all components are working correctly

Write-Host "🔍 ShopZada Data Warehouse Verification" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host ""

$errors = 0

# 1. Check Docker Services
Write-Host "1️⃣ Checking Docker Services..." -ForegroundColor Green
$services = @("shopzada-db", "shopzada-airflow-webserver", "shopzada-airflow-scheduler")
foreach ($service in $services) {
    $running = docker ps --format "{{.Names}}" | Select-String -Pattern "^${service}$"
    if ($running) {
        Write-Host "  ✓ $service is running" -ForegroundColor Green
    } else {
        Write-Host "  ✗ $service is NOT running" -ForegroundColor Red
        $errors++
    }
}
Write-Host ""

# 2. Check Database Connection
Write-Host "2️⃣ Checking Database Connection..." -ForegroundColor Green
try {
    $result = docker exec shopzada-db psql -U postgres -d shopzada -c "SELECT version();" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ Database connection successful" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Database connection failed" -ForegroundColor Red
        $errors++
    }
} catch {
    Write-Host "  ✗ Database connection failed: $_" -ForegroundColor Red
    $errors++
}
Write-Host ""

# 3. Check Staging Tables
Write-Host "3️⃣ Checking Staging Tables..." -ForegroundColor Green
try {
    $stagingCount = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'stg_%';
    " 2>&1 | ForEach-Object { $_.Trim() }
    
    if ($stagingCount -gt 0) {
        Write-Host "  ✓ Found $stagingCount staging tables" -ForegroundColor Green
        
        # Show sample tables
        $tables = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE 'stg_%'
            ORDER BY table_name LIMIT 5;
        " 2>&1 | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
        
        Write-Host "  Sample tables:" -ForegroundColor Gray
        foreach ($table in $tables) {
            Write-Host "    - $table" -ForegroundColor Gray
        }
    } else {
        Write-Host "  ⚠ No staging tables found. Run ingestion first." -ForegroundColor Yellow
        $errors++
    }
} catch {
    Write-Host "  ✗ Error checking staging tables: $_" -ForegroundColor Red
    $errors++
}
Write-Host ""

# 4. Check Ingestion Log
Write-Host "4️⃣ Checking Ingestion Log..." -ForegroundColor Green
try {
    $logCount = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
        SELECT COUNT(*) FROM ingestion_log;
    " 2>&1 | ForEach-Object { $_.Trim() }
    
    if ($logCount -gt 0) {
        Write-Host "  ✓ Found $logCount ingestion log entries" -ForegroundColor Green
        
        # Show recent logs
        Write-Host "  Recent ingestion status:" -ForegroundColor Gray
        docker exec shopzada-db psql -U postgres -d shopzada -c "
            SELECT file_path, status, rows_ingested, ts 
            FROM ingestion_log 
            ORDER BY ts DESC 
            LIMIT 5;
        " 2>&1 | Select-String -Pattern "success|failed" | ForEach-Object {
            if ($_ -match "success") {
                Write-Host "    ✓ $_" -ForegroundColor Green
            } elseif ($_ -match "failed") {
                Write-Host "    ✗ $_" -ForegroundColor Red
                $errors++
            }
        }
    } else {
        Write-Host "  ⚠ No ingestion logs found. Run ingestion first." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ Could not check ingestion log (table may not exist)" -ForegroundColor Yellow
}
Write-Host ""

# 5. Check Dimensional Model (if ETL has been run)
Write-Host "5️⃣ Checking Dimensional Model..." -ForegroundColor Green
try {
    $dimCount = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name LIKE 'dim_%' OR table_name LIKE 'fact_%');
    " 2>&1 | ForEach-Object { $_.Trim() }
    
    if ($dimCount -gt 0) {
        Write-Host "  ✓ Found $dimCount dimensional model tables" -ForegroundColor Green
        
        # Check dimensions
        $dims = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE 'dim_%'
            ORDER BY table_name;
        " 2>&1 | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
        
        Write-Host "  Dimensions:" -ForegroundColor Gray
        foreach ($dim in $dims) {
            $rowCount = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
                SELECT COUNT(*) FROM $dim;
            " 2>&1 | ForEach-Object { $_.Trim() }
            Write-Host "    - $dim : $rowCount rows" -ForegroundColor Gray
        }
        
        # Check facts
        $facts = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE 'fact_%'
            ORDER BY table_name;
        " 2>&1 | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
        
        if ($facts) {
            Write-Host "  Facts:" -ForegroundColor Gray
            foreach ($fact in $facts) {
                $rowCount = docker exec shopzada-db psql -U postgres -d shopzada -t -c "
                    SELECT COUNT(*) FROM $fact;
                " 2>&1 | ForEach-Object { $_.Trim() }
                Write-Host "    - $fact : $rowCount rows" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "  ⚠ Dimensional model not created yet. Run ETL pipeline." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ Could not check dimensional model: $_" -ForegroundColor Yellow
}
Write-Host ""

# 6. Check Airflow
Write-Host "6️⃣ Checking Airflow..." -ForegroundColor Green
try {
    $airflowHealth = docker exec shopzada-airflow-webserver curl -s http://localhost:8080/health 2>&1
    if ($airflowHealth -match "healthy" -or $LASTEXITCODE -eq 0) {
        Write-Host "  ✓ Airflow webserver is accessible" -ForegroundColor Green
        Write-Host "    URL: http://localhost:8080" -ForegroundColor Gray
        Write-Host "    Login: admin / admin" -ForegroundColor Gray
    } else {
        Write-Host "  ⚠ Airflow webserver may not be ready yet" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ Could not check Airflow health: $_" -ForegroundColor Yellow
}
Write-Host ""

# Summary
Write-Host "=" * 60 -ForegroundColor Cyan
if ($errors -eq 0) {
    Write-Host "✅ Verification Complete - All checks passed!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Verification Complete - Found $errors issue(s)" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "  1. If no staging tables: Run ingestion" -ForegroundColor Gray
    Write-Host "     docker compose -f docker/docker-compose.yml run --rm shopzada-ingest" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. If dimensional model missing: Run ETL pipeline" -ForegroundColor Gray
    Write-Host "     Trigger 'shopzada_etl_pipeline' DAG in Airflow" -ForegroundColor Gray
}
Write-Host ""

