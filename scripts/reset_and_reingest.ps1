# Reset ShopZada database and re-run ingestion (PowerShell)
# This script clears all staging tables and re-ingests data

Write-Host "🔄 ShopZada Reset and Re-ingestion" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""

$DB_CONTAINER = "shopzada-db"

# Check if container is running
$containerRunning = docker ps --format "{{.Names}}" | Select-String -Pattern "^${DB_CONTAINER}$"

if (-not $containerRunning) {
    Write-Host "❌ Container ${DB_CONTAINER} not running. Starting services..." -ForegroundColor Yellow
    docker compose -f docker/docker-compose.yml up -d shopzada-db
    Write-Host "⏳ Waiting for database to be ready..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
}

# Step 1: Reset database (drop all staging tables)
Write-Host ""
Write-Host "📋 Step 1: Clearing staging tables..." -ForegroundColor Green
if (Test-Path "./sql/reset.sql") {
    Get-Content ./sql/reset.sql | docker exec -i ${DB_CONTAINER} psql -U postgres -d shopzada
    Write-Host "✅ Staging tables cleared" -ForegroundColor Green
} else {
    Write-Host "⚠️  reset.sql not found, skipping database reset" -ForegroundColor Yellow
}

# Step 2: Clear Parquet export directory (optional - only if using Parquet export DAGs)
# Write-Host ""
# Write-Host "📋 Step 2: Clearing Parquet export directory..." -ForegroundColor Green
# if (Test-Path "./data/staging_parquet") {
#     Remove-Item -Path "./data/staging_parquet/*.parquet" -Force -ErrorAction SilentlyContinue
#     Write-Host "✅ Parquet files cleared" -ForegroundColor Green
# }

# Step 3: Re-run ingestion
Write-Host ""
Write-Host "📋 Step 3: Re-running ingestion..." -ForegroundColor Green
docker compose -f docker/docker-compose.yml run --rm shopzada-ingest

Write-Host ""
Write-Host "✅ Reset and re-ingestion completed!" -ForegroundColor Green
Write-Host "📊 Check ingestion_log table for details:" -ForegroundColor Cyan
Write-Host "   docker exec -it ${DB_CONTAINER} psql -U postgres -d shopzada -c 'SELECT * FROM ingestion_log ORDER BY ts DESC LIMIT 10;'" -ForegroundColor Gray

