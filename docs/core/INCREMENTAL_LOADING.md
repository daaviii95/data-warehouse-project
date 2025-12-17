# Incremental Loading Implementation

## Overview

The ETL pipeline now supports **incremental loading** - it only processes files that haven't been processed yet or have been modified since last ingestion. This prevents duplicate data accumulation in staging tables and improves performance.

## How It Works

### File Tracking

The system tracks processed files using the `file_source` column in staging tables:

1. **Before Processing**: Queries staging table to get list of already processed files
2. **File Check**: For each file found:
   - Checks if file path exists in processed files list
   - If processed, checks file modification time vs. last ingestion time
   - Only processes if file is new or was modified after last ingestion
3. **Skip Logic**: Files that are already processed and unchanged are skipped

### File Modification Detection

If a file was already processed, the system:
- Gets file's modification timestamp
- Compares with `loaded_at` timestamp from staging table
- Re-processes if file was modified after last ingestion

## Configuration

### Environment Variable

Set `FORCE_FULL_RELOAD=true` to disable incremental loading and process all files:

```bash
export FORCE_FULL_RELOAD=true
```

Or in Airflow:
```python
env = {
    'FORCE_FULL_RELOAD': 'true'  # Process all files regardless of status
}
```

### Default Behavior

By default, incremental loading is **enabled** (`FORCE_FULL_RELOAD=false`).

## Benefits

### Performance
- Only processes new/changed files
- Faster pipeline execution
- Reduced database load

### No duplicates
- Prevents duplicate data in staging tables
- Cleaner data warehouse
- Reduced storage usage

### Smart updates
- Automatically detects file changes
- Re-processes updated files
- Handles file updates gracefully

## Usage Examples

### Normal Run (Incremental)
```bash
# Default behavior - only processes new/changed files
python scripts/ingest.py
```

### Force Full Reload
```bash
# Process all files regardless of status
FORCE_FULL_RELOAD=true python scripts/ingest.py
```

### In Airflow DAG
```python
# Normal incremental loading
ingest_task = PythonOperator(
    task_id='ingest',
    python_callable=run_ingest,
    env={'FORCE_FULL_RELOAD': 'false'}  # Default
)

# Force full reload
ingest_task = PythonOperator(
    task_id='ingest',
    python_callable=run_ingest,
    env={'FORCE_FULL_RELOAD': 'true'}
)
```

## Logging

The system provides detailed logging:

```
INGESTING: Operations Department
Order data: 5 files processed, 10 files skipped
  - Processing: order_data_2024.csv (File not yet processed)
  - Skipping: order_data_2023.csv (File already processed and not modified)
  - Processing: order_data_2024_updated.csv (File modified after last ingestion)
```

## Edge Cases Handled

### File Not Found
- If a processed file is deleted, it's skipped (no error)
- New files are processed normally

### File Modification Time Issues
- If modification time can't be checked, file is processed (safer approach)
- Logs warning for debugging

### Database Connection Issues
- If processed files can't be retrieved, all files are processed
- Prevents data loss from connection problems

## Migration from Full Reload

If you're migrating from full reload to incremental loading:

1. **First Run**: Will process all files (none are tracked yet)
2. **Subsequent Runs**: Only new/changed files are processed
3. **Clean Slate**: Use `truncate_staging_tables.py` if you need to start fresh

## Comparison

| Feature | Full Reload | Incremental Loading |
|---------|------------|---------------------|
| **Processes** | All files | New/changed files only |
| **Performance** | Slower | Faster |
| **Duplicates** | Yes (in staging) | No |
| **File Updates** | Re-processes all | Re-processes only changed |
| **Storage** | Accumulates | Clean |

## Troubleshooting

### Files Not Being Processed

**Check logs** for skip reasons:
```
Skipping file.csv: File already processed and not modified
```

**Solution**: 
- Set `FORCE_FULL_RELOAD=true` to process all files
- Or manually update file modification time

### Files Being Re-Processed

**Check logs** for processing reasons:
```
Processing file.csv: File modified after last ingestion
```

**Solution**: 
- This is expected if file was actually modified
- Check file modification time if unexpected

### Performance Issues

If incremental loading is slow:
- Check database query performance on staging tables
- Consider adding indexes on `file_source` column
- Verify file system performance

## Technical Details

### Functions

- `_get_processed_files(table_name)`: Gets set of processed file paths
- `_should_process_file(file_path, table_name, processed_files)`: Determines if file should be processed

### Database Queries

```sql
-- Get processed files
SELECT DISTINCT file_source 
FROM stg_operations_department_order_data
WHERE file_source IS NOT NULL

-- Get last ingestion time
SELECT MAX(loaded_at) 
FROM stg_operations_department_order_data
WHERE file_source = 'path/to/file.csv'
```

## Best Practices

1. **Use Incremental Loading in Production**: Better performance and no duplicates
2. **Use Full Reload for Testing**: Easier to debug with consistent state
3. **Monitor Logs**: Check skip/process counts to verify behavior
4. **Handle File Updates**: System automatically detects and re-processes updated files
