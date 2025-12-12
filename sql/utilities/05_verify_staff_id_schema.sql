-- Verify staff_id column exists in staging table
-- Per PHYSICALMODEL.txt: fact_orders requires staff_sk, which comes from staff_id

DO $$
DECLARE
    has_staff_id BOOLEAN;
    total_rows INTEGER;
    null_staff_id INTEGER;
BEGIN
    -- Check if staff_id column exists
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'stg_enterprise_department_order_with_merchant_data' 
        AND column_name = 'staff_id'
    ) INTO has_staff_id;
    
    IF has_staff_id THEN
        RAISE NOTICE '✓ staff_id column exists in stg_enterprise_department_order_with_merchant_data';
        
        -- Check data status
        SELECT COUNT(*) INTO total_rows 
        FROM stg_enterprise_department_order_with_merchant_data;
        
        SELECT COUNT(*) INTO null_staff_id 
        FROM stg_enterprise_department_order_with_merchant_data 
        WHERE staff_id IS NULL;
        
        RAISE NOTICE '';
        RAISE NOTICE 'Data Status:';
        RAISE NOTICE '  Total rows: %', total_rows;
        RAISE NOTICE '  Rows with NULL staff_id: %', null_staff_id;
        RAISE NOTICE '  Rows with staff_id: %', total_rows - null_staff_id;
        RAISE NOTICE '';
        
        IF null_staff_id = total_rows AND total_rows > 0 THEN
            RAISE WARNING '⚠️  All rows have NULL staff_id!';
            RAISE WARNING '   Action required: Re-run ingest_enterprise_department task';
            RAISE WARNING '   to reload data with staff_id populated';
        ELSIF null_staff_id > 0 THEN
            RAISE WARNING '⚠️  Some rows have NULL staff_id. These will be skipped in fact_orders';
        ELSIF total_rows = 0 THEN
            RAISE NOTICE 'ℹ️  Table is empty. Run ingest_enterprise_department task to load data';
        ELSE
            RAISE NOTICE '✓ All rows have staff_id populated (ready for fact_orders loading)';
        END IF;
    ELSE
        RAISE ERROR '✗ staff_id column MISSING!';
        RAISE NOTICE '';
        RAISE NOTICE 'Action required:';
        RAISE NOTICE '1. Run: sql/utilities/01_add_staff_id_to_order_merchant_staging.sql';
        RAISE NOTICE '2. Re-run ingest_enterprise_department task';
    END IF;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Per PHYSICALMODEL.txt:';
    RAISE NOTICE '  fact_orders.staff_sk is REQUIRED (NOT NULL)';
    RAISE NOTICE '  staff_sk comes from dim_staff lookup using staff_id';
    RAISE NOTICE '  Therefore, staff_id must be populated in staging table';
END $$;
