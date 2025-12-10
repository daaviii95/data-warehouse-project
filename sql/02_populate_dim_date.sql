-- Populate Date Dimension
-- Kimball Methodology: Standard date dimension for time-based analysis
-- Populates date dimension from 2020-01-01 to 2025-12-31
-- Idempotent: Only inserts dates that don't already exist (safe to run multiple times)

-- Generate date dimension data
-- Using ON CONFLICT DO NOTHING makes this idempotent and avoids foreign key constraint issues
INSERT INTO dim_date (date_sk, date, year, month, month_name, day, quarter)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_sk,
    d AS date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::INTEGER AS day,
    'Q' || TO_CHAR(d, 'Q') AS quarter
FROM generate_series(
    '2020-01-01'::DATE,
    '2025-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_sk) DO NOTHING;

DO $$
BEGIN
    RAISE NOTICE 'Date dimension populated successfully';
END $$;

