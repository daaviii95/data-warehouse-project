-- ============================================================================
-- Populate Date Dimension
-- ============================================================================
-- Generates date dimension for years 2020-2025 (covers all order data)
-- ============================================================================

INSERT INTO dim_date (
    date_key,
    date_actual,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_number,
    month_name,
    quarter_number,
    quarter_name,
    year_number,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS date_actual,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(DOY FROM d)::INTEGER AS day_of_year,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
    EXTRACT(MONTH FROM d)::INTEGER AS month_number,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter_number,
    'Q' || EXTRACT(QUARTER FROM d)::INTEGER AS quarter_name,
    EXTRACT(YEAR FROM d)::INTEGER AS year_number,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2020-01-01'::DATE,
    '2025-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_key) DO NOTHING;

COMMENT ON TABLE dim_date IS 'Date dimension populated for 2020-2025';

