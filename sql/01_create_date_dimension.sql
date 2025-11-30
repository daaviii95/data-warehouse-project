-- Create Date Dimension Table
-- Kimball Methodology: Standard date dimension for time-based analysis
-- Populates date dimension from 2020-01-01 to 2025-12-31

DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(50) NOT NULL,
    day INTEGER NOT NULL,
    quarter VARCHAR(2) NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(50) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Generate date dimension data
INSERT INTO dim_date (date_sk, date, year, month, month_name, day, quarter, day_of_week, day_name, is_weekend)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_sk,
    d AS date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::INTEGER AS day,
    'Q' || TO_CHAR(d, 'Q') AS quarter,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2020-01-01'::DATE,
    '2025-12-31'::DATE,
    '1 day'::INTERVAL
) AS d;

CREATE INDEX idx_dim_date_date ON dim_date(date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

