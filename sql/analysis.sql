-- =============================================================================
-- analysis.sql
-- -----------------------------------------------------------------------------
-- Exploratory / data-quality Spark SQL used while building the Silver and Gold
-- layers. Distinct from business_queries.sql, which holds the final,
-- stakeholder-facing business questions. These queries are more about
-- understanding the shape and health of the data.
--
-- Run against the same `silver_startup_funding` temp view (see
-- business_queries.sql header for how to register it), or, for the raw-data
-- checks, against a `bronze_startup_funding` view registered the same way
-- from src/bronze.py's output.
-- =============================================================================

-- A. Row count and basic shape of the Silver table
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT startup_name) AS distinct_startups
FROM silver_startup_funding;

-- B. Null / missing-value audit across key columns
SELECT
    SUM(CASE WHEN startup_name IS NULL THEN 1 ELSE 0 END) AS null_startup_name,
    SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN sector IS NULL THEN 1 ELSE 0 END) AS null_sector,
    SUM(CASE WHEN investors_name IS NULL THEN 1 ELSE 0 END) AS null_investors,
    SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date
FROM silver_startup_funding;

-- C. Distribution of funding amounts (min / max / avg / stddev)
SELECT
    ROUND(MIN(amount_usd), 2) AS min_amount,
    ROUND(MAX(amount_usd), 2) AS max_amount,
    ROUND(AVG(amount_usd), 2) AS avg_amount,
    ROUND(STDDEV(amount_usd), 2) AS stddev_amount
FROM silver_startup_funding;

-- D. Number of distinct canonical city values after standardization
--    (sanity check that "Bengaluru", "bangalore", "BLR" etc. all collapsed
--    into a single "Bangalore" value)
SELECT DISTINCT city FROM silver_startup_funding ORDER BY city;

-- E. Number of distinct canonical investor tokens after standardization
SELECT DISTINCT investor
FROM (
    SELECT EXPLODE(SPLIT(investors_name, ',\\s*')) AS investor
    FROM silver_startup_funding
)
ORDER BY investor;

-- F. Records per funding_year (sanity check partitioning done at Silver write time)
SELECT funding_year, COUNT(*) AS row_count
FROM silver_startup_funding
GROUP BY funding_year
ORDER BY funding_year;

-- G. Outlier check: deals more than 3 standard deviations above the mean
SELECT startup_name, amount_usd
FROM silver_startup_funding
WHERE amount_usd > (
    SELECT AVG(amount_usd) + 3 * STDDEV(amount_usd) FROM silver_startup_funding
)
ORDER BY amount_usd DESC;

-- H. Sector coverage: how many rows fell back to 'Unknown' sector
SELECT
    SUM(CASE WHEN sector = 'Unknown' THEN 1 ELSE 0 END) AS unknown_sector_rows,
    COUNT(*) AS total_rows,
    ROUND(100.0 * SUM(CASE WHEN sector = 'Unknown' THEN 1 ELSE 0 END) / COUNT(*), 2) AS unknown_pct
FROM silver_startup_funding;

-- I. Earliest and latest funding date present in the cleaned dataset
SELECT MIN(date) AS earliest_deal_date, MAX(date) AS latest_deal_date
FROM silver_startup_funding;

-- J. Duplicate check post-cleaning (should return zero rows if Silver logic is correct)
SELECT startup_name, city, investors_name, amount_usd, date, COUNT(*) AS occurrences
FROM silver_startup_funding
GROUP BY startup_name, city, investors_name, amount_usd, date
HAVING COUNT(*) > 1;
