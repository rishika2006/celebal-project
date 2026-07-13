-- =============================================================================
-- business_queries.sql
-- -----------------------------------------------------------------------------
-- 20+ Spark SQL business queries for the Indian Startup Funding dataset.
--
-- How to run:
--   These queries run against the `silver_startup_funding` temp view that is
--   registered by src/gold.py (register_silver_view). From a pyspark shell:
--
--       from src.spark_session import get_spark_session
--       from src.silver import read_silver_parquet, run_silver_layer
--       from src.gold import register_silver_view
--       spark = get_spark_session()
--       silver_df = read_silver_parquet(spark)
--       register_silver_view(spark, silver_df)
--   Then see README.md section 6 for a ready-to-use snippet that safely
--   strips comments from this file and runs every query in sequence.
--
-- Columns available on silver_startup_funding:
--   record_id, startup_name, sector, city, investors_name, investment_type,
--   amount_usd, date, funding_year, funding_month, funding_quarter
-- =============================================================================

-- 1. Top 10 most-funded startups by total amount raised
SELECT startup_name, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
GROUP BY startup_name
ORDER BY total_funding_usd DESC
LIMIT 10;

-- 2. Top 10 individual investors by total capital deployed
SELECT investor, ROUND(SUM(amount_usd), 2) AS total_invested_usd
FROM (
    SELECT EXPLODE(SPLIT(investors_name, ',\\s*')) AS investor, amount_usd
    FROM silver_startup_funding
)
GROUP BY investor
ORDER BY total_invested_usd DESC
LIMIT 10;

-- 3. Total funding raised, grouped by city
SELECT city, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
GROUP BY city
ORDER BY total_funding_usd DESC;

-- 4. Total funding raised, grouped by year
SELECT funding_year, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
GROUP BY funding_year
ORDER BY funding_year;

-- 5. Total funding raised, grouped by sector
SELECT sector, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
GROUP BY sector
ORDER BY total_funding_usd DESC;

-- 6. Average funding amount across all deals
SELECT ROUND(AVG(amount_usd), 2) AS average_funding_usd
FROM silver_startup_funding;

-- 7. The single highest investment amount recorded
SELECT startup_name, investors_name, amount_usd, date
FROM silver_startup_funding
ORDER BY amount_usd DESC
LIMIT 1;

-- 8. The single lowest (but still valid) investment amount recorded
SELECT startup_name, investors_name, amount_usd, date
FROM silver_startup_funding
ORDER BY amount_usd ASC
LIMIT 1;

-- 9. Most active investors, ranked by number of deals participated in
SELECT investor, COUNT(*) AS number_of_deals
FROM (
    SELECT EXPLODE(SPLIT(investors_name, ',\\s*')) AS investor
    FROM silver_startup_funding
)
GROUP BY investor
ORDER BY number_of_deals DESC
LIMIT 10;

-- 10. City that has attracted the single highest amount of total funding
SELECT city, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
GROUP BY city
ORDER BY total_funding_usd DESC
LIMIT 1;

-- 11. Funding breakdown by investment/round type (Seed, Series A, etc.)
SELECT investment_type, ROUND(SUM(amount_usd), 2) AS total_funding_usd, COUNT(*) AS deal_count
FROM silver_startup_funding
GROUP BY investment_type
ORDER BY total_funding_usd DESC;

-- 12. Number of unique startups funded per year
SELECT funding_year, COUNT(DISTINCT startup_name) AS unique_startups_funded
FROM silver_startup_funding
GROUP BY funding_year
ORDER BY funding_year;

-- 13. Top 5 sectors by number of deals (not amount)
SELECT sector, COUNT(*) AS number_of_deals
FROM silver_startup_funding
GROUP BY sector
ORDER BY number_of_deals DESC
LIMIT 5;

-- 14. Monthly funding trend for a specific year (parameterize funding_year as needed)
SELECT funding_month, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
WHERE funding_year = 2023
GROUP BY funding_month
ORDER BY funding_month;

-- 15. Startups that have raised more than 3 distinct funding rounds
SELECT startup_name, COUNT(*) AS number_of_rounds
FROM silver_startup_funding
GROUP BY startup_name
HAVING COUNT(*) > 3
ORDER BY number_of_rounds DESC;

-- 16. Average deal size per city (only cities with 5+ deals, to avoid noise)
SELECT city, COUNT(*) AS number_of_deals, ROUND(AVG(amount_usd), 2) AS avg_deal_size_usd
FROM silver_startup_funding
GROUP BY city
HAVING COUNT(*) >= 5
ORDER BY avg_deal_size_usd DESC;

-- 17. Sector performing best (highest total funding) in each individual year
SELECT funding_year, sector, total_funding_usd
FROM (
    SELECT
        funding_year,
        sector,
        ROUND(SUM(amount_usd), 2) AS total_funding_usd,
        RANK() OVER (PARTITION BY funding_year ORDER BY SUM(amount_usd) DESC) AS sector_rank
    FROM silver_startup_funding
    GROUP BY funding_year, sector
)
WHERE sector_rank = 1
ORDER BY funding_year;

-- 18. Investors who have backed startups across the widest range of sectors
SELECT investor, COUNT(DISTINCT sector) AS distinct_sectors_backed
FROM (
    SELECT EXPLODE(SPLIT(investors_name, ',\\s*')) AS investor, sector
    FROM silver_startup_funding
)
GROUP BY investor
ORDER BY distinct_sectors_backed DESC
LIMIT 10;

-- 19. Quarter-over-quarter funding totals (across all years)
SELECT funding_year, funding_quarter, ROUND(SUM(amount_usd), 2) AS total_funding_usd
FROM silver_startup_funding
GROUP BY funding_year, funding_quarter
ORDER BY funding_year, funding_quarter;

-- 20. Year-over-year percentage growth in total funding
SELECT
    funding_year,
    total_funding_usd,
    ROUND(
        100.0 * (total_funding_usd - LAG(total_funding_usd) OVER (ORDER BY funding_year))
        / LAG(total_funding_usd) OVER (ORDER BY funding_year),
        2
    ) AS yoy_growth_pct
FROM (
    SELECT funding_year, SUM(amount_usd) AS total_funding_usd
    FROM silver_startup_funding
    GROUP BY funding_year
);

-- 21. Median-style percentile funding amount per sector (approx 50th percentile)
SELECT sector, ROUND(PERCENTILE_APPROX(amount_usd, 0.5), 2) AS median_funding_usd
FROM silver_startup_funding
GROUP BY sector
ORDER BY median_funding_usd DESC;

-- 22. Top 3 investment types by average ticket size
SELECT investment_type, ROUND(AVG(amount_usd), 2) AS avg_ticket_size_usd
FROM silver_startup_funding
GROUP BY investment_type
ORDER BY avg_ticket_size_usd DESC
LIMIT 3;

-- 23. Startups that raised funding in more than one distinct city (data-quality-style check)
SELECT startup_name, COUNT(DISTINCT city) AS distinct_cities
FROM silver_startup_funding
GROUP BY startup_name
HAVING COUNT(DISTINCT city) > 1
ORDER BY distinct_cities DESC;

-- 24. Cumulative running total of funding by year (useful for trend charts)
SELECT
    funding_year,
    total_funding_usd,
    SUM(total_funding_usd) OVER (ORDER BY funding_year) AS cumulative_funding_usd
FROM (
    SELECT funding_year, ROUND(SUM(amount_usd), 2) AS total_funding_usd
    FROM silver_startup_funding
    GROUP BY funding_year
);
