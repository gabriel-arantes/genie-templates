-- ============================================================================
-- BMA CPI Dashboard — SQL Queries
-- These queries power the AI/BI Lakeview Dashboard widgets.
-- Table: ${catalog}.${schema}.cpi_world_country_aggregates
-- ============================================================================

-- Widget 1: CPI Overview — Latest year, all countries
-- Name: cpi_latest_year
SELECT
    country_name,
    country_code,
    indicator_name,
    year,
    value AS cpi_value
FROM ${catalog}.${schema}.cpi_world_country_aggregates
WHERE year = (
    SELECT MAX(year)
    FROM ${catalog}.${schema}.cpi_world_country_aggregates
    WHERE value IS NOT NULL
)
AND value IS NOT NULL
ORDER BY value DESC;

-- Widget 2: CPI Trend — Time series for selected countries
-- Name: cpi_trend
SELECT
    country_name,
    year,
    value AS cpi_value
FROM ${catalog}.${schema}.cpi_world_country_aggregates
WHERE country_name IN ('Bermuda', 'United States', 'United Kingdom', 'Canada', 'World')
AND value IS NOT NULL
ORDER BY country_name, year;

-- Widget 3: Top 10 Highest CPI (latest year)
-- Name: top10_highest_cpi
SELECT
    country_name,
    value AS cpi_value,
    year
FROM ${catalog}.${schema}.cpi_world_country_aggregates
WHERE year = (
    SELECT MAX(year)
    FROM ${catalog}.${schema}.cpi_world_country_aggregates
    WHERE value IS NOT NULL
)
AND value IS NOT NULL
ORDER BY value DESC
LIMIT 10;

-- Widget 4: Top 10 Lowest CPI (latest year)
-- Name: top10_lowest_cpi
SELECT
    country_name,
    value AS cpi_value,
    year
FROM ${catalog}.${schema}.cpi_world_country_aggregates
WHERE year = (
    SELECT MAX(year)
    FROM ${catalog}.${schema}.cpi_world_country_aggregates
    WHERE value IS NOT NULL
)
AND value IS NOT NULL
AND value > 0
ORDER BY value ASC
LIMIT 10;

-- Widget 5: Year-over-Year Change for Bermuda
-- Name: bermuda_yoy_change
SELECT
    year,
    value AS cpi_value,
    value - LAG(value) OVER (ORDER BY year) AS yoy_change,
    ROUND(
        ((value - LAG(value) OVER (ORDER BY year)) /
         NULLIF(LAG(value) OVER (ORDER BY year), 0)) * 100, 2
    ) AS yoy_change_pct
FROM ${catalog}.${schema}.cpi_world_country_aggregates
WHERE country_name = 'Bermuda'
AND value IS NOT NULL
ORDER BY year;

-- Widget 6: Summary Statistics
-- Name: summary_stats
SELECT
    COUNT(DISTINCT country_name) AS num_countries,
    COUNT(DISTINCT year) AS num_years,
    MIN(year) AS earliest_year,
    MAX(year) AS latest_year,
    COUNT(*) AS total_records
FROM ${catalog}.${schema}.cpi_world_country_aggregates
WHERE value IS NOT NULL;
