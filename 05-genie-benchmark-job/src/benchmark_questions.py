# benchmark_questions.py — Benchmark suite for the Acme CPI Genie Space.
#
# Each question has:
#   - id: unique identifier
#   - question: natural-language question to ask Genie
#   - expected_sql: the SQL Genie should generate (or equivalent)
#   - validation_query: SQL that validates the Genie result independently
#   - category: grouping for reporting

CATALOG = "my_catalog"
SCHEMA = "genie_ready"
TABLE = "cpi_world_country_aggregates"
FQN = f"{CATALOG}.{SCHEMA}.{TABLE}"

BENCHMARK_QUESTIONS = [
    {
        "id": "BQ-001",
        "question": "What is the most recent CPI value for Bermuda?",
        "category": "single_value",
        "validation_query": f"""
            SELECT value FROM {FQN}
            WHERE country_name = 'Bermuda' AND value IS NOT NULL
            ORDER BY year DESC LIMIT 1
        """,
    },
    {
        "id": "BQ-002",
        "question": "Which country had the highest CPI in 2020?",
        "category": "ranking",
        "validation_query": f"""
            SELECT country_name, value FROM {FQN}
            WHERE year = 2020 AND value IS NOT NULL
            ORDER BY value DESC LIMIT 1
        """,
    },
    {
        "id": "BQ-003",
        "question": "How many countries are in the dataset?",
        "category": "aggregation",
        "validation_query": f"""
            SELECT COUNT(DISTINCT country_name) AS num_countries FROM {FQN}
        """,
    },
    {
        "id": "BQ-004",
        "question": "Show the CPI trend for Bermuda from 2015 to 2023",
        "category": "time_series",
        "validation_query": f"""
            SELECT year, value FROM {FQN}
            WHERE country_name = 'Bermuda'
            AND year BETWEEN 2015 AND 2023
            AND value IS NOT NULL
            ORDER BY year
        """,
    },
    {
        "id": "BQ-005",
        "question": "Compare the CPI of United States and United Kingdom in 2019",
        "category": "comparison",
        "validation_query": f"""
            SELECT country_name, value FROM {FQN}
            WHERE country_name IN ('United States', 'United Kingdom')
            AND year = 2019 AND value IS NOT NULL
        """,
    },
    {
        "id": "BQ-006",
        "question": "What is the average CPI across all countries for each year?",
        "category": "aggregation",
        "validation_query": f"""
            SELECT year, AVG(value) AS avg_cpi FROM {FQN}
            WHERE value IS NOT NULL
            GROUP BY year ORDER BY year
        """,
    },
    {
        "id": "BQ-007",
        "question": "List the top 5 countries with the lowest CPI in the most recent year",
        "category": "ranking",
        "validation_query": f"""
            SELECT country_name, value FROM {FQN}
            WHERE year = (SELECT MAX(year) FROM {FQN} WHERE value IS NOT NULL)
            AND value IS NOT NULL AND value > 0
            ORDER BY value ASC LIMIT 5
        """,
    },
    {
        "id": "BQ-008",
        "question": "What years of data are available?",
        "category": "metadata",
        "validation_query": f"""
            SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM {FQN}
        """,
    },
    {
        "id": "BQ-009",
        "question": "What was the year-over-year change in CPI for Bermuda in 2022?",
        "category": "calculation",
        "validation_query": f"""
            SELECT year, value,
                   value - LAG(value) OVER (ORDER BY year) AS yoy_change
            FROM {FQN}
            WHERE country_name = 'Bermuda' AND value IS NOT NULL
            ORDER BY year
        """,
    },
    {
        "id": "BQ-010",
        "question": "Which countries have CPI data for all years in the dataset?",
        "category": "complex",
        "validation_query": f"""
            SELECT country_name, COUNT(DISTINCT year) AS years_count
            FROM {FQN} WHERE value IS NOT NULL
            GROUP BY country_name
            HAVING COUNT(DISTINCT year) = (SELECT COUNT(DISTINCT year) FROM {FQN})
        """,
    },
]
