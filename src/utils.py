"""
utils.py
--------
Reusable helper functions shared across the Bronze, Silver and Gold layers:

- Centralized logging setup
- Spark SQL / column expression helpers to standardize city names,
  investor names, startup names, clean funding amounts and parse dates.

Keeping these here avoids duplicated code across bronze.py / silver.py /
gold.py, per the project's coding standards.
"""

import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import config  # noqa: E402

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    """
    Create (or reuse) a logger that writes to both the console and to
    logs/pipeline.log, as required by the project's logging standard.
    """
    config.ensure_directories_exist()
    logger = logging.getLogger(name)

    if logger.handlers:
        # Logger already configured (avoids duplicate handlers on re-import)
        return logger

    logger.setLevel(config.LOG_LEVEL)
    formatter = logging.Formatter(config.LOG_FORMAT)

    file_handler = logging.FileHandler(config.LOG_FILE_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


# ---------------------------------------------------------------------------
# Standardization dictionaries
# ---------------------------------------------------------------------------
# Canonical city name -> list of known messy variants (lower-cased for matching)
CITY_ALIASES = {
    "Bangalore": ["bangalore", "bengaluru", "blr"],
    "Mumbai": ["mumbai", "bombay"],
    "Delhi": ["delhi", "new delhi", "ncr"],
    "Gurugram": ["gurugram", "gurgaon", "ggn"],
    "Pune": ["pune"],
    "Hyderabad": ["hyderabad"],
    "Chennai": ["chennai", "madras"],
    "Noida": ["noida"],
    "Kolkata": ["kolkata", "calcutta"],
    "Ahmedabad": ["ahmedabad"],
}

# Canonical investor name -> list of known messy variants (lower-cased)
INVESTOR_ALIASES = {
    "Sequoia Capital": ["sequoia capital", "sequoia", "sequoia capital india"],
    "Tiger Global": ["tiger global", "tiger global management"],
    "SoftBank": ["softbank", "softbank vision fund", "soft bank"],
    "Accel Partners": ["accel partners", "accel", "accel india"],
    "Matrix Partners": ["matrix partners", "matrix partners india"],
    "Lightspeed Venture Partners": ["lightspeed venture partners", "lightspeed", "lightspeed india"],
    "Nexus Venture Partners": ["nexus venture partners", "nexus venture", "nexus partners"],
    "Blume Ventures": ["blume ventures", "blume"],
    "Falcon Edge Capital": ["falcon edge capital", "falcon edge"],
    "Kalaari Capital": ["kalaari capital", "kalaari"],
    "Elevation Capital": ["elevation capital", "elevation", "saif partners"],
    "Steadview Capital": ["steadview capital", "steadview"],
    "Y Combinator": ["y combinator", "yc"],
    "Chiratae Ventures": ["chiratae ventures", "chiratae", "idg ventures"],
    "Info Edge": ["info edge", "infoedge", "info edge ventures"],
}


def _build_replace_expr(col: Column, aliases: dict) -> Column:
    """
    Build a chained `F.when(...)` expression that maps any known messy
    variant (case-insensitive, trimmed) to its canonical name. Falls back
    to a trimmed, title-cased version of the original value when no
    alias matches, so unseen values degrade gracefully instead of
    becoming null.
    """
    normalized = F.trim(F.lower(col))
    expr = None
    for canonical, variants in aliases.items():
        condition = normalized.isin(variants)
        expr = F.when(condition, F.lit(canonical)) if expr is None else expr.when(condition, F.lit(canonical))
    expr = expr.otherwise(F.initcap(F.trim(col)))
    return expr


def standardize_city_column(df: DataFrame, column_name: str = "city") -> DataFrame:
    """Standardize messy city name variants into a single canonical value."""
    return df.withColumn(column_name, _build_replace_expr(F.col(column_name), CITY_ALIASES))


def standardize_investor_column(df: DataFrame, column_name: str = "investors_name") -> DataFrame:
    """
    Standardize investor name(s). A row may contain multiple comma-separated
    investors (e.g. "Sequoia, Tiger Global"), so each token is split,
    trimmed, mapped to its canonical name individually, de-duplicated, and
    re-joined.
    """
    split_col = F.split(F.col(column_name), r"\s*,\s*")

    def map_single(token: Column) -> Column:
        return _build_replace_expr(token, INVESTOR_ALIASES)

    mapped_array = F.transform(split_col, map_single)
    deduped_array = F.array_distinct(mapped_array)
    return df.withColumn(column_name, F.array_join(deduped_array, ", "))


def standardize_startup_name_column(df: DataFrame, column_name: str = "startup_name") -> DataFrame:
    """Trim whitespace and apply consistent title-casing to startup names."""
    return df.withColumn(column_name, F.initcap(F.trim(F.col(column_name))))


def clean_funding_amount_column(
    df: DataFrame, column_name: str = "amount_usd", output_name: str = "amount_usd"
) -> DataFrame:
    """
    Clean a messy funding-amount string column into a proper double
    (USD) column.

    Handles:
        - Currency symbols ($ and ₹) and thousands separators
        - "N/A", "Undisclosed", empty strings -> null
        - Values prefixed with "USD"
        - INR amounts (₹ symbol) converted to USD using a fixed rate
    """
    raw = F.col(column_name)
    is_inr = raw.contains("₹")

    # Strip everything except digits and decimal point
    digits_only = F.regexp_replace(raw, r"[^0-9.]", "")
    numeric_value = F.when(digits_only == "", None).otherwise(digits_only.cast(DoubleType()))

    converted = F.when(is_inr, numeric_value * F.lit(config.CURRENCY_INR_TO_USD_RATE)).otherwise(numeric_value)

    # Anything that was "N/A", "Undisclosed" or blank collapses to null above
    return df.withColumn(output_name, converted)


def parse_flexible_date_column(df: DataFrame, column_name: str = "date", output_name: str = "date") -> DataFrame:
    """
    Parse a date column that may arrive in several different formats
    (yyyy-MM-dd, dd/MM/yyyy, MM-dd-yyyy, dd-MMM-yyyy, yyyy/MM/dd) into a
    single canonical DateType column. Unparseable or missing values become
    null (and are dropped later as invalid records).
    """
    raw = F.col(column_name)
    candidates = [
        F.to_date(raw, "yyyy-MM-dd"),
        F.to_date(raw, "dd/MM/yyyy"),
        F.to_date(raw, "MM-dd-yyyy"),
        F.to_date(raw, "dd-MMM-yyyy"),
        F.to_date(raw, "yyyy/MM/dd"),
    ]
    coalesced = F.coalesce(*candidates)
    return df.withColumn(output_name, coalesced)


def null_if_blank(df: DataFrame, column_name: str) -> DataFrame:
    """Convert empty / whitespace-only strings to proper nulls for a column."""
    col = F.col(column_name)
    return df.withColumn(column_name, F.when(F.trim(col) == "", None).otherwise(F.trim(col)))
