"""
Dagster Definitions — The entry point of the entire pipeline.

This file wires everything together:
1. Assets      — what data to produce (Bronze, Silver, Gold)
2. Resources   — how to store data (DuckDB IOManager)
3. Schedules   — when to run (later)
4. Sensors     — what triggers runs (later)

Dagster reads THIS file to understand your entire project.
"""

import os
from dagster import Definitions, load_assets_from_modules
from dagster_duckdb_pandas import DuckDBPandasIOManager

# Import our asset modules
from etl_pipeline.assets.bronze import raw_ingestion

# ---------------------------------------------------------------------------
# Load all assets from the bronze module
# ---------------------------------------------------------------------------
bronze_assets = load_assets_from_modules([raw_ingestion])

# ---------------------------------------------------------------------------
# Configure DuckDB IOManager
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

defs = Definitions(
    assets=[*bronze_assets],
    resources={
        "io_manager": DuckDBPandasIOManager(
            database=os.path.join(PROJECT_ROOT, "warehouse", "pipeline.duckdb"),
        ),
    },
)
