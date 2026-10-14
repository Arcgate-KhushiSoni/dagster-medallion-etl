"""
Bronze Layer — Generic Raw Data Extraction from ANY REST API

HOW TO USE:
1. Put your API URL in the @asset function
2. Run `dagster dev`
3. Click "Materialize" in the UI
4. Data gets saved to DuckDB automatically

You can add as many @asset functions as you have API endpoints.
"""

import requests
import pandas as pd
from dagster import asset, get_dagster_logger, MetadataValue


# ---------------------------------------------------------------------------
# Helper: Fetch data from any API URL
# ---------------------------------------------------------------------------
def extract_from_api(url: str, params: dict = None, headers: dict = None) -> list:
    """
    Call any REST API and return the JSON response.

    Args:
        url:     Your API endpoint (e.g., "https://dummyjson.com/products")
        params:  Query parameters (e.g., {"limit": 100, "page": 1})
        headers: Request headers (e.g., {"Authorization": "Bearer xxx"})

    Returns:
        JSON response (list or dict)
    """
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


# ===========================================================================
#  YOUR ASSETS BELOW — Add your API URLs here
# ===========================================================================


# ---------------------------------------------------------------------------
# Asset 1: Change the URL to your API
# ---------------------------------------------------------------------------
@asset(
    group_name="bronze",
    description="Raw data extracted from API endpoint 1",
    compute_kind="api",
)
def raw_data_1(context) -> pd.DataFrame:
    """Extract data from your first API endpoint."""
    logger = get_dagster_logger()

    #  CHANGE THIS — Put your API URL here
    url = "https://dummyjson.com/products"

    #  CHANGE THIS — If your API needs query params
    params = {"limit": 0}  # limit=0 means get all in DummyJSON

    #  CHANGE THIS — If your API needs auth headers
    headers = None

    logger.info(f"Extracting data from: {url}")
    data = extract_from_api(url, params=params, headers=headers)

    #  CHANGE THIS — Adjust based on your API response structure
    records = data["products"]

    df = pd.DataFrame(records)
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")

    # Metadata — shows up in Dagster UI
    context.add_output_metadata({
        "row_count": len(df),
        "columns": list(df.columns),
        "preview": MetadataValue.md(df.head(5).to_markdown()),
    })

    return df


# ---------------------------------------------------------------------------
# Asset 2: Copy this block for your second API endpoint
# ---------------------------------------------------------------------------
@asset(
    group_name="bronze",
    description="Raw data extracted from API endpoint 2",
    compute_kind="api",
)
def raw_data_2(context) -> pd.DataFrame:
    """Extract data from your second API endpoint."""
    logger = get_dagster_logger()

    #  CHANGE THIS
    url = "https://dummyjson.com/users"
    params = {"limit": 0}
    headers = None

    logger.info(f"Extracting data from: {url}")
    data = extract_from_api(url, params=params, headers=headers)

    #  CHANGE THIS — adjust the key based on your API response
    records = data["users"]

    df = pd.DataFrame(records)
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")

    context.add_output_metadata({
        "row_count": len(df),
        "columns": list(df.columns),
        "preview": MetadataValue.md(df.head(5).to_markdown()),
    })

    return df


# ---------------------------------------------------------------------------
# Asset 3: Copy this block for your third API endpoint
# ---------------------------------------------------------------------------
@asset(
    group_name="bronze",
    description="Raw data extracted from API endpoint 3",
    compute_kind="api",
)
def raw_data_3(context) -> pd.DataFrame:
    """Extract data from your third API endpoint."""
    logger = get_dagster_logger()

    #  CHANGE THIS
    url = "https://dummyjson.com/carts"
    params = {"limit": 0}
    headers = None

    logger.info(f"Extracting data from: {url}")
    data = extract_from_api(url, params=params, headers=headers)

    #  CHANGE THIS — adjust the key based on your API response
    records = data["carts"]

    df = pd.DataFrame(records)
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")

    context.add_output_metadata({
        "row_count": len(df),
        "columns": list(df.columns),
        "preview": MetadataValue.md(df.head(5).to_markdown()),
    })

    return df
