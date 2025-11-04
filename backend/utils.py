"""Utility functions for reading config and writing data."""
import yaml
import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, Any


def read_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Read YAML configuration file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def save_parquet(df: pd.DataFrame, output_path: str) -> None:
    """Save DataFrame to Parquet file."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def write_duckdb(df: pd.DataFrame, db_path: str, table_name: str = "zipview") -> None:
    """Write DataFrame to DuckDB table."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.register("df_temp", df)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_temp")
    conn.unregister("df_temp")
    conn.close()

