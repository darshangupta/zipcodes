"""Census ACS data provider for ZCTA-level demographics."""
import pandas as pd
from pathlib import Path
from datetime import datetime

from backend.utils import save_parquet


def fetch(force: bool = False, raw_csv_path: str = "backend/raw/acs_zcta.csv",
          cache_path: str = "backend/cache/acs_zcta.parquet") -> pd.DataFrame:
    """Fetch and normalize Census ACS data.
    
    Args:
        force: If True, re-fetch even if cache exists
        raw_csv_path: Path to raw ACS CSV
        cache_path: Path to write cached parquet
        
    Returns:
        DataFrame with columns: zcta, vacancy_rate, renter_occupied_pct, median_hh_income
        Returns empty DataFrame if raw file doesn't exist
    """
    cache_file = Path(cache_path)
    if cache_file.exists() and not force:
        return load(cache_path)
    
    raw_file = Path(raw_csv_path)
    if not raw_file.exists():
        # Return empty DataFrame with correct structure
        print(f"ACS raw file not found: {raw_csv_path}, skipping")
        return pd.DataFrame(columns=["zcta", "vacancy_rate", "renter_occupied_pct", "median_hh_income"])
    
    # Read and normalize
    df = pd.read_csv(raw_file)
    
    # Normalize column names
    col_map = {
        "zcta": ["zcta", "zip_code", "zip"],
        "vacancy_rate": ["vacancy_rate", "vacancy"],
        "renter_occupied_pct": ["renter_occupied_pct", "renter_pct", "renter_occupied"],
        "median_hh_income": ["median_hh_income", "median_income", "income"]
    }
    
    for target, possible in col_map.items():
        for col in possible:
            if col in df.columns and target not in df.columns:
                df = df.rename(columns={col: target})
                break
    
    # Ensure zcta is string
    if "zcta" in df.columns:
        df["zcta"] = df["zcta"].astype(str).str.zfill(5)
    
    # Select columns (create missing ones as NaN)
    result_cols = ["zcta", "vacancy_rate", "renter_occupied_pct", "median_hh_income"]
    for col in result_cols:
        if col not in df.columns:
            df[col] = None
    
    df = df[result_cols].copy()
    
    # Convert numeric columns
    for col in ["vacancy_rate", "renter_occupied_pct", "median_hh_income"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    df = df.dropna(subset=["zcta"])
    
    # Write cache
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, cache_path)
    
    pulled_at = datetime.utcnow().isoformat() + "Z"
    print(f"acs_zcta cached: rows={len(df)} pulled_at={pulled_at}")
    
    return df


def load(cache_path: str = "backend/cache/acs_zcta.parquet") -> pd.DataFrame:
    """Load cached Census ACS data.
    
    Args:
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: zcta, vacancy_rate, renter_occupied_pct, median_hh_income
        Returns empty DataFrame if cache doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        return pd.DataFrame(columns=["zcta", "vacancy_rate", "renter_occupied_pct", "median_hh_income"])
    
    df = pd.read_parquet(cache_path)
    # Ensure zcta is string
    if "zcta" in df.columns:
        df["zcta"] = df["zcta"].astype(str).str.zfill(5)
    return df

