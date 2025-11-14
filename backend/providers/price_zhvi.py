"""ZHVI price data provider."""
import pandas as pd
from pathlib import Path
from datetime import datetime

from backend.utils import save_parquet


def _parse_date_column(col: str) -> datetime:
    """Parse date column name (YYYY-MM-DD or YYYY-MM format)."""
    try:
        # Try YYYY-MM-DD format
        return datetime.strptime(col, "%Y-%m-%d")
    except ValueError:
        try:
            # Try YYYY-MM format
            return datetime.strptime(col, "%Y-%m")
        except ValueError:
            return None


def _find_latest_date_column(df: pd.DataFrame) -> str:
    """Find the latest date column in the DataFrame."""
    date_cols = []
    for col in df.columns:
        parsed = _parse_date_column(col)
        if parsed:
            date_cols.append((col, parsed))
    
    if not date_cols:
        raise ValueError("No date columns found in ZHVI data")
    
    # Sort by date and get the latest
    date_cols.sort(key=lambda x: x[1], reverse=True)
    return date_cols[0][0]


def fetch(force: bool = False, raw_csv_path: str = "backend/raw/zhvi_zip.csv",
          cache_path: str = "backend/cache/zhvi_zip.parquet",
          states_allowlist: list = None) -> pd.DataFrame:
    """Fetch and normalize ZHVI price data at ZIP level.
    
    Args:
        force: If True, re-fetch even if cache exists
        raw_csv_path: Path to raw ZHVI ZIP CSV
        cache_path: Path to write cached parquet
        states_allowlist: Optional list of state codes to filter by
        
    Returns:
        DataFrame with columns: zip, state, median_price
    """
    cache_file = Path(cache_path)
    if cache_file.exists() and not force:
        return load(cache_path)
    
    raw_file = Path(raw_csv_path)
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw ZHVI ZIP CSV not found: {raw_csv_path}")
    
    # Read CSV
    df = pd.read_csv(raw_file)
    
    # Filter for ZIP-level data (RegionType == 'Zip' or 'zip')
    if "RegionType" in df.columns:
        zip_mask = (df["RegionType"] == "Zip") | (df["RegionType"] == "zip")
        if zip_mask.any():
            df = df[zip_mask].copy()
        else:
            raise ValueError(f"No ZIP-level data found in {raw_csv_path}. RegionType values: {df['RegionType'].unique()}")
    else:
        raise ValueError("RegionType column not found in ZHVI data")
    
    # Find the latest date column
    latest_date_col = _find_latest_date_column(df)
    latest_date = _parse_date_column(latest_date_col)
    
    # Build result DataFrame
    result = pd.DataFrame()
    
    # Extract zip from RegionName (should be 5-digit ZIP code)
    if "RegionName" in df.columns:
        result["zip"] = df["RegionName"].astype(str).str.strip()
        # Ensure it's 5 digits (zero-pad if needed)
        result["zip"] = result["zip"].str.zfill(5)
    else:
        raise ValueError("RegionName column not found in ZHVI data")
    
    # Extract state from StateName if available
    if "StateName" in df.columns:
        result["state"] = df["StateName"].astype(str).str.strip().str.upper()
    else:
        raise ValueError("StateName column not found in ZHVI data")
    
    # Extract median_price from latest date column
    result["median_price"] = pd.to_numeric(df[latest_date_col], errors="coerce")
    
    # Drop rows with missing data
    result = result.dropna(subset=["zip", "median_price", "state"])
    
    # Filter to states_allowlist if provided
    if states_allowlist:
        result = result[result["state"].isin(states_allowlist)].copy()
    
    # Ensure zip is exactly 5 digits (filter out non-ZIP rows)
    result = result[result["zip"].str.len() == 5]
    result = result[result["zip"].str.isdigit()]
    
    # Write cache
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    save_parquet(result, cache_path)
    
    pulled_at = datetime.utcnow().isoformat() + "Z"
    latest_date_str = latest_date.strftime("%Y-%m-%d") if latest_date else latest_date_col
    print(f"zhvi_zip cached: rows={len(result)} pulled_at={pulled_at} latest_date={latest_date_str}")
    
    return result


def load(cache_path: str = "backend/cache/zhvi_zip.parquet") -> pd.DataFrame:
    """Load cached ZHVI price data.
    
    Args:
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: zip, state, median_price
        Raises FileNotFoundError if cache doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        raise FileNotFoundError(f"ZHVI cache not found: {cache_path}. Run ingest first.")
    
    df = pd.read_parquet(cache_path)
    # Ensure zip is zero-padded string
    if "zip" in df.columns:
        df["zip"] = df["zip"].astype(str).str.zfill(5)
    return df

