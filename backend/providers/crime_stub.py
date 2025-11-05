"""Stub crime data provider."""
import pandas as pd
from pathlib import Path
from typing import Optional


def fetch(force: bool = False, cache_path: str = "backend/cache/crime_county.parquet") -> pd.DataFrame:
    """Fetch county crime data (stub - returns empty for now).
    
    Args:
        force: If True, re-fetch even if cache exists
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: county_fips, violent_per_100k
        Returns empty DataFrame for now
    """
    return pd.DataFrame(columns=["county_fips", "violent_per_100k"])


def load(cache_path: str = "backend/cache/crime_county.parquet") -> pd.DataFrame:
    """Load cached county crime data.
    
    Args:
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: county_fips, violent_per_100k
        Returns empty DataFrame if cache doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        return pd.DataFrame(columns=["county_fips", "violent_per_100k"])
    
    try:
        df = pd.read_parquet(cache_path)
        # Ensure county_fips is string
        if "county_fips" in df.columns:
            df["county_fips"] = df["county_fips"].astype(str)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["county_fips", "violent_per_100k"])


def load_crime(zips: Optional[pd.Series] = None) -> pd.DataFrame:
    """Load crime data by ZIP (stub implementation).
    
    Args:
        zips: Optional Series of zip codes to get crime data for
        
    Returns:
        DataFrame with columns: zip, crime_index
        Default crime_index is 1.0 for all zips
    """
    if zips is None or len(zips) == 0:
        return pd.DataFrame(columns=["zip", "crime_index"])
    
    df = pd.DataFrame({"zip": zips.unique()})
    df["zip"] = df["zip"].astype(str).str.zfill(5)
    df["crime_index"] = 1.0  # Default neutral crime index
    
    return df

