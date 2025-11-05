"""Geographic crosswalk providers for ZIP→county and ZIP→ZCTA mappings."""
import pandas as pd
from pathlib import Path
from typing import Optional


def load_zip_county(cache_path: str = "backend/cache/crosswalk_zip_county.parquet") -> pd.DataFrame:
    """Load ZIP→county crosswalk from cached parquet.
    
    Args:
        cache_path: Path to cached crosswalk parquet
        
    Returns:
        DataFrame with columns: zip, county_fips
        Returns empty DataFrame if file doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        return pd.DataFrame(columns=["zip", "county_fips"])
    
    try:
        df = pd.read_parquet(path)
        # Ensure zip is zero-padded string
        if "zip" in df.columns:
            df["zip"] = df["zip"].astype(str).str.zfill(5)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["zip", "county_fips"])


def load_zip_zcta(cache_path: str = "backend/cache/crosswalk_zip_zcta.parquet") -> pd.DataFrame:
    """Load ZIP→ZCTA crosswalk from cached parquet.
    
    Args:
        cache_path: Path to cached crosswalk parquet
        
    Returns:
        DataFrame with columns: zip, zcta
        Returns empty DataFrame if file doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        return pd.DataFrame(columns=["zip", "zcta"])
    
    try:
        df = pd.read_parquet(path)
        # Ensure zip is zero-padded string
        if "zip" in df.columns:
            df["zip"] = df["zip"].astype(str).str.zfill(5)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["zip", "zcta"])

