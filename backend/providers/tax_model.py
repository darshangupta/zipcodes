"""County tax data provider."""
import pandas as pd
from pathlib import Path
from typing import Optional


def fetch(force: bool = False, cache_path: str = "backend/cache/county_tax.parquet") -> pd.DataFrame:
    """Fetch county tax data (placeholder - expects manual maintenance of parquet).
    
    Args:
        force: If True, re-fetch even if cache exists (no-op for now)
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: county_fips, eff_tax_rate
        Returns empty DataFrame if cache doesn't exist
    """
    return load(cache_path)


def load(cache_path: str = "backend/cache/county_tax.parquet") -> pd.DataFrame:
    """Load cached county tax data.
    
    Args:
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: county_fips, eff_tax_rate
        Returns empty DataFrame if cache doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        return pd.DataFrame(columns=["county_fips", "eff_tax_rate"])
    
    try:
        df = pd.read_parquet(cache_path)
        # Ensure county_fips is string
        if "county_fips" in df.columns:
            df["county_fips"] = df["county_fips"].astype(str)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["county_fips", "eff_tax_rate"])

