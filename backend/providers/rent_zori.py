"""ZORI rent data provider."""
import pandas as pd
from pathlib import Path
from datetime import datetime

from backend.utils import save_parquet


def fetch(force: bool = False, raw_csv_path: str = "backend/raw/zori_zip.csv",
          cache_path: str = "backend/cache/zori_zip.parquet") -> pd.DataFrame:
    """Fetch and normalize ZORI rent data.
    
    Args:
        force: If True, re-fetch even if cache exists
        raw_csv_path: Path to raw ZORI CSV
        cache_path: Path to write cached parquet
        
    Returns:
        DataFrame with columns: zip, median_rent
    """
    cache_file = Path(cache_path)
    if cache_file.exists() and not force:
        return load(cache_path)
    
    raw_file = Path(raw_csv_path)
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw ZORI CSV not found: {raw_csv_path}")
    
    # Read and normalize
    df = pd.read_csv(raw_file)
    
    # Normalize column names
    if "zip" not in df.columns and "zip_code" in df.columns:
        df = df.rename(columns={"zip_code": "zip"})
    if "median_rent" not in df.columns and "rent" in df.columns:
        df = df.rename(columns={"rent": "median_rent"})
    
    # Ensure required columns
    required_cols = ["zip", "median_rent"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in ZORI data: {missing}. Found: {df.columns.tolist()}")
    
    # Normalize zip to zero-padded string
    df["zip"] = df["zip"].astype(str).str.zfill(5)
    
    # Select and normalize
    df = df[required_cols].copy()
    df["median_rent"] = pd.to_numeric(df["median_rent"], errors="coerce")
    df = df.dropna(subset=["zip", "median_rent"])
    
    # Write cache
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, cache_path)
    
    pulled_at = datetime.utcnow().isoformat() + "Z"
    print(f"zori_zip cached: rows={len(df)} pulled_at={pulled_at}")
    
    return df


def load(cache_path: str = "backend/cache/zori_zip.parquet") -> pd.DataFrame:
    """Load cached ZORI rent data.
    
    Args:
        cache_path: Path to cached parquet
        
    Returns:
        DataFrame with columns: zip, median_rent
        Raises FileNotFoundError if cache doesn't exist
    """
    path = Path(cache_path)
    if not path.exists():
        raise FileNotFoundError(f"ZORI cache not found: {cache_path}. Run ingest first.")
    
    df = pd.read_parquet(cache_path)
    # Ensure zip is zero-padded string
    if "zip" in df.columns:
        df["zip"] = df["zip"].astype(str).str.zfill(5)
    return df

