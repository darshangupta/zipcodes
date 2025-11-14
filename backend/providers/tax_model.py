"""County tax data provider."""
import pandas as pd
from pathlib import Path
from datetime import datetime
import re

from backend.utils import save_parquet

# State name to code mapping
STATE_NAME_TO_CODE = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI",
    "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME",
    "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE",
    "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM",
    "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX",
    "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY"
}


def _parse_county_name(county: str) -> str:
    """Normalize county name (remove 'County' suffix, title case)."""
    county = str(county).strip()
    county = re.sub(r'\s+County\s*$', '', county, flags=re.IGNORECASE)
    return county.title()


def fetch(force: bool = False, raw_csv_path: str = "backend/raw/county_property_tax.csv",
          cache_path: str = "backend/cache/county_tax.parquet") -> pd.DataFrame:
    """Fetch and normalize county tax data.
    
    Args:
        force: If True, re-fetch even if cache exists
        raw_csv_path: Path to raw county tax CSV
        cache_path: Path to write cached parquet
        
    Returns:
        DataFrame with columns: county_fips, eff_tax_rate, state, county_name
        Note: county_fips will be empty - we'll need a crosswalk to map county names to FIPS
    """
    cache_file = Path(cache_path)
    if cache_file.exists() and not force:
        return load(cache_path)
    
    raw_file = Path(raw_csv_path)
    if not raw_file.exists():
        # Return empty DataFrame if file doesn't exist
        print(f"County tax CSV not found: {raw_csv_path}, skipping")
        return pd.DataFrame(columns=["county_fips", "eff_tax_rate", "state", "county_name"])
    
    # Read CSV
    df = pd.read_csv(raw_file)
    
    # Normalize column names
    col_map = {
        "State": "state",
        "County": "county_name",
        "Effective Property Tax Rate (2023)": "eff_tax_rate_str",
    }
    
    for old, new in col_map.items():
        if old in df.columns:
            df = df.rename(columns={old: new})
    
    # Parse effective tax rate (remove % and convert to decimal)
    if "eff_tax_rate_str" in df.columns:
        df["eff_tax_rate"] = df["eff_tax_rate_str"].astype(str).str.replace("%", "").str.strip()
        df["eff_tax_rate"] = pd.to_numeric(df["eff_tax_rate"], errors="coerce") / 100
        df = df.drop(columns=["eff_tax_rate_str"])
    else:
        df["eff_tax_rate"] = 0.015  # Default
    
    # Normalize county names
    if "county_name" in df.columns:
        df["county_name"] = df["county_name"].apply(_parse_county_name)
    
    # Normalize state codes - convert full state names to 2-letter codes
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.strip().str.upper()
        # Convert full state names to codes (e.g., "ALABAMA" -> "AL")
        df["state"] = df["state"].map(STATE_NAME_TO_CODE).fillna(df["state"])
        # If already a 2-letter code, keep it; otherwise try to match
        # This handles cases where the CSV might already have codes
    
    # For now, county_fips will be empty - we'll need a crosswalk
    # The join in run() will use county_name + state to match
    df["county_fips"] = ""  # Placeholder - will be filled by crosswalk
    
    # Select columns
    result = df[["state", "county_name", "eff_tax_rate", "county_fips"]].copy()
    result = result.dropna(subset=["state", "county_name", "eff_tax_rate"])
    
    # Write cache
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    save_parquet(result, cache_path)
    
    pulled_at = datetime.utcnow().isoformat() + "Z"
    print(f"county_tax cached: rows={len(result)} pulled_at={pulled_at}")
    
    return result


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

