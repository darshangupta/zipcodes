"""Data loading functions for baselines and signals."""
import pandas as pd
from typing import List, Optional


def load_baselines(zips: pd.DataFrame, states: List[str]) -> pd.DataFrame:
    """Load baseline data for specified zip codes and states.
    
    Args:
        zips: DataFrame with zip code data
        states: List of state codes to filter by
        
    Returns:
        DataFrame with baseline data filtered by states
    """
    # Filter by states if zip column exists
    if "state" in zips.columns:
        baselines = zips[zips["state"].isin(states)].copy()
    else:
        baselines = zips.copy()
    
    return baselines


def load_signals(
    prices: pd.DataFrame,
    rents: pd.DataFrame,
    taxes: pd.DataFrame
) -> pd.DataFrame:
    """Load and merge price, rent, and tax signals.
    
    Args:
        prices: DataFrame with price data
        rents: DataFrame with rent data
        taxes: DataFrame with tax data
        
    Returns:
        Merged DataFrame with all signals
    """
    # Merge on zip code (assuming all have 'zip' column)
    merged = prices.merge(rents, on="zip", how="outer")
    merged = merged.merge(taxes, on="zip", how="outer")
    
    return merged

