"""Manual inventory provider that reads from CSV."""
import pandas as pd
from pathlib import Path
from typing import Dict, Any


def load_inventory(csv_path: str = "backend/data/inventory.csv") -> pd.DataFrame:
    """Load inventory data from CSV file.
    
    Expected CSV format: zip, inventory_hits
    
    Args:
        csv_path: Path to inventory CSV file
        
    Returns:
        DataFrame with columns: zip, inventory_hits
        Returns empty DataFrame with correct columns if file doesn't exist
    """
    path = Path(csv_path)
    
    if not path.exists():
        # Return empty DataFrame with correct structure
        return pd.DataFrame(columns=["zip", "inventory_hits"])
    
    try:
        df = pd.read_csv(path)
        # Ensure required columns exist
        if "zip" not in df.columns:
            raise ValueError(f"Inventory CSV must have 'zip' column. Found: {df.columns.tolist()}")
        if "inventory_hits" not in df.columns:
            df["inventory_hits"] = 0
        return df[["zip", "inventory_hits"]]
    except Exception as e:
        # Return empty DataFrame on error
        return pd.DataFrame(columns=["zip", "inventory_hits"])

