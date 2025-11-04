"""Scoring functions for computing NOI and Cap Rate."""
import pandas as pd
from typing import Dict, Any


def compute_caps(
    df: pd.DataFrame,
    assumptions: Dict[str, Any]
) -> pd.DataFrame:
    """Compute NOI (Net Operating Income) and Cap Rate.
    
    Formula:
        Gross Income = Rent * 12
        Vacancy Loss = Gross Income * vacancy_rate
        Effective Gross Income = Gross Income - Vacancy Loss
        Operating Expenses = Price * (maintenance_pct + property_management_pct + insurance_pct + capex_pct)
        NOI = Effective Gross Income - Operating Expenses
        Cap Rate = NOI / Price
        
    Args:
        df: DataFrame with columns: price, rent (monthly)
        assumptions: Dict with vacancy_rate, maintenance_pct, property_management_pct, insurance_pct, capex_pct
        
    Returns:
        DataFrame with added columns: gross_income, vacancy_loss, egi, operating_expenses, noi, cap_rate
    """
    df = df.copy()
    
    # Extract assumptions
    vacancy_rate = assumptions.get("vacancy_rate", 0.05)
    maintenance_pct = assumptions.get("maintenance_pct", 0.10)
    property_management_pct = assumptions.get("property_management_pct", 0.08)
    insurance_pct = assumptions.get("insurance_pct", 0.015)
    capex_pct = assumptions.get("capex_pct", 0.05)
    
    # Compute Gross Income (annual rent)
    df["gross_income"] = df["rent"] * 12
    
    # Vacancy Loss
    df["vacancy_loss"] = df["gross_income"] * vacancy_rate
    
    # Effective Gross Income
    df["egi"] = df["gross_income"] - df["vacancy_loss"]
    
    # Operating Expenses
    total_opex_pct = maintenance_pct + property_management_pct + insurance_pct + capex_pct
    df["operating_expenses"] = df["price"] * total_opex_pct
    
    # Net Operating Income
    df["noi"] = df["egi"] - df["operating_expenses"]
    
    # Cap Rate
    df["cap_rate"] = df["noi"] / df["price"]
    
    return df

