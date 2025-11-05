"""Scoring functions for computing NOI and Cap Rate."""
import pandas as pd
import numpy as np
from typing import Dict, Any


def winsorize_by_state(df: pd.DataFrame, column: str, state_col: str = "state", 
                       lower_pct: float = 0.01, upper_pct: float = 0.99) -> pd.DataFrame:
    """Winsorize a column by state at specified percentiles.
    
    Args:
        df: DataFrame
        column: Column name to winsorize
        state_col: Column name for state
        lower_pct: Lower percentile (default 0.01)
        upper_pct: Upper percentile (default 0.99)
        
    Returns:
        DataFrame with winsorized column
    """
    df = df.copy()
    
    if column not in df.columns or state_col not in df.columns:
        return df
    
    for state in df[state_col].unique():
        mask = df[state_col] == state
        if mask.sum() > 0:
            lower_bound = df.loc[mask, column].quantile(lower_pct)
            upper_bound = df.loc[mask, column].quantile(upper_pct)
            df.loc[mask, column] = df.loc[mask, column].clip(lower=lower_bound, upper=upper_bound)
    
    return df


def fill_missing_by_state_then_global(df: pd.DataFrame, column: str, 
                                       state_col: str = "state") -> pd.DataFrame:
    """Fill missing values first by state median, then by global median.
    
    Args:
        df: DataFrame
        column: Column name to fill
        state_col: Column name for state
        
    Returns:
        DataFrame with filled column
    """
    df = df.copy()
    
    if column not in df.columns:
        return df
    
    # Fill by state median
    if state_col in df.columns:
        state_medians = df.groupby(state_col)[column].transform('median')
        df[column] = df[column].fillna(state_medians)
    
    # Fill remaining by global median
    global_median = df[column].median()
    df[column] = df[column].fillna(global_median)
    
    return df


def compute_caps(
    df: pd.DataFrame,
    assumptions: Dict[str, Any]
) -> pd.DataFrame:
    """Compute NOI (Net Operating Income) and Cap Rate with full formula.
    
    Formula:
        Gross Income = Rent × 12
        Vacancy Loss = Gross Income × vacancy_rate
        Effective Gross Income = Gross Income - Vacancy Loss
        Tax Expense = Price × eff_tax_rate (from taxes data)
        Insurance Expense = Price × insurance_pct
        Repairs = Price × maintenance_pct
        Management = Effective Gross Income × property_management_pct
        CapEx = Price × capex_pct
        Total Operating Expenses = Tax + Insurance + Repairs + Management + CapEx
        NOI = Effective Gross Income - Total Operating Expenses
        Cap Rate = NOI / Price
        
    Args:
        df: DataFrame with columns: price, rent (monthly), eff_tax_rate (optional)
        assumptions: Dict with vacancy_rate, maintenance_pct, property_management_pct, 
                     insurance_pct, capex_pct
        
    Returns:
        DataFrame with added columns: gross_income, vacancy_loss, egi, tax_expense,
        insurance_expense, repairs, management, capex, operating_expenses, noi, cap_rate,
        rent_growth (placeholder), landlord_score (placeholder)
    """
    df = df.copy()
    
    # Winsorize price and rent by state
    if "state" in df.columns:
        df = winsorize_by_state(df, "price", "state")
        df = winsorize_by_state(df, "rent", "state")
    
    # Fill missing values
    df = fill_missing_by_state_then_global(df, "price")
    df = fill_missing_by_state_then_global(df, "rent")
    
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
    
    # Tax Expense (from eff_tax_rate if available, else 0)
    if "eff_tax_rate" in df.columns:
        df["tax_expense"] = df["price"] * df["eff_tax_rate"]
    else:
        df["tax_expense"] = 0.0
    
    # Insurance Expense
    df["insurance_expense"] = df["price"] * insurance_pct
    
    # Repairs
    df["repairs"] = df["price"] * maintenance_pct
    
    # Management (on EGI, not price)
    df["management"] = df["egi"] * property_management_pct
    
    # CapEx
    df["capex"] = df["price"] * capex_pct
    
    # Total Operating Expenses
    df["operating_expenses"] = (
        df["tax_expense"] +
        df["insurance_expense"] +
        df["repairs"] +
        df["management"] +
        df["capex"]
    )
    
    # Net Operating Income
    df["noi"] = df["egi"] - df["operating_expenses"]
    
    # Cap Rate
    df["cap_rate"] = df["noi"] / df["price"]
    df["cap_rate"] = df["cap_rate"].replace([np.inf, -np.inf], 0).fillna(0)
    
    # Placeholder columns
    df["rent_growth"] = 0.0  # Placeholder
    df["landlord_score"] = 0.0  # Placeholder
    
    return df

