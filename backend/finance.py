"""Financial calculation functions for mortgages and financing constraints."""
import pandas as pd
import numpy as np
from typing import Dict, Any


def mortgage_payment(
    principal: float,
    rate: float,
    term_years: int
) -> float:
    """Calculate monthly mortgage payment.
    
    Formula: P = (P0 * r * (1 + r)^n) / ((1 + r)^n - 1)
    where P0 = principal, r = monthly rate, n = number of payments
    
    Args:
        principal: Loan principal amount
        rate: Annual interest rate (e.g., 0.065 for 6.5%)
        term_years: Loan term in years
        
    Returns:
        Monthly payment amount
    """
    if principal == 0:
        return 0.0
    
    monthly_rate = rate / 12
    num_payments = term_years * 12
    
    if monthly_rate == 0:
        return principal / num_payments
    
    payment = principal * (monthly_rate * (1 + monthly_rate) ** num_payments) / \
              ((1 + monthly_rate) ** num_payments - 1)
    
    return payment


def attach_financing_constraints(
    df: pd.DataFrame,
    loan_params: Dict[str, Any],
    cash_costs: Dict[str, Any],
    budget: Dict[str, Any]
) -> pd.DataFrame:
    """Attach financing constraints: cash_needed, dscr, cash_on_cash.
    
    Args:
        df: DataFrame with columns: price, noi (annual)
        loan_params: Dict with rate, term_years, down_payment_pct
        cash_costs: Dict with closing_costs_pct, inspection, appraisal, title_insurance
        budget: Dict with max_cash
        
    Returns:
        DataFrame with added columns: cash_needed, dscr, cash_on_cash, monthly_payment
    """
    df = df.copy()
    
    # Extract parameters
    rate = loan_params.get("rate", 0.065)
    term_years = loan_params.get("term_years", 30)
    down_payment_pct = loan_params.get("down_payment_pct", 0.20)
    
    closing_costs_pct = cash_costs.get("closing_costs_pct", 0.03)
    inspection = cash_costs.get("inspection", 500)
    appraisal = cash_costs.get("appraisal", 600)
    title_insurance = cash_costs.get("title_insurance", 1000)
    
    # Calculate loan amount
    df["loan_amount"] = df["price"] * (1 - down_payment_pct)
    
    # Calculate down payment
    df["down_payment"] = df["price"] * down_payment_pct
    
    # Calculate monthly mortgage payment
    df["monthly_payment"] = df["loan_amount"].apply(
        lambda x: mortgage_payment(x, rate, term_years)
    )
    
    # Calculate annual debt service
    df["annual_debt_service"] = df["monthly_payment"] * 12
    
    # Calculate cash needed (down payment + closing costs + other fees)
    closing_costs = df["price"] * closing_costs_pct
    df["cash_needed"] = (
        df["down_payment"] +
        closing_costs +
        inspection +
        appraisal +
        title_insurance
    )
    
    # Debt Service Coverage Ratio (DSCR) = NOI / Annual Debt Service
    df["dscr"] = df["noi"] / df["annual_debt_service"]
    
    # Cash on Cash Return = (NOI - Annual Debt Service) / Cash Needed
    df["cash_on_cash"] = (df["noi"] - df["annual_debt_service"]) / df["cash_needed"]
    
    # Replace infinities and NaN with 0
    df["dscr"] = df["dscr"].replace([np.inf, -np.inf], 0).fillna(0)
    df["cash_on_cash"] = df["cash_on_cash"].replace([np.inf, -np.inf], 0).fillna(0)
    
    return df

