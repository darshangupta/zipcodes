"""CLI application using Typer for running the real estate zip code analysis."""
import typer
import pandas as pd
from pathlib import Path
from typing import Optional

try:
    from backend.utils import read_config, save_parquet, write_duckdb
    from backend.sources import load_baselines, load_signals
    from backend.scoring import compute_caps
    from backend.finance import attach_financing_constraints
except ImportError:
    # Fallback for running directly from backend directory
    from utils import read_config, save_parquet, write_duckdb
    from sources import load_baselines, load_signals
    from scoring import compute_caps
    from finance import attach_financing_constraints

app = typer.Typer()


@app.command()
def run(
    config_path: str = "backend/config.yaml",
    output_dir: str = "output"
):
    """Run the real estate zip code analysis pipeline.
    
    Loads CSVs, computes caps, attaches finance, filters by cap_threshold & max_cash,
    scores, and writes target_zips.parquet and duckdb zipview.
    """
    # Read configuration
    config = read_config(config_path)
    
    # Extract configuration values
    states_allowlist = config.get("states_allowlist", [])
    cap_threshold = config.get("cap_threshold", 0.05)
    max_cash = config.get("budget", {}).get("max_cash", 60000)
    loan_params = config.get("loan", {})
    cash_costs = config.get("cash_costs", {})
    assumptions = config.get("assumptions", {})
    scoring_weights = config.get("scoring_weights", {})
    data_sources = config.get("data_sources", {})
    
    # Load data sources
    zips_path = data_sources.get("zips", "data/zips.csv")
    baselines_path = data_sources.get("baselines", "data/baselines.csv")
    prices_path = data_sources.get("prices", "data/prices.csv")
    rents_path = data_sources.get("rents", "data/rents.csv")
    taxes_path = data_sources.get("taxes", "data/taxes.csv")
    
    typer.echo(f"Loading data from {zips_path}...")
    zips = pd.read_csv(zips_path)
    
    typer.echo(f"Loading baselines from {baselines_path}...")
    baselines = pd.read_csv(baselines_path)
    
    typer.echo(f"Loading prices from {prices_path}...")
    prices = pd.read_csv(prices_path)
    
    typer.echo(f"Loading rents from {rents_path}...")
    rents = pd.read_csv(rents_path)
    
    typer.echo(f"Loading taxes from {taxes_path}...")
    taxes = pd.read_csv(taxes_path)
    
    # Load baselines filtered by states
    typer.echo(f"Filtering baselines by states: {states_allowlist}...")
    filtered_baselines = load_baselines(baselines, states_allowlist)
    
    # Load and merge signals
    typer.echo("Merging price, rent, and tax signals...")
    signals = load_signals(prices, rents, taxes)
    
    # Merge baselines with signals
    # Assuming both have 'zip' column for merging
    df = filtered_baselines.merge(signals, on="zip", how="inner")
    
    # Compute caps (NOI and Cap Rate)
    typer.echo("Computing NOI and Cap Rates...")
    df = compute_caps(df, assumptions)
    
    # Attach financing constraints
    typer.echo("Attaching financing constraints...")
    budget = {"max_cash": max_cash}
    df = attach_financing_constraints(df, loan_params, cash_costs, budget)
    
    # Filter by cap_threshold and max_cash
    typer.echo(f"Filtering by cap_threshold >= {cap_threshold} and cash_needed <= {max_cash}...")
    df_filtered = df[
        (df["cap_rate"] >= cap_threshold) &
        (df["cash_needed"] <= max_cash)
    ].copy()
    
    # Score the results
    typer.echo("Computing scores...")
    df_filtered["score"] = (
        df_filtered["cap_rate"] * scoring_weights.get("cap_rate", 0.40) +
        df_filtered["cash_on_cash"] * scoring_weights.get("cash_on_cash", 0.30) +
        df_filtered["dscr"] * scoring_weights.get("dscr", 0.20) +
        (1 / df_filtered["price"]) * scoring_weights.get("price", 0.10) * 1000000  # Normalize price
    )
    
    # Sort by score descending
    df_filtered = df_filtered.sort_values("score", ascending=False)
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Write target_zips.parquet
    output_parquet = Path(output_dir) / "target_zips.parquet"
    typer.echo(f"Writing results to {output_parquet}...")
    save_parquet(df_filtered, str(output_parquet))
    
    # Write DuckDB
    db_path = Path(output_dir) / "zipview.db"
    typer.echo(f"Writing DuckDB to {db_path}...")
    write_duckdb(df_filtered, str(db_path), "zipview")
    
    typer.echo(f"✅ Analysis complete! Found {len(df_filtered)} target zip codes.")


if __name__ == "__main__":
    app()

