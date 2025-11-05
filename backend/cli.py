"""CLI application using Typer for running the real estate zip code analysis."""
import typer
import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import datetime

try:
    from backend.utils import read_config, save_parquet, write_duckdb
    from backend.sources import load_baselines, load_signals
    from backend.scoring import compute_caps
    from backend.finance import attach_financing_constraints
    from backend.providers.inventory_manual import load_inventory
    from backend.providers.crime_stub import load_crime
except ImportError:
    # Fallback for running directly from backend directory
    from utils import read_config, save_parquet, write_duckdb
    from sources import load_baselines, load_signals
    from scoring import compute_caps
    from finance import attach_financing_constraints
    from providers.inventory_manual import load_inventory
    from providers.crime_stub import load_crime

app = typer.Typer()


@app.command()
def run(
    config_path: str = "backend/config.yaml",
    output_dir: str = "backend/out"
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
    min_dscr = config.get("min_dscr", 1.2)
    max_cash = config.get("budget", {}).get("max_cash", 60000)
    loan_params = config.get("loan", {})
    cash_costs = config.get("cash_costs", {})
    assumptions = config.get("assumptions", {})
    scoring_weights = config.get("scoring_weights", {})
    data_sources = config.get("data_sources", {})
    paths = config.get("paths", {})
    
    # Load baselines (still from CSV for now)
    baselines_path = data_sources.get("baselines", "backend/data/csv/zips.csv")
    typer.echo(f"Loading baselines from {baselines_path}...")
    baselines = pd.read_csv(baselines_path)
    if "zip" in baselines.columns:
        baselines["zip"] = baselines["zip"].astype(str).str.zfill(5)
    
    # Try to load prices from provider, fallback to legacy CSV
    prices = None
    try:
        from backend.providers.price_redfin import load
        typer.echo("Loading prices from provider cache...")
        prices = load()
        # Rename median_price to price for compatibility
        if "median_price" in prices.columns:
            prices = prices.rename(columns={"median_price": "price"})
    except (FileNotFoundError, ImportError) as e:
        prices_path = data_sources.get("prices", "backend/data/csv/zip_price.csv")
        typer.echo(f"⚠️  Falling back to legacy CSV: {prices_path}")
        prices = pd.read_csv(prices_path)
        if "median_price" in prices.columns:
            prices = prices.rename(columns={"median_price": "price"})
        if "zip" in prices.columns:
            prices["zip"] = prices["zip"].astype(str).str.zfill(5)
    
    # Try to load rents from provider, fallback to legacy CSV
    rents = None
    try:
        from backend.providers.rent_zori import load
        typer.echo("Loading rents from provider cache...")
        rents = load()
        # Rename median_rent to rent for compatibility
        if "median_rent" in rents.columns:
            rents = rents.rename(columns={"median_rent": "rent"})
    except (FileNotFoundError, ImportError) as e:
        rents_path = data_sources.get("rents", "backend/data/csv/zip_rent.csv")
        typer.echo(f"⚠️  Falling back to legacy CSV: {rents_path}")
        rents = pd.read_csv(rents_path)
        if "median_rent" in rents.columns:
            rents = rents.rename(columns={"median_rent": "rent"})
        if "zip" in rents.columns:
            rents["zip"] = rents["zip"].astype(str).str.zfill(5)
    
    # Load geographic crosswalks
    typer.echo("Loading geographic crosswalks...")
    from backend.providers.geo import load_zip_county, load_zip_zcta
    zip_county = load_zip_county(paths.get("crosswalk_zip_county", "backend/cache/crosswalk_zip_county.parquet"))
    zip_zcta = load_zip_zcta(paths.get("crosswalk_zip_zcta", "backend/cache/crosswalk_zip_zcta.parquet"))
    
    # Load county tax data
    taxes = None
    try:
        from backend.providers.tax_model import load
        typer.echo("Loading tax data from provider cache...")
        county_tax = load()
        if len(county_tax) > 0 and len(zip_county) > 0:
            # Join ZIP→county→tax
            taxes = zip_county.merge(county_tax, on="county_fips", how="left")
            taxes = taxes[["zip", "eff_tax_rate"]].copy()
            taxes["eff_tax_rate"] = taxes["eff_tax_rate"].fillna(0.015)  # Default tax rate
        else:
            taxes = pd.DataFrame(columns=["zip", "eff_tax_rate"])
    except Exception as e:
        taxes_path = data_sources.get("taxes", "backend/data/csv/zip_effective_tax.csv")
        typer.echo(f"⚠️  Falling back to legacy CSV: {taxes_path}")
        taxes = pd.read_csv(taxes_path)
        if "zip" in taxes.columns:
            taxes["zip"] = taxes["zip"].astype(str).str.zfill(5)
    
    # Optional: Load ACS data and join via ZCTA
    try:
        from backend.providers.census_acs import load
        acs_data = load()
        if len(acs_data) > 0 and len(zip_zcta) > 0:
            # This could be used to enrich vacancy_rate assumptions later
            pass
    except Exception:
        pass  # ACS is optional
    
    # Load baselines filtered by states
    typer.echo(f"Filtering baselines by states: {states_allowlist}...")
    filtered_baselines = load_baselines(baselines, states_allowlist)
    
    # Load and merge signals
    typer.echo("Merging price, rent, and tax signals...")
    # Use load_signals helper, but ensure all DataFrames have zip column
    if prices is not None and "zip" not in prices.columns:
        raise ValueError("Prices DataFrame missing 'zip' column")
    if rents is not None and "zip" not in rents.columns:
        raise ValueError("Rents DataFrame missing 'zip' column")
    if taxes is not None and "zip" not in taxes.columns:
        raise ValueError("Taxes DataFrame missing 'zip' column")
    
    signals = load_signals(prices, rents, taxes)
    
    # Merge baselines with signals
    # Assuming both have 'zip' column for merging
    df = filtered_baselines.merge(signals, on="zip", how="inner")
    
    # Schema validation: ensure required columns exist
    required_cols = ["price", "rent"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after merge: {missing}. Available: {df.columns.tolist()}")
    
    if "eff_tax_rate" not in df.columns:
        typer.echo("⚠️  Warning: eff_tax_rate not found, defaulting to 0.015")
        df["eff_tax_rate"] = 0.015
    
    # Load inventory and crime data
    typer.echo("Loading inventory data...")
    inventory = load_inventory()
    if len(inventory) > 0:
        df = df.merge(inventory, on="zip", how="left")
        df["inventory_hits"] = df["inventory_hits"].fillna(0)
    else:
        df["inventory_hits"] = 0
    
    typer.echo("Loading crime data...")
    crime = load_crime(df["zip"] if "zip" in df.columns else None)
    if len(crime) > 0:
        df = df.merge(crime, on="zip", how="left")
        df["crime_index"] = df["crime_index"].fillna(1.0)
    else:
        df["crime_index"] = 1.0
    
    # Check for demo numbers (rent/price ratio warnings)
    if "rent" in df.columns and "price" in df.columns:
        rent_price_ratio = df["rent"] / df["price"]
        high_ratio = (rent_price_ratio > 0.03).sum()
        low_ratio = (rent_price_ratio < 0.002).sum()
        if high_ratio > 0:
            typer.echo(f"⚠️  Warning: {high_ratio} rows have rent/price > 0.03 (possibly demo numbers)")
        if low_ratio > 0:
            typer.echo(f"⚠️  Warning: {low_ratio} rows have rent/price < 0.002 (possibly demo numbers)")
    
    # Compute caps (NOI and Cap Rate)
    typer.echo("Computing NOI and Cap Rates...")
    df = compute_caps(df, assumptions)
    
    # Attach financing constraints
    typer.echo("Attaching financing constraints...")
    budget = {"max_cash": max_cash}
    df = attach_financing_constraints(df, loan_params, cash_costs, budget)
    
    # Filter by cap_threshold, max_cash, and min_dscr
    typer.echo(f"Filtering by cap_threshold >= {cap_threshold}, cash_needed <= {max_cash}, dscr >= {min_dscr}...")
    
    # Track exclusions
    total_before = len(df)
    excluded_cap = len(df[df["cap_rate"] < cap_threshold])
    excluded_cash = len(df[df["cash_needed"] > max_cash])
    excluded_dscr = len(df[df["dscr"] < min_dscr])
    
    df_filtered = df[
        (df["cap_rate"] >= cap_threshold) &
        (df["cash_needed"] <= max_cash) &
        (df["dscr"] >= min_dscr)
    ].copy()
    
    # Print exclusion tally
    typer.echo(f"\n📊 Filter Results:")
    typer.echo(f"  Total before filters: {total_before}")
    typer.echo(f"  Excluded (cap_rate < {cap_threshold}): {excluded_cap}")
    typer.echo(f"  Excluded (cash_needed > {max_cash}): {excluded_cash}")
    typer.echo(f"  Excluded (dscr < {min_dscr}): {excluded_dscr}")
    typer.echo(f"  ✅ Passing all filters: {len(df_filtered)}")
    
    # Score the results
    typer.echo("Computing scores...")
    df_filtered["score"] = (
        df_filtered["cap_rate"] * scoring_weights.get("cap_rate", 0.40) +
        df_filtered["cash_on_cash"] * scoring_weights.get("cash_on_cash", 0.30) +
        df_filtered["dscr"] * scoring_weights.get("dscr", 0.20) +
        (1 / df_filtered["price"]) * scoring_weights.get("price", 0.10) * 1000000  # Normalize price
    )
    
    # Apply soft crime penalty: score *= 0.95 if crime_index > 1.25
    if "crime_index" in df_filtered.columns:
        crime_penalty_mask = df_filtered["crime_index"] > 1.25
        df_filtered.loc[crime_penalty_mask, "score"] = df_filtered.loc[crime_penalty_mask, "score"] * 0.95
    
    # Sort by score descending
    df_filtered = df_filtered.sort_values("score", ascending=False)
    
    # Create output directories (backend/out per cursorrules)
    backend_out = Path("backend/out")
    backend_out.mkdir(parents=True, exist_ok=True)
    
    # Write target_zips.parquet
    output_parquet = backend_out / "target_zips.parquet"
    typer.echo(f"Writing results to {output_parquet}...")
    save_parquet(df_filtered, str(output_parquet))
    
    # Write dated snapshot
    date_str = datetime.now().strftime("%Y%m%d")
    snapshot_path = backend_out / f"run_{date_str}.parquet"
    typer.echo(f"Writing snapshot to {snapshot_path}...")
    save_parquet(df_filtered, str(snapshot_path))
    
    # Write DuckDB
    db_path = backend_out / "zipfinder.duckdb"
    typer.echo(f"Writing DuckDB to {db_path}...")
    write_duckdb(df_filtered, str(db_path), "zipview")
    
    typer.echo(f"\n✅ Analysis complete! Found {len(df_filtered)} target zip codes.")


@app.command()
def ingest(
    force: bool = typer.Option(False, "--force", help="Force re-fetch even if cache exists"),
    config_path: str = "backend/config.yaml"
):
    """Ingest data from providers and cache as parquet files."""
    import warnings
    
    # Read configuration
    config = read_config(config_path)
    ingest_config = config.get("ingest", {})
    paths = config.get("paths", {})
    
    # Create cache directory
    Path("backend/cache").mkdir(parents=True, exist_ok=True)
    
    typer.echo("Starting data ingest...")
    report = {}
    
    # Redfin price data
    if ingest_config.get("redfin_zip", False):
        try:
            from backend.providers.price_redfin import fetch
            raw_path = paths.get("raw_redfin_zip_csv", "backend/raw/redfin_zip.csv")
            df = fetch(force=force, raw_csv_path=raw_path)
            report["redfin_zip"] = len(df)
        except Exception as e:
            typer.echo(f"⚠️  Redfin ingest failed: {e}")
            report["redfin_zip"] = 0
    
    # ZORI rent data
    if ingest_config.get("zori_zip", False):
        try:
            from backend.providers.rent_zori import fetch
            raw_path = paths.get("raw_zori_zip_csv", "backend/raw/zori_zip.csv")
            df = fetch(force=force, raw_csv_path=raw_path)
            report["zori_zip"] = len(df)
        except Exception as e:
            typer.echo(f"⚠️  ZORI ingest failed: {e}")
            report["zori_zip"] = 0
    
    # Census ACS data
    if ingest_config.get("acs_zcta", False):
        try:
            from backend.providers.census_acs import fetch
            raw_path = "backend/raw/acs_zcta.csv"
            df = fetch(force=force, raw_csv_path=raw_path)
            report["acs_zcta"] = len(df)
        except Exception as e:
            typer.echo(f"⚠️  ACS ingest failed: {e}")
            report["acs_zcta"] = 0
    
    # County tax data (no-op fetch, just ensures cache exists)
    if ingest_config.get("county_tax", False):
        try:
            from backend.providers.tax_model import fetch
            df = fetch(force=force)
            report["county_tax"] = len(df) if df is not None else 0
        except Exception as e:
            typer.echo(f"⚠️  County tax ingest failed: {e}")
            report["county_tax"] = 0
    
    # Crime data (stub)
    if ingest_config.get("crime_county", False):
        try:
            from backend.providers.crime_stub import fetch
            df = fetch(force=force)
            report["crime_county"] = len(df) if df is not None else 0
        except Exception as e:
            typer.echo(f"⚠️  Crime ingest failed: {e}")
            report["crime_county"] = 0
    
    # Print ingest report
    typer.echo("\n📊 Ingest Report:")
    for name, count in report.items():
        typer.echo(f"  {name}: {count} rows")
    
    total = sum(report.values())
    typer.echo(f"\n✅ Ingest complete! Total rows cached: {total}")


@app.command()
def deltas(
    since: str = typer.Option(..., help="Date in YYYY-MM-DD format"),
    config_path: str = "backend/config.yaml",
    output_dir: str = "backend/out"
):
    """Compare latest run vs snapshot from specified date and write deltas.parquet."""
    from datetime import datetime
    
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d")
        since_str = since_date.strftime("%Y%m%d")
    except ValueError:
        typer.echo(f"❌ Error: Invalid date format '{since}'. Use YYYY-MM-DD.")
        raise typer.Exit(1)
    
    # Find latest snapshot
    backend_out = Path("backend/out")
    if not backend_out.exists():
        typer.echo(f"❌ Error: No snapshots found in backend/out")
        raise typer.Exit(1)
    
    snapshot_files = sorted(backend_out.glob("run_*.parquet"), reverse=True)
    if not snapshot_files:
        typer.echo(f"❌ Error: No snapshots found in backend/out")
        raise typer.Exit(1)
    
    latest_snapshot = snapshot_files[0]
    latest_date_str = latest_snapshot.stem.replace("run_", "")
    
    # Find snapshot from 'since' date
    since_snapshot = backend_out / f"run_{since_str}.parquet"
    if not since_snapshot.exists():
        typer.echo(f"❌ Error: Snapshot for {since} not found: {since_snapshot}")
        raise typer.Exit(1)
    
    typer.echo(f"Comparing latest ({latest_date_str}) vs {since}...")
    
    # Load both snapshots
    latest_df = pd.read_parquet(latest_snapshot)
    since_df = pd.read_parquet(since_snapshot)
    
    # Merge on zip to compare
    merged = latest_df.merge(
        since_df,
        on="zip",
        how="outer",
        suffixes=("_latest", "_since"),
        indicator=True
    )
    
    # Calculate deltas
    delta_rows = []
    for _, row in merged.iterrows():
        if row["_merge"] == "left_only":
            # New in latest
            delta_rows.append({
                "zip": row["zip"],
                "change_type": "new",
                "score_delta": row.get("score_latest", 0),
                "cap_rate_delta": row.get("cap_rate_latest", 0),
                "cash_needed_delta": row.get("cash_needed_latest", 0),
            })
        elif row["_merge"] == "right_only":
            # Removed
            delta_rows.append({
                "zip": row["zip"],
                "change_type": "removed",
                "score_delta": -row.get("score_since", 0),
                "cap_rate_delta": -row.get("cap_rate_since", 0),
                "cash_needed_delta": -row.get("cash_needed_since", 0),
            })
        elif row["_merge"] == "both":
            # Changed
            score_delta = row.get("score_latest", 0) - row.get("score_since", 0)
            cap_delta = row.get("cap_rate_latest", 0) - row.get("cap_rate_since", 0)
            cash_delta = row.get("cash_needed_latest", 0) - row.get("cash_needed_since", 0)
            
            if abs(score_delta) > 0.01 or abs(cap_delta) > 0.001 or abs(cash_delta) > 100:
                delta_rows.append({
                    "zip": row["zip"],
                    "change_type": "changed",
                    "score_delta": score_delta,
                    "cap_rate_delta": cap_delta,
                    "cash_needed_delta": cash_delta,
                })
    
    if not delta_rows:
        typer.echo("No significant changes found.")
        return
    
    deltas_df = pd.DataFrame(delta_rows)
    
    # Write deltas
    backend_out = Path("backend/out")
    backend_out.mkdir(parents=True, exist_ok=True)
    deltas_path = backend_out / "deltas.parquet"
    save_parquet(deltas_df, str(deltas_path))
    
    typer.echo(f"✅ Wrote {len(deltas_df)} deltas to {deltas_path}")
    typer.echo(f"  New: {(deltas_df['change_type'] == 'new').sum()}")
    typer.echo(f"  Removed: {(deltas_df['change_type'] == 'removed').sum()}")
    typer.echo(f"  Changed: {(deltas_df['change_type'] == 'changed').sum()}")


if __name__ == "__main__":
    app()

