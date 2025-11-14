# How Tax is Currently Calculated

## Current Flow

### 1. **Tax Data Source** (`backend/raw/county_property_tax.csv`)
- Contains county-level property tax data
- Columns: `State`, `County`, `Median Housing Value`, `Median Property Taxes Paid`, `Effective Property Tax Rate (2023)`
- Example: Alabama, Autauga County, 0.2850% effective tax rate

### 2. **Tax Data Processing** (`backend/providers/tax_model.py`)
- Reads the CSV and parses the "Effective Property Tax Rate" column
- Converts percentage to decimal (e.g., "0.2850%" → 0.002850)
- Normalizes county names (removes "County" suffix)
- Outputs: `state`, `county_name`, `eff_tax_rate`, `county_fips` (empty placeholder)
- Caches as `backend/cache/county_tax.parquet`

### 3. **Tax Application in Pipeline** (`backend/cli.py` lines 122-137)
**⚠️ CURRENT LIMITATION:**
- **Uses STATE AVERAGE tax rate, not county-specific rates**
- Line 130: `state_tax = county_tax.groupby("state")["eff_tax_rate"].mean().reset_index()`
- Line 131: Merges on `state` only (not county)
- **Result**: All ZIPs in a state get the same tax rate (state average)
- Fallback: 0.015 (1.5%) if no tax data available

**Example:**
- Alabama has 67 counties with tax rates ranging from 0.178% to 0.596%
- Current system: All Alabama ZIPs get 0.339% (state average)
- Should be: Each ZIP gets its specific county's tax rate

### 4. **Tax Calculation in NOI** (`backend/scoring.py` lines 121-125)
- Formula: `tax_expense = price × eff_tax_rate`
- This is **correct** - annual property tax = property value × effective tax rate
- Example: $100,000 property × 0.00339 (0.339%) = $339/year in taxes

## The Problem

**Missing ZIP→County Crosswalk:**
- The system has county-level tax data
- But it can't map ZIP codes to counties
- So it falls back to state averages
- This loses precision - county tax rates vary significantly within states

**Example from data:**
- Georgia counties: 0.349% to 1.711% (5x difference!)
- Current: All GA ZIPs get ~0.898% (state average)
- Should be: Each ZIP gets its county's specific rate

## What Needs to Be Done

To get accurate county-level tax rates:

1. **Create/Obtain ZIP→County Crosswalk**
   - Map each ZIP code to its county
   - Could use:
     - HUD USPS ZIP Code Crosswalk Files
     - Census ZCTA to County relationship files
     - Commercial ZIP code databases

2. **Update Tax Join Logic** (`backend/cli.py`)
   - Instead of: `merge on state → use state average`
   - Do: `merge on state + county → use county-specific rate`
   - Fallback chain: county rate → state average → 0.015 default

3. **Update Tax Provider** (`backend/providers/tax_model.py`)
   - If you get county FIPS codes, can join on FIPS instead of county name
   - More reliable than string matching county names

## Current Tax Rate Examples (State Averages)

From the cached data:
- **Alabama**: 0.339% average (ranges 0.178% - 0.596%)
- **Texas**: ~1.8% average (varies significantly)
- **Florida**: 0.731% average (ranges 0.454% - 1.050%)
- **Ohio**: ~1.5% average
- **Georgia**: 0.898% average (ranges 0.349% - 1.711%)

## Formula Verification

The tax calculation formula is correct:
```
Annual Tax Expense = Property Price × Effective Tax Rate
```

This is then used in NOI calculation:
```
NOI = Effective Gross Income - Operating Expenses
Operating Expenses = Tax + Insurance + Repairs + Management + CapEx
```

So tax directly reduces NOI and affects cap rate.

