"""FastAPI application for real estate zip code API."""
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import duckdb
from pathlib import Path
from typing import Optional, List
import io

from backend.utils import read_config

app = FastAPI(title="Real Estate Zip Code API")

# CORS for http://localhost:3000 and http://localhost:3001
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/meta")
async def get_meta():
    """Get basic configuration info."""
    try:
        config = read_config("backend/config.yaml")
        return {
            "states_allowlist": config.get("states_allowlist", []),
            "cap_threshold": config.get("cap_threshold", 0.05),
            "min_dscr": config.get("min_dscr", 1.2),
            "max_cash": config.get("budget", {}).get("max_cash", 60000),
            "loan": config.get("loan", {}),
            "scoring_weights": config.get("scoring_weights", {}),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading config: {str(e)}")


@app.get("/api/zips")
async def get_zips(
    limit: Optional[int] = Query(None, description="Limit number of results"),
    state: Optional[str] = Query(None, description="Filter by state code"),
    min_cap: Optional[float] = Query(None, description="Minimum cap rate"),
    max_cash: Optional[float] = Query(None, description="Maximum cash needed"),
    min_dscr: Optional[float] = Query(None, description="Minimum DSCR"),
    min_coc: Optional[float] = Query(None, description="Minimum cash-on-cash return"),
):
    """Query DuckDB zipview with filters (returns ZIP-level data)."""
    db_path = Path("backend/out/zipfinder.duckdb")
    
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found. Run CLI first.")
    
    try:
        conn = duckdb.connect(str(db_path))
        
        # Build query with conditions
        conditions = []
        
        if state:
            conditions.append(f"state = '{state}'")
        
        if min_cap is not None:
            conditions.append(f"cap_rate >= {min_cap}")
        
        if max_cash is not None:
            conditions.append(f"cash_needed <= {max_cash}")
        
        if min_dscr is not None:
            conditions.append(f"dscr >= {min_dscr}")
        
        if min_coc is not None:
            conditions.append(f"cash_on_cash >= {min_coc}")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"SELECT * FROM zipview{where_clause} ORDER BY score DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert to dict
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@app.get("/api/export.csv")
async def export_csv(
    state: Optional[str] = Query(None, description="Filter by state code"),
    min_cap: Optional[float] = Query(None, description="Minimum cap rate"),
    max_cash: Optional[float] = Query(None, description="Maximum cash needed"),
    min_dscr: Optional[float] = Query(None, description="Minimum DSCR"),
    min_coc: Optional[float] = Query(None, description="Minimum cash-on-cash return"),
):
    """Export filtered results as CSV."""
    db_path = Path("backend/out/zipfinder.duckdb")
    
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found. Run CLI first.")
    
    try:
        conn = duckdb.connect(str(db_path))
        
        # Build query with conditions (same as /api/zips)
        conditions = []
        
        if state:
            conditions.append(f"state = '{state}'")
        
        if min_cap is not None:
            conditions.append(f"cap_rate >= {min_cap}")
        
        if max_cash is not None:
            conditions.append(f"cash_needed <= {max_cash}")
        
        if min_dscr is not None:
            conditions.append(f"dscr >= {min_dscr}")
        
        if min_coc is not None:
            conditions.append(f"cash_on_cash >= {min_coc}")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"SELECT * FROM zipview{where_clause} ORDER BY score DESC"
        
        # Execute query
        df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert to CSV
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        stream.seek(0)
        
        return StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=target_zips.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

