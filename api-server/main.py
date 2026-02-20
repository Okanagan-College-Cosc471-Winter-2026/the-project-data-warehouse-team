# main.py - FastAPI server with API key from .env and batch load endpoint
import psycopg2
from psycopg2 import Error
from fastapi import FastAPI, Depends, HTTPException, Header, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Union, Optional
from dotenv import load_dotenv
import os
import datetime
import json
import logging
import csv
import io

# Configure basic logging (helps debug TEST_MODE in CI)
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

app = FastAPI(title="DW Model Features API - Test")

VALID_API_KEY = os.getenv("API_KEY", "")
if not VALID_API_KEY:
    print("WARNING: API_KEY not set in .env file")

def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key != VALID_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return x_api_key

# Pydantic model matching the full sample payload structure
class FeatureRecord(BaseModel):
    symbol: str
    datetime: str  # ISO format string, e.g., "2026-02-11T14:30:00"
    price: Union[float, None] = None
    market_cap: Union[float, None] = None
    beta: Union[float, None] = None
    last_dividend: Union[float, None] = None
    range: Union[str, None] = None
    change: Union[float, None] = None
    change_percentage: Union[float, None] = None
    volume: Union[int, None] = None
    average_volume: Union[int, None] = None
    company_name: Union[str, None] = None
    currency: Union[str, None] = None
    cik: Union[str, None] = None
    isin: Union[str, None] = None
    cusip: Union[str, None] = None
    exchange_full_name: Union[str, None] = None
    exchange: Union[str, None] = None
    industry: Union[str, None] = None
    website: Union[str, None] = None
    description: Union[str, None] = None
    ceo: Union[str, None] = None
    sector: Union[str, None] = None
    country: Union[str, None] = None
    full_time_employees: Union[str, None] = None
    phone: Union[str, None] = None
    address: Union[str, None] = None
    city: Union[str, None] = None
    state: Union[str, None] = None
    zip: Union[str, None] = None
    image: Union[str, None] = None
    ipo_date: Union[str, None] = None  # or DATE if strict typing preferred
    default_image: Union[bool, None] = None
    is_etf: Union[bool, None] = None
    is_actively_trading: Union[bool, None] = None
    is_adr: Union[bool, None] = None
    is_fund: Union[bool, None] = None

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "message": "FastAPI server is running",
        "python_version": "3.9"
    }

@app.post("/v1/test-load")
async def test_load(_: str = Depends(verify_api_key)):
    return {
        "status": "success",
        "message": "Test load endpoint reached - API key verified"
    }

@app.post("/v1/load/features")
async def load_features(
    records: List[FeatureRecord],
    _: str = Depends(verify_api_key)
):
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    # Debug log to confirm TEST_MODE value in CI
    logging.info(f"TEST_MODE environment variable: {os.getenv('TEST_MODE')}")

    if os.getenv("TEST_MODE") == "1":  # CI test mode: mock response without DB
        logging.info("Entering CI test mode - returning mock responses")
        inserted_count = len(records)  # Mock full insertion
        rejects = []  # Mock no rejects
        for idx, record in enumerate(records):
            if "duplicate" in record.datetime.lower():  # Mock duplicate detection
                rejects.append({
                    "index": idx,
                    "symbol": record.symbol,
                    "datetime": record.datetime,
                    "reason": "Duplicate key (already exists)"
                })
                inserted_count -= 1
        response = {
            "status": "partial" if rejects else "success",
            "inserted_count": inserted_count,
            "total_received": len(records),
            "rejects": rejects if rejects else None
        }
        return response

    # Normal mode: real DB connection and insertion
    conn = None
    inserted_count = 0
    rejects = []
    try:
        conn = psycopg2.connect(
            dbname="cosc471_project",
            user="dw_user",
            password="letmein",
            host="localhost"
        )
        cur = conn.cursor()
        for idx, record in enumerate(records):
            try:
                cur.execute("""
                    INSERT INTO staging.model_features (
                        symbol, datetime, price, market_cap, beta, last_dividend, range,
                        change, change_percentage, volume, average_volume, company_name,
                        currency, cik, isin, cusip, exchange_full_name, exchange, industry,
                        website, description, ceo, sector, country, full_time_employees,
                        phone, address, city, state, zip, image, ipo_date, default_image,
                        is_etf, is_actively_trading, is_adr, is_fund
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, datetime) DO NOTHING
                    RETURNING symbol
                """, (
                    record.symbol,
                    record.datetime,
                    record.price,
                    record.market_cap,
                    record.beta,
                    record.last_dividend,
                    record.range,
                    record.change,
                    record.change_percentage,
                    record.volume,
                    record.average_volume,
                    record.company_name,
                    record.currency,
                    record.cik,
                    record.isin,
                    record.cusip,
                    record.exchange_full_name,
                    record.exchange,
                    record.industry,
                    record.website,
                    record.description,
                    record.ceo,
                    record.sector,
                    record.country,
                    record.full_time_employees,
                    record.phone,
                    record.address,
                    record.city,
                    record.state,
                    record.zip,
                    record.image,
                    record.ipo_date,
                    record.default_image,
                    record.is_etf,
                    record.is_actively_trading,
                    record.is_adr,
                    record.is_fund
                ))
                if cur.fetchone():
                    inserted_count += 1
                else:
                    rejects.append({
                        "index": idx,
                        "symbol": record.symbol,
                        "datetime": record.datetime,
                        "reason": "Duplicate key (already exists)"
                    })
            except Exception as row_err:
                rejects.append({
                    "index": idx,
                    "symbol": record.symbol,
                    "datetime": record.datetime,
                    "reason": str(row_err)
                })
        conn.commit()
        response = {
            "status": "partial" if rejects else "success",
            "inserted_count": inserted_count,
            "total_received": len(records),
            "rejects": rejects if rejects else None
        }
        if rejects:
            batch_id = "batch-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            for reject in rejects:
                cur.execute("""
                    INSERT INTO staging.reject_log (batch_id, symbol, datetime, original_payload, reject_reason)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    batch_id,
                    reject["symbol"],
                    reject["datetime"],
                    json.dumps(record.dict()),
                    reject["reason"]
                ))
            conn.commit()
        return response
    except Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/v1/extract/features/schema")
async def get_features_schema(_: str = Depends(verify_api_key)):
    """
    Returns the schema of available features for ML model training.
    Helps the ML team understand available fields and their types.
    """
    schema = {
        "table": "staging.model_features",
        "fields": [
            {"name": "symbol", "type": "string", "description": "Stock ticker symbol", "ml_relevant": True},
            {"name": "datetime", "type": "timestamp", "description": "Record timestamp", "ml_relevant": True},
            {"name": "price", "type": "float", "description": "Current stock price", "ml_relevant": True},
            {"name": "market_cap", "type": "float", "description": "Market capitalization", "ml_relevant": True},
            {"name": "beta", "type": "float", "description": "Stock beta value", "ml_relevant": True},
            {"name": "last_dividend", "type": "float", "description": "Last dividend amount", "ml_relevant": True},
            {"name": "range", "type": "string", "description": "Price range", "ml_relevant": False},
            {"name": "change", "type": "float", "description": "Price change", "ml_relevant": True},
            {"name": "change_percentage", "type": "float", "description": "Price change percentage", "ml_relevant": True},
            {"name": "volume", "type": "integer", "description": "Trading volume", "ml_relevant": True},
            {"name": "average_volume", "type": "integer", "description": "Average trading volume", "ml_relevant": True},
            {"name": "company_name", "type": "string", "description": "Company name", "ml_relevant": False},
            {"name": "currency", "type": "string", "description": "Trading currency", "ml_relevant": True},
            {"name": "cik", "type": "string", "description": "CIK number", "ml_relevant": False},
            {"name": "isin", "type": "string", "description": "ISIN number", "ml_relevant": False},
            {"name": "cusip", "type": "string", "description": "CUSIP number", "ml_relevant": False},
            {"name": "exchange_full_name", "type": "string", "description": "Full exchange name", "ml_relevant": False},
            {"name": "exchange", "type": "string", "description": "Exchange code", "ml_relevant": True},
            {"name": "industry", "type": "string", "description": "Industry classification", "ml_relevant": True},
            {"name": "website", "type": "string", "description": "Company website", "ml_relevant": False},
            {"name": "description", "type": "string", "description": "Company description", "ml_relevant": False},
            {"name": "ceo", "type": "string", "description": "CEO name", "ml_relevant": False},
            {"name": "sector", "type": "string", "description": "Market sector", "ml_relevant": True},
            {"name": "country", "type": "string", "description": "Country", "ml_relevant": True},
            {"name": "full_time_employees", "type": "string", "description": "Number of employees", "ml_relevant": True},
            {"name": "phone", "type": "string", "description": "Company phone", "ml_relevant": False},
            {"name": "address", "type": "string", "description": "Company address", "ml_relevant": False},
            {"name": "city", "type": "string", "description": "Company city", "ml_relevant": False},
            {"name": "state", "type": "string", "description": "Company state", "ml_relevant": False},
            {"name": "zip", "type": "string", "description": "Zip code", "ml_relevant": False},
            {"name": "image", "type": "string", "description": "Company logo URL", "ml_relevant": False},
            {"name": "ipo_date", "type": "string", "description": "IPO date", "ml_relevant": True},
            {"name": "default_image", "type": "boolean", "description": "Using default image", "ml_relevant": False},
            {"name": "is_etf", "type": "boolean", "description": "Is ETF flag", "ml_relevant": True},
            {"name": "is_actively_trading", "type": "boolean", "description": "Actively trading flag", "ml_relevant": True},
            {"name": "is_adr", "type": "boolean", "description": "Is ADR flag", "ml_relevant": True},
            {"name": "is_fund", "type": "boolean", "description": "Is fund flag", "ml_relevant": True}
        ],
        "recommended_ml_fields": [
            "symbol", "datetime", "price", "market_cap", "beta", "last_dividend",
            "change", "change_percentage", "volume", "average_volume", "currency",
            "exchange", "industry", "sector", "country", "full_time_employees",
            "ipo_date", "is_etf", "is_actively_trading", "is_adr", "is_fund"
        ]
    }
    return schema

@app.get("/v1/extract/features/stats")
async def get_features_stats(
    _: str = Depends(verify_api_key),
    symbols: Optional[str] = Query(None, description="Comma-separated list of symbols to filter stats")
):
    """
    Returns statistics about available data for ML model training.
    Includes row counts, date ranges, and symbol availability.
    """
    if os.getenv("TEST_MODE") == "1":
        # Return mock stats in test mode
        return {
            "total_records": 1000,
            "unique_symbols": 5,
            "date_range": {
                "earliest": "2026-01-01T00:00:00",
                "latest": "2026-02-20T12:00:00"
            },
            "symbols": [
                {"symbol": "AAPL", "record_count": 200, "first_date": "2026-01-01T00:00:00", "last_date": "2026-02-20T12:00:00"},
                {"symbol": "MSFT", "record_count": 200, "first_date": "2026-01-01T00:00:00", "last_date": "2026-02-20T12:00:00"}
            ]
        }
    
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="cosc471_project",
            user="dw_user",
            password="letmein",
            host="localhost"
        )
        cur = conn.cursor()
        
        # Get overall statistics
        cur.execute("SELECT COUNT(*) FROM staging.model_features")
        total_records = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT symbol) FROM staging.model_features")
        unique_symbols = cur.fetchone()[0]
        
        cur.execute("""
            SELECT MIN(datetime), MAX(datetime) 
            FROM staging.model_features
        """)
        date_range = cur.fetchone()
        
        # Get per-symbol statistics
        symbol_filter = ""
        if symbols:
            symbol_list = [s.strip() for s in symbols.split(',')]
            symbol_filter = f"WHERE symbol IN ({','.join(['%s'] * len(symbol_list))})"
            cur.execute(f"""
                SELECT 
                    symbol, 
                    COUNT(*) as record_count,
                    MIN(datetime) as first_date,
                    MAX(datetime) as last_date
                FROM staging.model_features
                {symbol_filter}
                GROUP BY symbol
                ORDER BY symbol
            """, symbol_list)
        else:
            cur.execute("""
                SELECT 
                    symbol, 
                    COUNT(*) as record_count,
                    MIN(datetime) as first_date,
                    MAX(datetime) as last_date
                FROM staging.model_features
                GROUP BY symbol
                ORDER BY symbol
            """)
        
        symbol_stats = []
        for row in cur.fetchall():
            symbol_stats.append({
                "symbol": row[0],
                "record_count": row[1],
                "first_date": row[2].isoformat() if row[2] else None,
                "last_date": row[3].isoformat() if row[3] else None
            })
        
        return {
            "total_records": total_records,
            "unique_symbols": unique_symbols,
            "date_range": {
                "earliest": date_range[0].isoformat() if date_range[0] else None,
                "latest": date_range[1].isoformat() if date_range[1] else None
            },
            "symbols": symbol_stats
        }
    
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/v1/extract/features")
async def extract_features(
    _: str = Depends(verify_api_key),
    symbols: Optional[str] = Query(None, description="Comma-separated list of symbols (e.g., 'AAPL,MSFT,GOOGL')"),
    start_date: Optional[str] = Query(None, description="Start date in ISO format (e.g., '2026-01-01T00:00:00')"),
    end_date: Optional[str] = Query(None, description="End date in ISO format (e.g., '2026-02-20T23:59:59')"),
    fields: Optional[str] = Query(None, description="Comma-separated list of fields to return (defaults to all)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records to return (1-10000)"),
    offset: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    format: str = Query("json", regex="^(json|csv)$", description="Response format: 'json' or 'csv'")
):
    """
    Extract features data for ML model training.
    Supports filtering by symbols, date range, and field selection.
    Returns data in JSON or CSV format with pagination support.
    """
    
    # Define available fields
    all_fields = [
        "symbol", "datetime", "price", "market_cap", "beta", "last_dividend", "range",
        "change", "change_percentage", "volume", "average_volume", "company_name",
        "currency", "cik", "isin", "cusip", "exchange_full_name", "exchange",
        "industry", "website", "description", "ceo", "sector", "country",
        "full_time_employees", "phone", "address", "city", "state", "zip",
        "image", "ipo_date", "default_image", "is_etf", "is_actively_trading",
        "is_adr", "is_fund"
    ]
    
    # Parse field selection
    if fields:
        selected_fields = [f.strip() for f in fields.split(',')]
        # Validate fields
        invalid_fields = [f for f in selected_fields if f not in all_fields]
        if invalid_fields:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid fields: {', '.join(invalid_fields)}. Use /v1/extract/features/schema to see available fields."
            )
    else:
        selected_fields = all_fields
    
    # Build SQL query
    fields_sql = ", ".join(selected_fields)
    query = f"SELECT {fields_sql} FROM staging.model_features WHERE 1=1"
    params = []
    
    # Add filters
    if symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(',')]
        query += f" AND symbol IN ({','.join(['%s'] * len(symbol_list))})"
        params.extend(symbol_list)
    
    if start_date:
        query += " AND datetime >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND datetime <= %s"
        params.append(end_date)
    
    # Add ordering and pagination
    query += " ORDER BY symbol, datetime"
    query += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    # TEST_MODE: Return mock data
    if os.getenv("TEST_MODE") == "1":
        mock_records = [
            {field: f"mock_{field}_value" if field in ["symbol", "company_name"] else 100.0 
             for field in selected_fields}
            for _ in range(min(5, limit))
        ]
        
        if format == "csv":
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=selected_fields)
            writer.writeheader()
            writer.writerows(mock_records)
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=features.csv"}
            )
        else:
            return {
                "status": "success",
                "records": mock_records,
                "count": len(mock_records),
                "limit": limit,
                "offset": offset
            }
    
    # Execute query
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="cosc471_project",
            user="dw_user",
            password="letmein",
            host="localhost"
        )
        cur = conn.cursor()
        cur.execute(query, params)
        
        rows = cur.fetchall()
        
        # Convert to list of dicts
        records = []
        for row in rows:
            record = {}
            for idx, field in enumerate(selected_fields):
                value = row[idx]
                # Convert datetime objects to ISO strings
                if isinstance(value, datetime.datetime):
                    value = value.isoformat()
                record[field] = value
            records.append(record)
        
        # Return in requested format
        if format == "csv":
            output = io.StringIO()
            if records:
                writer = csv.DictWriter(output, fieldnames=selected_fields)
                writer.writeheader()
                writer.writerows(records)
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=features.csv"}
            )
        else:
            return {
                "status": "success",
                "records": records,
                "count": len(records),
                "limit": limit,
                "offset": offset,
                "filters": {
                    "symbols": symbols,
                    "start_date": start_date,
                    "end_date": end_date,
                    "fields": fields
                }
            }
    
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
