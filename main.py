from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from database import get_connection

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

VALID_TABLES = {
    "revenue_per_product_2021",
    "revenue_per_shop_202101",
    "top_k_products",
    "top_k_products_202101"
}

@app.get("/api/data")
def get_data(table: str):
    if table not in VALID_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    conn = get_connection()
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    conn.close()

    return df.to_dict(orient="records")