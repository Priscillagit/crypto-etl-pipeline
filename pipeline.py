from __future__ import annotations
import argparse
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import requests
import pandas as pd

# ---------------- Paths & Config ----------------
BASE_DIR = Path(__file__).resolve().parent
WAREHOUSE = BASE_DIR / "data" / "warehouse"
WAREHOUSE.mkdir(parents=True, exist_ok=True)
DB_PATH = WAREHOUSE / "crypto_prices.db"
SOURCE = "coingecko"

# Choose the coins you want to track
COINS = [
    {"id": "bitcoin", "symbol": "BTC"},
    {"id": "ethereum", "symbol": "ETH"},
    {"id": "solana", "symbol": "SOL"},
    {"id": "cardano", "symbol": "ADA"},
]

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("crypto-pipeline")

# ---------------- DB Setup ----------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS prices (
    ts_utc TEXT NOT NULL,
    coin_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price_usd REAL,
    market_cap_usd REAL,
    volume_24h_usd REAL,
    change_24h_pct REAL,
    source TEXT NOT NULL,
    PRIMARY KEY (ts_utc, coin_id)
);
"""
INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_prices_coin_ts ON prices(coin_id, ts_utc);
"""

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CREATE_SQL)
        conn.execute(INDEX_SQL)
    log.info(f"DB ready: {DB_PATH}")

# ---------------- Fetch ----------------
def fetch_current_prices(coin_ids: List[str]) -> List[Dict[str, Any]]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "price_change_percentage": "24h",
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def normalize_rows(payload: List[Dict[str, Any]]) -> pd.DataFrame:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
    rows = []
    for item in payload:
        rows.append({
            "ts_utc": now,
            "coin_id": item.get("id"),
            "symbol": (item.get("symbol") or "").upper(),
            "price_usd": item.get("current_price"),
            "market_cap_usd": item.get("market_cap"),
            "volume_24h_usd": item.get("total_volume"),
            "change_24h_pct": (item.get("price_change_percentage_24h") or 0.0),
            "source": SOURCE,
        })
    return pd.DataFrame(rows)

# ---------------- Load ----------------
def load_into_sqlite(df: pd.DataFrame):
    if df.empty:
        log.warning("No rows to load.")
        return
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("prices", conn, if_exists="append", index=False)
    log.info(f"Loaded {len(df)} rows into {DB_PATH} (prices).")

# ---------------- Analyze/Export ----------------
def export_snapshots():
    with sqlite3.connect(DB_PATH) as conn:
        # Latest snapshot for each coin
        latest_ts = pd.read_sql_query(
            "SELECT MAX(ts_utc) as ts FROM prices;", conn
        )["ts"].iloc[0]
        snap = pd.read_sql_query(
            """
            SELECT coin_id, symbol, ts_utc, price_usd, market_cap_usd, volume_24h_usd, change_24h_pct
            FROM prices
            WHERE ts_utc = ?
            ORDER BY market_cap_usd DESC;
            """,
            conn, params=(latest_ts,)
        )
    snap_path = WAREHOUSE / "latest_snapshot.csv"
    snap.to_csv(snap_path, index=False)
    log.info(f"Wrote {snap_path}")

    # Top movers by 24h change from that snapshot
    movers = snap.sort_values("change_24h_pct", ascending=False)
    movers_path = WAREHOUSE / "top_movers_24h.csv"
    movers.to_csv(movers_path, index=False)
    log.info(f"Wrote {movers_path}")

def run_fetch():
    init_db()
    coin_ids = [c["id"] for c in COINS]
    payload = fetch_current_prices(coin_ids)
    df = normalize_rows(payload)
    load_into_sqlite(df)
    log.info("Fetch step complete ✅")

def run_analyze():
    export_snapshots()
    log.info("Analyze step complete ✅")

# ---------------- CLI ----------------
def main():
    parser = argparse.ArgumentParser(description="Crypto Prices Pipeline")
    parser.add_argument("command", nargs="?", default="all", choices=["fetch", "analyze", "all"],
                        help="fetch (API→DB), analyze (DB→CSV), or all")
    args = parser.parse_args()

    if args.command in ("fetch", "all"):
        run_fetch()
    if args.command in ("analyze", "all"):
        run_analyze()

if __name__ == "__main__":
    main()
