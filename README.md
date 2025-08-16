# Crypto Price Tracker ETL

A simple ETL pipeline that fetches real-time cryptocurrency prices from the [CoinGecko API](https://www.coingecko.com/en/api), cleans the data, and stores it in a local SQLite database.  
It also exports a CSV of the latest snapshot and computes top price movers.

---

## 🚀 Features
- Extracts live crypto data from the CoinGecko API  
- Cleans and standardizes data (ranks, market caps, prices, volumes)  
- Loads the data into a SQLite database (`crypto.db`)  
- Creates helpful indices for fast querying  
- Exports:  
  - `data/exports/latest_snapshot.csv` → most recent coin data  
  - `data/exports/top_movers.csv` → top 10 gainers & losers in the last 24h  

---

## 🛠️ Tech Stack
- **Python 3**
- **Pandas**
- **SQLite**
- **Requests** (for API calls)

---

## 📂 Project Structure
crypto-etl-pipeline/
│── pipeline.py # Main ETL script
│── README.md # Project documentation
│── data/
│ ├── warehouse/crypto.db # SQLite database
│ └── exports/ # Exported CSV files


---

## ▶️ How to Run

1. Clone this repo:
   ```bash
   git clone https://github.com/Priscillagit/crypto-etl-pipeline.git
   cd crypto-etl-pipeline

2. Create and activate a virtual environment (optional but recommended):
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows


3. Install dependencies:
pip install requests pandas
Run the pipeline:
python pipeline.py

📊 Example Output

## Database:
- Table: crypto_prices
- Columns: timestamp, id, symbol, name, current_price, market_cap, total_volume, price_change_24h, price_change_pct_24h

## Exports:
- data/exports/latest_snapshot.csv
- data/exports/top_movers.csv

## ✨ Next Steps 
- Automate with a cron job / Task Scheduler to fetch prices daily
- Visualize with a dashboard (Streamlit, Tableau, Power BI, or Excel)
- Add more coins or historical tracking


👩‍💻 Built by Priscillagit as part of a portfolio of data projects.
