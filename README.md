# tushare-etl

A standalone ETL pipeline for fetching A-share (Chinese stock market) data from the [Tushare](https://tushare.pro/) API and storing it as Parquet files.

## What it does

- **Stock daily data**: Price (OHLCV), valuation ratios (PE/PB/PS), market cap, turnover, dividend yield — merged from `pro_bar` and `daily_basic` APIs
- **Stock basic info**: Code, name, industry, area, listing date, market, exchange
- **Company info**: Chairman, employees, registered capital, introduction, main business, business scope
- **Dragon Tiger List (龙虎榜)**: Daily summary and institutional trading details for stocks with unusual activity
- **Incremental updates**: Only fetches new data since last run; skips stocks already up to date
- **Concurrent fetching**: Multi-threaded with automatic rate limiting (configurable workers)

## Setup

```bash
cd tushare-etl
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `tokens.txt` file in the project root containing your Tushare API token:

```bash
echo "your_tushare_token_here" > tokens.txt
```

## Usage

### Fetch stock data

```bash
# Fetch all listed stocks (incremental)
python main.py fetch

# Fetch specific stocks only
python main.py fetch --stocks 000001.SZ,600036.SH
```

This fetches:
1. Company information for all stocks (saved to `data/stock_company/`)
2. Daily price + fundamental data per stock (saved to `data/stock_ticks/{ts_code}.parquet`)

### Synchronize stock list

```bash
python main.py sync
```

Compares the latest listed stocks from Tushare with local tick data:
- Removes parquet files for delisted stocks
- Fetches data for newly listed stocks

### Fetch Dragon Tiger List (龙虎榜)

```bash
# Incremental fetch (last 365 days, skips existing dates)
python main.py toplist

# Specific date range
python main.py toplist --range 20250101,20250131

# Force refresh all data in range
python main.py toplist --range 20250101,20250131 --force

# Skip institutional details
python main.py toplist --no-inst

# Show statistics only
python main.py toplist --stats
```

### Options

```bash
python main.py --verbose fetch    # Debug logging
python main.py -v toplist         # Same thing
```

## Data Output

All data is stored as Parquet files (snappy compression) under `data/`:

```
data/
├── stock_basic/
│   └── stock_basic.parquet        # All listed stocks metadata
├── stock_company/
│   └── stock_company.parquet      # Company details
├── stock_ticks/
│   ├── 000001.SZ.parquet          # Per-stock daily OHLCV + fundamentals
│   ├── 600036.SH.parquet
│   └── ...
├── top_list/
│   ├── 20250101.parquet           # Dragon Tiger List daily summary
│   ├── 20250102.parquet
│   └── ...
└── top_inst/
    ├── 20250101.parquet           # Institutional trading details
    ├── 20250102.parquet
    └── ...
```

### Schema: Stock Ticks

| Column | Description |
|--------|-------------|
| ts_code | Stock code (e.g. `000001.SZ`) |
| trade_date | Trade date (`YYYYMMDD`) |
| open, high, low, close | OHLC prices (forward-adjusted) |
| pre_close | Previous close |
| change, pct_chg | Price change / percentage change |
| vol, amount | Volume (shares) / trading amount (RMB) |
| turnover_rate | Turnover rate (%) |
| pe, pe_ttm | P/E ratio / P/E TTM |
| pb | P/B ratio |
| ps, ps_ttm | P/S ratio / P/S TTM |
| dv_ratio, dv_ttm | Dividend yield / TTM |
| total_share, float_share, free_share | Share counts (万) |
| total_mv, circ_mv | Total / circulating market cap (万元) |

### Schema: Dragon Tiger List

| Column | Description |
|--------|-------------|
| trade_date | Trade date |
| ts_code | Stock code |
| name | Company name |
| close | Closing price |
| pct_change | Price change % |
| turnover_rate | Turnover rate % |
| amount | Total trading amount |
| l_sell, l_buy | DTL sell/buy amount |
| net_amount | Net buy (buy - sell) |
| net_rate | Net buy rate % |
| amount_rate | DTL amount / total volume % |
| reason | Listing reason |

## Using from another project

The simplest way to share the data with other projects is to **symlink the `data/` directory**:

```bash
# Example: use tushare-etl data in ~/trend-agent
cd ~/trend-agent
ln -s ~/tushare-etl/data data

# Now ~/trend-agent/data points to ~/tushare-etl/data
# Your code can read parquet files directly:
#   pd.read_parquet("data/stock_ticks/000001.SZ.parquet")
```

You can also symlink the entire project if you want to import its modules:

```bash
cd ~/trend-agent
ln -s ~/tushare-etl tushare_etl

# Then in Python:
#   from tushare_etl.src.data_fetcher import DataFetcher
#   from tushare_etl.src.top_list_manager import TopListManager
```

### Example: loading data in another project

```python
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")  # symlinked to ~/tushare-etl/data

# Load stock ticks
df = pd.read_parquet(DATA_DIR / "stock_ticks" / "000001.SZ.parquet")
print(df.tail())

# Load all Dragon Tiger List data for January 2025
from pathlib import Path
top_list_dir = DATA_DIR / "top_list"
dfs = []
for f in sorted(top_list_dir.glob("202501*.parquet")):
    dfs.append(pd.read_parquet(f))
df_top = pd.concat(dfs, ignore_index=True)

# Load stock basic info
basic = pd.read_parquet(DATA_DIR / "stock_basic" / "stock_basic.parquet")
```

### Example: using the TopListManager from another project

```python
import sys
sys.path.insert(0, str(Path.home() / "tushare-etl"))

from src.top_list_manager import TopListManager

manager = TopListManager()
df = manager.load_top_list(start_date='20250101', end_date='20250131')
stats = manager.get_stats()
```

## Configuration

Key settings in `config.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `DEFAULT_FETCH_YEARS` | 4 | Years of history to fetch for new stocks |
| `FETCH_MAX_WORKERS` | 8 | Concurrent threads for data fetching |
| `FETCH_RATE_LIMIT_PAUSE` | 0.5 | Extra pause when approaching rate limit |

## Rate Limiting

Tushare has API quota limits (~800 calls/minute for most endpoints). The fetcher automatically:
- Tracks API calls per 60-second window
- Pauses when approaching 700 calls/minute
- Each stock fetch makes 2 API calls (`pro_bar` + `daily_basic`)
