from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
STOCK_BASIC_DIR = DATA_DIR / "stock_basic"
STOCK_TICKS_DIR = DATA_DIR / "stock_ticks"
STOCK_COMPANY_DIR = DATA_DIR / "stock_company"
TOP_LIST_DIR = DATA_DIR / "top_list"
TOP_INST_DIR = DATA_DIR / "top_inst"

# Create data directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
STOCK_BASIC_DIR.mkdir(exist_ok=True)
STOCK_TICKS_DIR.mkdir(exist_ok=True)
STOCK_COMPANY_DIR.mkdir(exist_ok=True)
TOP_LIST_DIR.mkdir(exist_ok=True)
TOP_INST_DIR.mkdir(exist_ok=True)

# API Configuration
def get_api_token():
    """Read API token from tokens.txt file"""
    token_file = PROJECT_ROOT / "tokens.txt"
    if not token_file.exists():
        raise FileNotFoundError("tokens.txt file not found in project root")

    with open(token_file, 'r') as f:
        token = f.read().strip()

    if not token:
        raise ValueError("API token is empty in tokens.txt")

    return token

# Data configuration
DEFAULT_FETCH_YEARS = 4
DATA_FORMAT = "parquet"

# Data Fetch Concurrency Configuration
FETCH_MAX_WORKERS = 8
FETCH_RATE_LIMIT_PAUSE = 0.5
