import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def get_date_range(years_back=4):
    """Get date range for data fetching"""
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years_back * 365)).strftime('%Y%m%d')
    return start_date, end_date


def check_local_data_freshness(stock_code, data_dir):
    """Check if local data exists and how recent it is"""
    data_file = data_dir / f"{stock_code}.parquet"

    if not data_file.exists():
        return False, None

    try:
        df = pd.read_parquet(data_file)
        if df.empty:
            return False, None

        latest_date = df['trade_date'].max()
        return True, latest_date
    except Exception as e:
        logger.warning(f"Error reading local data for {stock_code}: {e}")
        return False, None


def save_to_parquet(df, filepath):
    """Save DataFrame to parquet format"""
    try:
        df.to_parquet(filepath, index=False, compression='snappy')
        logger.debug(f"Saved data to {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error saving to parquet: {e}")
        return False


def load_from_parquet(filepath):
    """Load DataFrame from parquet format"""
    try:
        if not filepath.exists():
            return None
        return pd.read_parquet(filepath)
    except Exception as e:
        logger.error(f"Error loading from parquet: {e}")
        return None
