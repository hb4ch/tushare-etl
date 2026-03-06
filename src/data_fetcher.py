import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", category=FutureWarning)

from src.utils import get_date_range, check_local_data_freshness, save_to_parquet, load_from_parquet
from config import get_api_token, STOCK_BASIC_DIR, STOCK_TICKS_DIR, STOCK_COMPANY_DIR, DEFAULT_FETCH_YEARS, FETCH_MAX_WORKERS, FETCH_RATE_LIMIT_PAUSE

logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self):
        """Initialize Tushare API connection"""
        self.token = get_api_token()
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        logger.info("Tushare API initialized successfully")

    def fetch_stock_basic_info(self):
        """Fetch basic information for all A-share stocks"""
        logger.info("Fetching stock basic information...")

        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date,market,exchange'
            )

            if df.empty:
                logger.warning("No stock basic information returned")
                return None

            filepath = STOCK_BASIC_DIR / "stock_basic.parquet"
            save_to_parquet(df, filepath)

            logger.info(f"Fetched {len(df)} stocks basic information")
            return df

        except Exception as e:
            logger.error(f"Error fetching stock basic info: {e}")
            return None

    def fetch_stock_daily_data(self, ts_code, start_date=None, end_date=None):
        """Fetch daily data for a specific stock"""
        if start_date is None or end_date is None:
            start_date, end_date = get_date_range(DEFAULT_FETCH_YEARS)

        logger.debug(f"Fetching daily data for {ts_code} from {start_date} to {end_date}")

        try:
            has_local_data, latest_date = check_local_data_freshness(ts_code, STOCK_TICKS_DIR)

            if has_local_data and latest_date:
                latest_date_obj = datetime.strptime(latest_date, '%Y%m%d')

                today_str = datetime.now().strftime('%Y%m%d')
                last_trade_day = datetime.now()
                while last_trade_day.weekday() >= 5:
                    last_trade_day -= timedelta(days=1)
                last_trade_day_str = last_trade_day.strftime('%Y%m%d')
                if latest_date == last_trade_day_str or latest_date == today_str:
                    logger.debug(f"Local data for {ts_code} is already up to date (latest: {latest_date})")
                    return load_from_parquet(STOCK_TICKS_DIR / f"{ts_code}.parquet"), False

                days_since_latest = (datetime.now() - latest_date_obj).days
                if days_since_latest <= 7:
                    start_date = (latest_date_obj + timedelta(days=1)).strftime('%Y%m%d')
                    logger.debug(f"Local data found, fetching from {start_date} onwards")

            price_df = ts.pro_bar(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount',
                adj='qfq'
            )

            basic_df = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
            )

            if price_df.empty:
                logger.warning(f"No daily price data returned for {ts_code}")
                return None, True

            if basic_df.empty:
                logger.warning(f"No daily basic data returned for {ts_code}")
                df = price_df
            else:
                df = pd.merge(price_df, basic_df, on=['ts_code', 'trade_date'], how='left')

            df = df.sort_values('trade_date')

            filepath = STOCK_TICKS_DIR / f"{ts_code}.parquet"

            if has_local_data:
                existing_df = load_from_parquet(filepath)
                if existing_df is not None:
                    df = pd.concat([existing_df, df]).drop_duplicates(subset=['trade_date'])
                    df = df.sort_values('trade_date')

            save_to_parquet(df, filepath)

            logger.debug(f"Fetched {len(df)} days of data for {ts_code} (including basic data)")
            return df, True

        except Exception as e:
            logger.error(f"Error fetching daily data for {ts_code}: {e}")
            return None, True

    def fetch_all_stocks_data(self, stock_list=None):
        """Fetch daily data for all stocks or specified list using concurrent threads"""
        if stock_list is None:
            basic_df = self.fetch_stock_basic_info()
            if basic_df is None:
                logger.error("Failed to fetch stock basic info")
                return []

            stock_list = basic_df['ts_code'].tolist()

        total = len(stock_list)
        logger.info(f"Starting to fetch data for {total} stocks with {FETCH_MAX_WORKERS} workers")

        successful = []
        failed = []
        lock = threading.Lock()
        api_calls = 0
        api_calls_window_start = time.monotonic()
        rate_limit_lock = threading.Lock()
        fetch_start_time = time.monotonic()

        def _fetch_one(ts_code):
            nonlocal api_calls, api_calls_window_start

            with rate_limit_lock:
                now = time.monotonic()
                elapsed = now - api_calls_window_start
                if elapsed >= 60:
                    api_calls = 0
                    api_calls_window_start = now
                elif api_calls >= 700:
                    sleep_time = 60 - elapsed + FETCH_RATE_LIMIT_PAUSE
                    logger.debug(f"Rate limit approaching ({api_calls} calls in {elapsed:.0f}s), pausing {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    api_calls = 0
                    api_calls_window_start = time.monotonic()

            try:
                df, api_called = self.fetch_stock_daily_data(ts_code)
                if api_called:
                    with rate_limit_lock:
                        api_calls += 2

                with lock:
                    if df is not None and not df.empty:
                        successful.append(ts_code)
                    else:
                        failed.append(ts_code)
                    done = len(successful) + len(failed)
                    elapsed_total = time.monotonic() - fetch_start_time
                    rate = done / elapsed_total if elapsed_total > 0 else 0
                    eta = (total - done) / rate if rate > 0 else 0
                    print(f"\rFetching: {done}/{total} ({len(successful)} ok, {len(failed)} failed) | {rate:.1f} stocks/s | ETA {eta:.0f}s", end="", flush=True)
            except Exception as e:
                logger.error(f"Failed to fetch data for {ts_code}: {e}")
                with lock:
                    failed.append(ts_code)

        with ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_one, code): code for code in stock_list}
            for future in as_completed(futures):
                pass

        print()
        logger.info(f"Data fetching completed: {len(successful)} successful, {len(failed)} failed")

        if failed:
            logger.warning(f"Failed stocks ({len(failed)}): {failed[:20]}{'...' if len(failed) > 20 else ''}")

        return successful

    def fetch_stock_company_info(self):
        """Fetch company information for all A-share stocks"""
        logger.info("Fetching stock company information...")

        try:
            df = self.pro.stock_company(
                exchange='',
                fields='ts_code,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,employees,main_business,business_scope'
            )

            if df.empty:
                logger.warning("No stock company information returned")
                return None

            filepath = STOCK_COMPANY_DIR / "stock_company.parquet"
            save_to_parquet(df, filepath)

            logger.info(f"Fetched {len(df)} stocks company information")
            return df

        except Exception as e:
            logger.error(f"Error fetching stock company info: {e}")
            return None

    def synchronize_stock_data(self):
        """Synchronize stock data by getting latest company list and managing tick data

        This function:
        1. Fetches the latest list of A-share companies from Tushare API
        2. Compares with existing tick data files
        3. Deletes tick data for stocks that are no longer listed
        4. Fetches tick data for newly listed stocks

        Returns:
            dict: Summary of synchronization results
        """
        logger.info("Starting stock data synchronization...")

        try:
            logger.info("Fetching latest stock basic information...")
            current_basic_df = self.fetch_stock_basic_info()
            if current_basic_df is None:
                logger.error("Failed to fetch current stock basic info")
                return None

            current_stocks = set(current_basic_df['ts_code'].tolist())
            logger.info(f"Found {len(current_stocks)} currently listed stocks")

            logger.info("Scanning existing tick data files...")
            existing_tick_files = list(STOCK_TICKS_DIR.glob("*.parquet"))
            existing_stocks = set(f.stem for f in existing_tick_files)
            logger.info(f"Found {len(existing_stocks)} stocks with existing tick data")

            stocks_to_remove = existing_stocks - current_stocks
            stocks_to_add = current_stocks - existing_stocks

            logger.info(f"Stocks to remove (delisted): {len(stocks_to_remove)}")
            logger.info(f"Stocks to add (newly listed): {len(stocks_to_add)}")

            removed_count = 0
            if stocks_to_remove:
                logger.info("Removing tick data for delisted stocks...")
                for ts_code in stocks_to_remove:
                    try:
                        tick_file = STOCK_TICKS_DIR / f"{ts_code}.parquet"
                        if tick_file.exists():
                            tick_file.unlink()
                            logger.info(f"Removed tick data for {ts_code}")
                            removed_count += 1
                    except Exception as e:
                        logger.error(f"Failed to remove tick data for {ts_code}: {e}")

            added_count = 0
            if stocks_to_add:
                logger.info("Fetching tick data for newly listed stocks...")
                stocks_to_add_list = list(stocks_to_add)
                successful_stocks = self.fetch_all_stocks_data(stocks_to_add_list)
                added_count = len(successful_stocks)

                if len(successful_stocks) < len(stocks_to_add_list):
                    failed_stocks = set(stocks_to_add_list) - set(successful_stocks)
                    logger.warning(f"Failed to fetch data for {len(failed_stocks)} newly listed stocks: {list(failed_stocks)}")

            summary = {
                'total_current': len(current_stocks),
                'total_existing': len(existing_stocks),
                'stocks_to_remove': list(stocks_to_remove),
                'stocks_to_add': list(stocks_to_add),
                'removed_count': removed_count,
                'added_count': added_count
            }

            logger.info("Stock data synchronization completed successfully")
            logger.info(f"Summary: {removed_count} stocks removed, {added_count} stocks added")

            return summary

        except Exception as e:
            logger.error(f"Error during stock data synchronization: {e}")
            return None
