"""
Top List (Dragon Tiger List) Data Manager

Handles fetching, storing, and managing Dragon Tiger List data from Tushare API.
"""

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from typing import Optional, Tuple, List

from config import get_api_token, TOP_LIST_DIR, TOP_INST_DIR

logger = logging.getLogger(__name__)


class TopListManager:
    """Manager for Dragon Tiger List data fetching and storage"""

    def __init__(self):
        """Initialize Tushare API connection"""
        self.token = get_api_token()
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        logger.info("TopListManager: Tushare API initialized successfully")

    def _get_latest_date_in_file(self, filepath):
        """Get the latest trade_date from a parquet file"""
        try:
            if not filepath.exists():
                return None
            df = pd.read_parquet(filepath)
            if df.empty:
                return None
            return df['trade_date'].max()
        except Exception as e:
            logger.warning(f"Error reading {filepath}: {e}")
            return None

    def _get_trade_dates_in_range(self, start_date, end_date):
        """Get list of trade dates (excluding weekends)"""
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')

        trade_dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                trade_dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)

        return trade_dates

    def fetch_top_list_daily(self, trade_date, force=False):
        """Fetch Dragon Tiger List data for a specific date"""
        filepath = TOP_LIST_DIR / f"{trade_date}.parquet"

        if not force and filepath.exists():
            logger.debug(f"Top list data for {trade_date} already exists, skipping")
            return pd.read_parquet(filepath), False

        try:
            logger.info(f"Fetching top_list data for {trade_date}")
            df = self.pro.top_list(trade_date=trade_date)

            if df is None or df.empty:
                logger.debug(f"No top_list data for {trade_date}")
                return None, True

            df.to_parquet(filepath, index=False, compression='snappy')
            logger.info(f"Saved {len(df)} top_list records for {trade_date}")

            return df, True

        except Exception as e:
            logger.error(f"Error fetching top_list for {trade_date}: {e}")
            return None, True

    def fetch_top_inst_daily(self, trade_date, force=False):
        """Fetch institutional trading details for a specific date"""
        filepath = TOP_INST_DIR / f"{trade_date}.parquet"

        if not force and filepath.exists():
            logger.debug(f"Top inst data for {trade_date} already exists, skipping")
            return pd.read_parquet(filepath), False

        try:
            logger.info(f"Fetching top_inst data for {trade_date}")
            df = self.pro.top_inst(trade_date=trade_date)

            if df is None or df.empty:
                logger.debug(f"No top_inst data for {trade_date}")
                return None, True

            df.to_parquet(filepath, index=False, compression='snappy')
            logger.info(f"Saved {len(df)} top_inst records for {trade_date}")

            return df, True

        except Exception as e:
            logger.error(f"Error fetching top_inst for {trade_date}: {e}")
            return None, True

    def fetch_top_list_range(self, start_date, end_date=None, include_inst=True, force=False):
        """Fetch Dragon Tiger List data for a date range"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        trade_dates = self._get_trade_dates_in_range(start_date, end_date)

        logger.info(f"Fetching top_list data for {len(trade_dates)} trade dates from {start_date} to {end_date}")

        summary = {
            'dates_processed': 0,
            'top_list_records': 0,
            'top_inst_records': 0,
            'api_calls': 0,
            'skipped': 0,
            'errors': []
        }

        for i, trade_date in enumerate(trade_dates):
            logger.info(f"Processing {i+1}/{len(trade_dates)}: {trade_date}")

            df_list, api_called = self.fetch_top_list_daily(trade_date, force=force)
            if api_called:
                summary['api_calls'] += 1
                time.sleep(0.2)

            if df_list is not None and not df_list.empty:
                summary['top_list_records'] += len(df_list)
                summary['dates_processed'] += 1
            elif not api_called:
                summary['skipped'] += 1

            if include_inst:
                df_inst, api_called = self.fetch_top_inst_daily(trade_date, force=force)
                if api_called:
                    summary['api_calls'] += 1
                    time.sleep(0.2)

                if df_inst is not None and not df_inst.empty:
                    summary['top_inst_records'] += len(df_inst)

            if summary['api_calls'] > 0 and summary['api_calls'] % 50 == 0:
                logger.info(f"Processed {i+1} dates, taking a short break...")
                time.sleep(2)

        logger.info(f"Top list fetch completed: {summary['dates_processed']} dates, "
                   f"{summary['top_list_records']} top_list records, "
                   f"{summary['top_inst_records']} top_inst records, "
                   f"{summary['api_calls']} API calls")

        return summary

    def fetch_recent(self, days_back=30, include_inst=True, force=False):
        """Fetch recent Dragon Tiger List data (incremental updates)"""
        latest_date = None
        for filepath in list(TOP_LIST_DIR.glob("*.parquet")):
            date = filepath.stem
            if latest_date is None or date > latest_date:
                latest_date = date

        if latest_date and not force:
            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
            start_date = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            logger.info(f"Incremental update: fetching from {start_date} (latest local: {latest_date})")
        else:
            start_dt = datetime.now() - timedelta(days=days_back)
            start_date = start_dt.strftime('%Y%m%d')
            if force:
                logger.info(f"Force refresh: fetching from {start_date}")
            else:
                logger.info(f"Initial fetch: fetching from {start_date}")

        end_date = datetime.now().strftime('%Y%m%d')

        return self.fetch_top_list_range(start_date, end_date, include_inst=include_inst, force=force)

    def load_top_list(self, start_date=None, end_date=None):
        """Load stored top_list data for a date range"""
        all_files = sorted(TOP_LIST_DIR.glob("*.parquet"))

        if not all_files:
            logger.warning("No top_list data files found")
            return pd.DataFrame()

        if start_date or end_date:
            filtered_files = []
            for f in all_files:
                date_str = f.stem
                if start_date and date_str < start_date:
                    continue
                if end_date and date_str > end_date:
                    continue
                filtered_files.append(f)
            all_files = filtered_files

        if not all_files:
            logger.warning("No top_list data files found in specified range")
            return pd.DataFrame()

        dfs = []
        for f in all_files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error reading {f}: {e}")

        if not dfs:
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        result = result.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)

        logger.info(f"Loaded {len(result)} top_list records from {len(dfs)} files")
        return result

    def load_top_inst(self, start_date=None, end_date=None):
        """Load stored top_inst data for a date range"""
        all_files = sorted(TOP_INST_DIR.glob("*.parquet"))

        if not all_files:
            logger.warning("No top_inst data files found")
            return pd.DataFrame()

        if start_date or end_date:
            filtered_files = []
            for f in all_files:
                date_str = f.stem
                if start_date and date_str < start_date:
                    continue
                if end_date and date_str > end_date:
                    continue
                filtered_files.append(f)
            all_files = filtered_files

        if not all_files:
            logger.warning("No top_inst data files found in specified range")
            return pd.DataFrame()

        dfs = []
        for f in all_files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error reading {f}: {e}")

        if not dfs:
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        result = result.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)

        logger.info(f"Loaded {len(result)} top_inst records from {len(dfs)} files")
        return result

    def get_stats(self):
        """Get statistics about stored top_list data"""
        top_list_files = list(TOP_LIST_DIR.glob("*.parquet"))
        top_inst_files = list(TOP_INST_DIR.glob("*.parquet"))

        dates = [f.stem for f in top_list_files]
        if dates:
            earliest = min(dates)
            latest = max(dates)
        else:
            earliest = latest = None

        total_list_records = 0
        for f in top_list_files:
            try:
                df = pd.read_parquet(f)
                total_list_records += len(df)
            except:
                pass

        total_inst_records = 0
        for f in top_inst_files:
            try:
                df = pd.read_parquet(f)
                total_inst_records += len(df)
            except:
                pass

        return {
            'top_list_files': len(top_list_files),
            'top_inst_files': len(top_inst_files),
            'top_list_records': total_list_records,
            'top_inst_records': total_inst_records,
            'date_range': {
                'earliest': earliest,
                'latest': latest
            }
        }
