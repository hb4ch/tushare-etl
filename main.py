#!/usr/bin/env python3
"""
Tushare ETL - A-share Stock Data Fetcher

Fetches stock data, company information, and Dragon Tiger List data from Tushare API
and stores them as Parquet files for downstream analysis.
"""

import argparse
import logging
import sys

from src.data_fetcher import DataFetcher
from src.top_list_manager import TopListManager
from config import get_api_token

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger().setLevel(level)


def fetch_data(args):
    """Fetch stock data command"""
    logger.info("Starting data fetching process...")

    try:
        fetcher = DataFetcher()

        logger.info("Fetching company information...")
        company_df = fetcher.fetch_stock_company_info()
        if company_df is not None:
            logger.info(f"Successfully fetched company information for {len(company_df)} stocks")
        else:
            logger.warning("Failed to fetch company information, continuing without it")

        if args.stocks:
            stock_list = args.stocks.split(',')
            successful = fetcher.fetch_all_stocks_data(stock_list)
        else:
            successful = fetcher.fetch_all_stocks_data()

        logger.info(f"Data fetching completed. Successfully fetched data for {len(successful)} stocks.")
        return True

    except Exception as e:
        logger.error(f"Data fetching failed: {e}")
        return False


def synchronize_data(args):
    """Synchronize stock data command"""
    logger.info("Starting stock data synchronization...")

    try:
        fetcher = DataFetcher()
        summary = fetcher.synchronize_stock_data()

        if summary is None:
            logger.error("Stock data synchronization failed")
            return False

        print("\n=== Stock Data Synchronization Results ===")
        print(f"Currently listed stocks: {summary['total_current']}")
        print(f"Existing tick data files: {summary['total_existing']}")
        print(f"Stocks removed (delisted): {summary['removed_count']}")
        print(f"Stocks added (newly listed): {summary['added_count']}")

        if summary['stocks_to_remove']:
            print(f"\nDelisted stocks removed: {', '.join(summary['stocks_to_remove'][:10])}")
            if len(summary['stocks_to_remove']) > 10:
                print(f"... and {len(summary['stocks_to_remove']) - 10} more")

        if summary['stocks_to_add']:
            print(f"\nNewly listed stocks added: {', '.join(summary['stocks_to_add'][:10])}")
            if len(summary['stocks_to_add']) > 10:
                print(f"... and {len(summary['stocks_to_add']) - 10} more")

        logger.info("Stock data synchronization completed successfully")
        return True

    except Exception as e:
        logger.error(f"Stock data synchronization failed: {e}")
        return False


def fetch_toplist(args):
    """Fetch Dragon Tiger List data command"""
    logger.info("Starting Dragon Tiger List data fetching...")

    try:
        manager = TopListManager()

        if args.stats:
            stats = manager.get_stats()
            print("\n=== Dragon Tiger List Data Statistics ===")
            print(f"Top list files: {stats['top_list_files']}")
            print(f"Top inst files: {stats['top_inst_files']}")
            print(f"Total top_list records: {stats['top_list_records']:,}")
            print(f"Total top_inst records: {stats['top_inst_records']:,}")
            if stats['date_range']['earliest']:
                print(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
            return True

        if args.range:
            parts = args.range.split(',')
            if len(parts) == 2:
                start_date, end_date = parts
            else:
                start_date = parts[0]
                end_date = None
            summary = manager.fetch_top_list_range(
                start_date=start_date,
                end_date=end_date,
                include_inst=not args.no_inst,
                force=args.force
            )
        else:
            days_back = args.days if args.days else 365
            summary = manager.fetch_recent(
                days_back=days_back,
                include_inst=not args.no_inst,
                force=args.force
            )

        print("\n=== Dragon Tiger List Fetch Results ===")
        print(f"Dates processed: {summary['dates_processed']}")
        print(f"Top list records: {summary['top_list_records']:,}")
        print(f"Top inst records: {summary['top_inst_records']:,}")
        print(f"API calls made: {summary['api_calls']}")
        print(f"Dates skipped (already have data): {summary['skipped']}")

        if summary['errors']:
            print(f"\nErrors encountered: {len(summary['errors'])}")
            for error in summary['errors'][:5]:
                print(f"  - {error}")

        logger.info("Dragon Tiger List data fetching completed successfully")
        return True

    except Exception as e:
        logger.error(f"Dragon Tiger List fetching failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Tushare ETL - A-share Stock Data Fetcher',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch data for all stocks
  python main.py fetch

  # Fetch data for specific stocks
  python main.py fetch --stocks 000001.SZ,000002.SZ

  # Synchronize stock data (remove delisted, add newly listed)
  python main.py sync

  # Fetch recent Dragon Tiger List data (incremental, last 365 days)
  python main.py toplist

  # Fetch Dragon Tiger List data for specific date range
  python main.py toplist --range 20250101,20250131

  # Force refresh all data in range
  python main.py toplist --range 20250101,20250131 --force

  # Show statistics of stored top_list data
  python main.py toplist --stats
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch stock data')
    fetch_parser.add_argument(
        '--stocks', type=str,
        help='Comma-separated list of stock codes (e.g., 000001.SZ,000002.SZ)'
    )

    # Synchronize command
    subparsers.add_parser('sync', help='Synchronize stock data (remove delisted, add newly listed)')

    # Top List command
    toplist_parser = subparsers.add_parser('toplist', help='Fetch Dragon Tiger List data')
    toplist_parser.add_argument(
        '--range', type=str,
        help='Date range: start_date,end_date (YYYYMMDD) or start_date only'
    )
    toplist_parser.add_argument(
        '--days', type=int,
        help='Number of days to look back for incremental fetch (default: 365)'
    )
    toplist_parser.add_argument(
        '--no-inst', action='store_true',
        help='Skip fetching institutional details (top_inst)'
    )
    toplist_parser.add_argument(
        '--force', action='store_true',
        help='Force refetch all data even if local data exists'
    )
    toplist_parser.add_argument(
        '--stats', action='store_true',
        help='Show statistics of stored top_list data'
    )

    # Global options
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    setup_logging(args.verbose)

    try:
        get_api_token()
    except Exception as e:
        logger.error(f"API token validation failed: {e}")
        return 1

    if args.command == 'fetch':
        success = fetch_data(args)
    elif args.command == 'sync':
        success = synchronize_data(args)
    elif args.command == 'toplist':
        success = fetch_toplist(args)
    else:
        logger.error(f"Unknown command: {args.command}")
        return 1

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
