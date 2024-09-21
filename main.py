# main.py

import json
import asyncio
import logging
import argparse
from pathlib import Path
from colorama import init
import signal

from src.core.config import config
from src.utils.config_manager import config_manager
from src.processing.zscore_processor import ZScoreProcessor
from src.streaming.data_stream_manager import DataStreamManager
from src.utils.helpers import get_symbols, calculate_start_date
from src.parallel_processor import ParallelProcessor

def parse_arguments():
    parser = argparse.ArgumentParser(description="Run asynchronous data stream for stock symbols")
    parser.add_argument('--file', type=str, help='File containing symbols (one per line)')
    parser.add_argument('--symbols', type=str, help='Comma-separated list of symbols')
    parser.add_argument('--ndays', type=int, default=2, help='Number of days of historical data to fetch')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--test', action='store_true', help='Run in test mode using historical data')
    parser.add_argument('--days_ago', type=int, default=1, help='Number of days ago to start simulation in test mode')
    parser.add_argument('--stream_type', type=str, choices=['trades', 'bars'], default='trades',
                        help='Choose what to subscribe to in the real-time stream (default: trades)')
    parser.add_argument('--sigma_thresh', type=float, default=None, help='Sigma threshold for alerts')
    parser.add_argument('--zscore_trend_thresh', type=float, default=None, help='Z-score trend threshold for alerts')
    parser.add_argument('--use_multiprocessing', action='store_true', help='Enable multiprocessing for data processing')
    parser.add_argument('--num_processes', type=int, default=8, help='Number of processes to use when multiprocessing is enabled')
    return parser.parse_args()

def setup_logging(debug=False):
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("app.log"),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging initialized at {'DEBUG' if debug else 'INFO'} level.")

def update_config_thresholds(config_manager, sigma_thresh, zscore_trend_thresh):
    if sigma_thresh is not None:
        config_manager.update('sigma_thresh', sigma_thresh)
    if zscore_trend_thresh is not None:
        config_manager.update('zscore_trend_thresh', zscore_trend_thresh)

async def main():
    args = parse_arguments()

    # Initialize colorama
    init(autoreset=True)

    # Setup logging
    setup_logging(args.debug)

    logger = logging.getLogger()

    logging.info("Starting program with the following parameters:")
    for arg, value in vars(args).items():
        logging.info(f"  {arg:<20}: {value}")

    # Update thresholds in config if provided
    update_config_thresholds(config_manager, args.sigma_thresh, args.zscore_trend_thresh)

    try:
        symbols = get_symbols(file_path=args.file, symbol_list=args.symbols)
        if not config.API_KEY or not config.API_SECRET:
            raise ValueError("Alpaca API credentials not found in environment variables.")
    except ValueError as e:
        logging.error(str(e))
        return

    logging.info(f"Processing data for symbols: {', '.join(sorted(symbols))}")

    processor = ZScoreProcessor()
    parallel_processor = None
    if args.use_multiprocessing:
        parallel_processor = ParallelProcessor(args.num_processes, processor)
        logging.info(f"Multiprocessing enabled with {args.num_processes} processes")
    
    manager = DataStreamManager(
        symbols=symbols,
        processor=processor,
        parallel_processor=parallel_processor,
        ndays=args.ndays,
        calculate_start_date=calculate_start_date,
        test_mode=args.test,
        days_ago=args.days_ago,
        stream_type=args.stream_type
    )

    config_path = config_manager.config_file
    print(f"To update lambda multipliers, sigma threshold, or zscore trend threshold, edit the values in {config_path}")

    loop = asyncio.get_event_loop()

    # Define shutdown handler
    def shutdown():
        logging.info("Received shutdown signal. Cancelling tasks...")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            # Signal handlers are not implemented on some platforms (e.g., Windows)
            pass

    try:
        await manager.run()
    except asyncio.CancelledError:
        logging.info("Stream cancelled")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        await manager.cleanup()
        if parallel_processor:
            parallel_processor.shutdown()
        logging.info("Application shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")
