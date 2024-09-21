import logging
import pandas as pd
import pandas_market_calendars as mcal
from typing import Set
from pytz import timezone
from datetime import datetime

def read_symbols_from_file(file_path: str) -> Set[str]:
    try:
        with open(file_path, 'r') as f:
            return set(symbol.strip().upper() for symbol in f.readlines() if symbol.strip())
    except IOError as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return set()

def get_symbols(file_path: str = None, symbol_list: str = None) -> Set[str]:
    symbols = set()
    
    if file_path:
        symbols.update(read_symbols_from_file(file_path))
    
    if symbol_list:
        symbols.update(symbol.strip().upper() for symbol in symbol_list.split(',') if symbol.strip())
    
    if not symbols:
        raise ValueError("No symbols provided. Use --file and/or --symbols to specify symbols.")
    
    return symbols

def calculate_start_date(ndays):
    nyse = mcal.get_calendar("NYSE")
    end_date = (
        pd.Timestamp.now(tz=timezone("America/New_York"))
        .floor("D")
        .tz_localize(None)
    )
    start_date = pd.Timestamp("2000-01-01")
    valid_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    if len(valid_days) < ndays:
        raise ValueError(
            f"Not enough trading days available. Requested: {ndays}, Available: {len(valid_days)}"
        )
    calculated_start_date = valid_days[-ndays]
    print(f"Start Date: {calculated_start_date.strftime('%Y-%m-%d')}")
    return calculated_start_date


