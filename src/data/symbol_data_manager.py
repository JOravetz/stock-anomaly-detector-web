import pytz
import math
import logging
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from typing import Dict, Set, Callable
from datetime import datetime, timedelta, date
from ..core.symbol_data import SymbolData

class SymbolDataManager:
    def __init__(self, symbols: Set[str], ndays: int, calculate_start_date: Callable,
                 test_mode: bool = False, days_ago: int = 1):
        self.symbols = symbols.copy()
        self.symbol_data: Dict[str, SymbolData] = {}
        self.ndays = ndays
        self.calculate_start_date = calculate_start_date
        self.test_mode = test_mode
        self.days_ago = days_ago
        self.last_processed_price: Dict[str, float] = {}
        self.current_trading_day = date.today()
        self.price_trends = {symbol: {'last_action': None, 'extreme_price': None, 'day_high': None, 'day_low': None} for symbol in symbols}

    async def initialize_data(self, fetcher):
        nyse = mcal.get_calendar('NYSE')
        today = datetime.now(pytz.UTC).date()
        valid_dates = nyse.valid_days(start_date=today - timedelta(days=30), end_date=today)

        if self.test_mode:
            end_date = valid_dates[-self.days_ago]
            start_date = valid_dates[max(-len(valid_dates), -self.ndays - self.days_ago)]
        else:
            end_date = today
            start_date = valid_dates[max(-len(valid_dates), -self.ndays)]

        start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
        end_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)

        historical_data = await fetcher.fetch_historical_data(self.symbols, start_datetime, end_datetime)
        for symbol, df in historical_data:
            if not df.empty:
                self.symbol_data[symbol] = SymbolData(symbol=symbol, historical_data=df)

        self.initialize_symbol_data(start_datetime, end_datetime)

    def initialize_symbol_data(self, start_date: datetime, end_date: datetime):
        nyse = mcal.get_calendar('NYSE')
        symbols_to_remove = set()
        log_data = []

        for symbol, data in self.symbol_data.items():
            logging.debug(f"Initializing data for symbol: {symbol}")

            df = data.historical_data

            logging.debug(f"Retrieved {len(df)} rows of historical data for {symbol}")

            if df.empty or len(df) < 2:
                logging.warning(f"Insufficient data for {symbol:<6}. Removing from processing list.")
                symbols_to_remove.add(symbol)
                continue

            valid_days = nyse.valid_days(start_date=start_date.date(), end_date=end_date.date())
            logging.debug(f"Valid trading days for symbol {symbol}: {valid_days}")

            # Ensure the last_valid_day is the day before the current trading day
            if len(valid_days) > 1:
                last_valid_day = pd.Timestamp(valid_days[-2]).tz_convert('UTC')
            else:
                last_valid_day = None

            logging.debug(f"Last valid trading day for baseline data for {symbol}: {last_valid_day}")

            if last_valid_day is None:
                logging.warning(f"No valid trading days found for baseline data for {symbol:<6}. Removing from processing list.")
                symbols_to_remove.add(symbol)
                continue

            # Filter the data to include only the baseline period (up to and including last_valid_day)
            # Drop the time component for comparison
            df['date'] = df.index.date
            baseline_data = df[df['date'] == last_valid_day.date()]
            baseline_prices = baseline_data['close'].values

            logging.debug(f"Baseline data for {symbol} on {last_valid_day.date()}: {baseline_data}")
            logging.debug(f"Baseline prices for {symbol}: {baseline_prices}")

            if len(baseline_prices) < 2:
                logging.warning(f"Insufficient baseline data for {symbol:<6}. Removing from processing list.")
                symbols_to_remove.add(symbol)
                continue

            baseline_mean = np.mean(baseline_prices)
            baseline_std = np.std(baseline_prices)

            logging.debug(f"Baseline mean for {symbol}: {baseline_mean}")
            logging.debug(f"Baseline std deviation for {symbol}: {baseline_std}")

            data.baseline_prices = baseline_prices
            data.baseline_mean = baseline_mean
            data.baseline_std = baseline_std
            data.full_prices = df['close'].values

            logging.debug(f"Updated SymbolData for {symbol} with baseline prices shape: {data.baseline_prices.shape} "
                          f"and full prices shape: {data.full_prices.shape}")

            last_price = baseline_prices[-1] if len(baseline_prices) > 0 else None
            logging.debug(f"Last price from baseline for {symbol}: {last_price}")

            if baseline_std == 0 or math.isnan(baseline_std) or last_price is None or math.isnan(last_price):
                logging.warning(f"Invalid baseline std or last price for {symbol:<6}. Removing from processing list.")
                symbols_to_remove.add(symbol)
                continue

            baseline_zscore = (last_price - baseline_mean) / baseline_std
            logging.debug(f"Baseline z-score for {symbol}: {baseline_zscore}")

            if math.isnan(baseline_zscore) or math.isinf(baseline_zscore):
                logging.warning(f"Invalid z-score for {symbol:<6}. Removing from processing list.")
                symbols_to_remove.add(symbol)
                continue

            log_data.append({
                'symbol': symbol,
                'mean': baseline_mean,
                'std': baseline_std,
                'baseline_samples': len(baseline_prices),
                'total_samples': len(data.full_prices),
                'last_price': last_price,
                'zscore': baseline_zscore
            })

        for symbol in symbols_to_remove:
            if symbol in self.symbol_data:
                del self.symbol_data[symbol]
            if symbol in self.symbols:
                self.symbols.remove(symbol)

        logging.info(f"Removed {len(symbols_to_remove)} symbols due to insufficient or invalid data.")
        logging.info(f"Remaining symbols for processing: {', '.join(sorted(self.symbols))}")

        sorted_log_data = sorted(log_data, key=lambda x: x['zscore'])

        for data in sorted_log_data:
            logging.info(f"Baseline Data for {data['symbol']:<6}: "
                         f"mean: {data['mean']:>7.2f}, "
                         f"std: {data['std']:>6.2f}, "
                         f"samples: {data['baseline_samples']:>5}, "
                         f"total samples: {data['total_samples']:>5}, "
                         f"last price: {data['last_price']:>10.3f}, "
                         f"zscore: {data['zscore']:>6.2f}")

    def get_symbol_data(self, symbol: str) -> SymbolData:
        return self.symbol_data.get(symbol, None)

    def update_price_trends(self, symbol: str, current_price: float, action: str):
        trend_data = self.price_trends[symbol]

        if trend_data['last_action'] == action:
            if action == 'buy':
                if trend_data['extreme_price'] is None or current_price < trend_data['extreme_price']:
                    trend_data['extreme_price'] = current_price
            elif action == 'sell':
                if trend_data['extreme_price'] is None or current_price > trend_data['extreme_price']:
                    trend_data['extreme_price'] = current_price
        else:
            trend_data['last_action'] = action
            trend_data['extreme_price'] = current_price

        if trend_data['day_high'] is None or current_price > trend_data['day_high']:
            trend_data['day_high'] = current_price
        if trend_data['day_low'] is None or current_price < trend_data['day_low']:
            trend_data['day_low'] = current_price

        logging.debug(f"Updated price trends for {symbol} with action: {action}, "
                      f"extreme_price: {trend_data['extreme_price']}, "
                      f"day_high: {trend_data['day_high']}, day_low: {trend_data['day_low']}")


