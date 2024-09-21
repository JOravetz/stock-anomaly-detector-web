import logging
import aiohttp
import asyncio
import pandas as pd
from typing import Set
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class HistoricalDataFetcher:
    def __init__(self, api_key: str, api_secret: str, base_url: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url

    async def fetch_historical_data(self, symbols: Set[str], start_date: datetime, end_date: datetime):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_symbol_data(session, symbol, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')) for symbol in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [result for result in results if not isinstance(result, Exception)]

    @retry(stop=stop_after_attempt(5), 
           wait=wait_exponential(multiplier=1, min=4, max=60),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)))
    async def fetch_symbol_data(self, session: aiohttp.ClientSession, symbol: str, start_date: str, end_date: str):
        url = f"{self.base_url}/v2/stocks/bars"
        headers = {"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.api_secret}
        params = {
            "symbols": symbol,
            "timeframe": "1Min",
            "start": start_date,
            "end": end_date,
            "limit": 10000,
            "adjustment": "split",
            "feed": "sip"
        }
        try:
            async with session.get(url, headers=headers, params=params, timeout=30) as response:
                if response.status == 429:
                    logging.warning(f"Rate limit exceeded for {symbol}. Retrying...")
                    await asyncio.sleep(60)  # Wait for 1 minute before retrying
                    raise aiohttp.ClientError("Rate limit exceeded")
                response.raise_for_status()
                data = await response.json()

                if "bars" in data and symbol in data["bars"]:
                    bar_list = data["bars"][symbol]
                    df = pd.DataFrame(bar_list)
                    df['timestamp'] = pd.to_datetime(df['t']).dt.tz_convert('UTC')
                    df.set_index('timestamp', inplace=True)
                    df = df[['c']]
                    df.columns = ['close']
                    logging.info(f"Fetched {len(df)} bars for {symbol}")
                    return symbol, df
                else:
                    logging.warning(f"No data received for {symbol}")
                    return symbol, pd.DataFrame()
        except Exception as e:
            logging.error(f"Error fetching data for symbol {symbol}: {str(e)}")
            raise


