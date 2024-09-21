# src/streaming/stream_processor.py

import logging
import math
import asyncio
import websockets
import json
from ..utils.config_manager import config_manager

class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON Encoder to handle non-serializable types like sets.
    Converts sets to lists for JSON serialization.
    """
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)

class StreamProcessor:
    def __init__(self, processor, stream_type: str, config_manager):
        self.processor = processor
        self.stream_type = stream_type
        self.config_manager = config_manager
        self.proxy_url = "ws://localhost:8765"
        self.websocket = None
        self.symbol_manager = None

    async def simulate(self):
        logging.info("Simulation mode is not yet implemented.")
        pass

    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.proxy_url)
            logging.info(f"Connected to proxy server at {self.proxy_url}")
        except Exception as e:
            logging.error(f"Failed to connect to proxy server: {e}")
            raise

    async def subscribe(self):
        if not self.websocket:
            logging.error("WebSocket connection is not established.")
            return

        subscribe_message = {
            "action": "subscribe",
            self.stream_type: list(self.symbol_manager.symbols)
        }

        try:
            await self.websocket.send(json.dumps(subscribe_message, cls=CustomJSONEncoder))
            logging.info(f"Sent subscription message: {subscribe_message}")
        except Exception as e:
            logging.error(f"Failed to send subscription message: {e}")
            raise

    async def handle_message(self, message: str):
        try:
            data = json.loads(message)
            message_type = data.get('T')
            symbol = data.get('S')

            if message_type and symbol:
                if self.stream_type == 'trades' and message_type == 't':
                    await self.process_trade(data)
                elif self.stream_type == 'bars' and message_type == 'b':
                    await self.process_bar(data)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logging.error(f"Error handling message: {e}", exc_info=True)

    async def process_trade(self, trade_data):
        symbol = trade_data['S']
        price = trade_data['p']
        await self.process_data(symbol, price)

    async def process_bar(self, bar_data):
        symbol = bar_data['S']
        close = bar_data['c']
        high = bar_data.get('h')
        low = bar_data.get('l')
        await self.process_data(symbol, close, high, low)

    async def process_data(self, symbol, price, high=None, low=None):
        if symbol in self.symbol_manager.symbol_data:
            last_price = self.symbol_manager.last_processed_price.get(symbol)
            logging.debug(f"Last processed price for {symbol}: {last_price}")

            if last_price is None or price != last_price:
                symbol_data = self.symbol_manager.symbol_data[symbol]

                if symbol_data.baseline_std == 0:
                    logging.warning(f"Baseline std is zero for {symbol}. Removing from processing list.")
                    del self.symbol_manager.symbol_data[symbol]
                    self.symbol_manager.symbols.remove(symbol)
                    return

                baseline_zscore = (price - symbol_data.baseline_mean) / symbol_data.baseline_std
                logging.debug(f"Calculated zscore for {symbol}: {baseline_zscore}")

                sigma_thresh = self.config_manager.get('sigma_thresh', 30.0)
                zscore_trend_thresh = self.config_manager.get('zscore_trend_thresh', 2.0)

                if abs(baseline_zscore) > sigma_thresh:
                    symbol_data.append_price(price)
                    result = self.processor.process(symbol_data, price)
                    logging.debug(f"Processing result for {symbol}: {result}")

                    if result['action'] is None or any(math.isnan(value) for value in result.values() if isinstance(value, (int, float))):
                        logging.warning(f"Invalid result for {symbol}. Removing from processing list.")
                        del self.symbol_manager.symbol_data[symbol]
                        self.symbol_manager.symbols.remove(symbol)
                        return

                    if abs(result['zscore']) > sigma_thresh and abs(result.get('zscore_trend', 0)) > zscore_trend_thresh:
                        current_action = result['action']
                        current_trend = self.symbol_manager.price_trends[symbol]

                        if high is not None and low is not None:
                            if current_trend.get('day_high') is None:
                                current_trend['day_high'] = high
                            else:
                                current_trend['day_high'] = max(current_trend['day_high'], high)

                            if current_trend.get('day_low') is None:
                                current_trend['day_low'] = low
                            else:
                                current_trend['day_low'] = min(current_trend['day_low'], low)
                        else:
                            if current_trend.get('day_high') is None:
                                current_trend['day_high'] = price
                            else:
                                current_trend['day_high'] = max(current_trend['day_high'], price)

                            if current_trend.get('day_low') is None:
                                current_trend['day_low'] = price
                            else:
                                current_trend['day_low'] = min(current_trend['day_low'], price)

                        if current_trend.get('last_action') == current_action:
                            if current_action == 'Buy':
                                current_trend['extreme_price'] = min(current_trend.get('extreme_price', price), price)
                            elif current_action == 'Sell':
                                current_trend['extreme_price'] = max(current_trend.get('extreme_price', price), price)
                        else:
                            if current_trend.get('extreme_price') is not None:
                                logging.info(
                                    f"TREND CHANGE: {symbol:<6} | Prev Act: {current_trend['last_action']:<4} | "
                                    f"New Act: {current_action:<4} | Ext. Price: {current_trend['extreme_price']:>8.3f} | "
                                    f"Day High: {current_trend['day_high']:>8.3f} | Day Low: {current_trend['day_low']:>8.3f}"
                                )

                            current_trend['extreme_price'] = price
                            current_trend['last_action'] = current_action

                        logging.info(
                            f"ALERT: {symbol:<6} | Price: {result['latest_price']:>8.3f} | "
                            f"Z-Score: {result['zscore']:>5.1f} | Act: {current_action:<4} | "
                            f"Samples Ago: {result.get('samples_ago', 'N/A'):>4} | "
                            f"Z-Trend: {result.get('zscore_trend', 'N/A'):>5.1f} | "
                            f"Lambda: {result['lambda']:>8} | Ext. Price: {current_trend['extreme_price']:>8.3f}"
                        )

                    self.symbol_manager.last_processed_price[symbol] = price

    async def listen(self):
        try:
            async for message in self.websocket:
                await self.handle_message(message)
        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"WebSocket connection closed: {e}. Attempting to reconnect...")
            await self.reconnect()
        except Exception as e:
            logging.error(f"Error while listening to WebSocket: {e}", exc_info=True)
            await self.reconnect()

    async def reconnect(self):
        try:
            await self.connect()
            await self.subscribe()
            await self.listen()
        except Exception as e:
            logging.error(f"Reconnection failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            await self.reconnect()

    async def run(self, symbol_manager):
        self.symbol_manager = symbol_manager
        await self.connect()
        await self.subscribe()
        await self.listen()
