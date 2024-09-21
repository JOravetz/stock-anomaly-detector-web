# src/streaming/stream_processor.py

import logging
import math
import asyncio
import websockets
import json
from ..utils.config_manager import config_manager

class CustomJSONEncoder(json.JSONEncoder):
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
        await self.process_data({symbol: price})

    async def process_bar(self, bar_data):
        symbol = bar_data['S']
        close = bar_data['c']
        await self.process_data({symbol: close})

    async def process_data(self, new_prices):
        symbol_data_list = [self.symbol_manager.symbol_data[symbol] for symbol in new_prices if symbol in self.symbol_manager.symbol_data]
        
        results = self.processor(symbol_data_list, new_prices)

        for result in results:
            symbol = result['symbol']
            zscore = result['zscore']
            zscore_trend = result.get('zscore_trend')
            
            sigma_thresh = self.config_manager.get('sigma_thresh', 30.0)
            zscore_trend_thresh = self.config_manager.get('zscore_trend_thresh', 2.0)

            if abs(zscore) > sigma_thresh and (zscore_trend is None or abs(zscore_trend) > zscore_trend_thresh):
                self.log_alert(result)

            self.symbol_manager.last_processed_price[symbol] = result['latest_price']

    def log_alert(self, result):
        symbol = result['symbol']
        current_trend = self.symbol_manager.price_trends[symbol]
        
        if current_trend.get('last_action') != result['action']:
            if current_trend.get('extreme_price') is not None:
                logging.info(
                    f"TREND CHANGE: {symbol:<6} | Prev Act: {current_trend['last_action']:<4} | "
                    f"New Act: {result['action']:<4} | Ext. Price: {current_trend['extreme_price']:>8.3f} | "
                    f"Day High: {current_trend['day_high']:>8.3f} | Day Low: {current_trend['day_low']:>8.3f}"
                )

            current_trend['extreme_price'] = result['latest_price']
            current_trend['last_action'] = result['action']

        logging.info(
            f"ALERT: {symbol:<6} | Price: {result['latest_price']:>8.3f} | "
            f"Z-Score: {result['zscore']:>5.1f} | Act: {result['action']:<4} | "
            f"Samples Ago: {result.get('samples_ago', 'N/A'):>4} | "
            f"Z-Trend: {result.get('zscore_trend', 'N/A'):>5.1f} | "
            f"Lambda: {result['lambda']:>8} | Ext. Price: {current_trend['extreme_price']:>8.3f}"
        )

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
