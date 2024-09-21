# src/streaming/data_stream_manager.py

import logging
from typing import Set, Callable
from ..core.config import config
from ..utils.config_manager import config_manager
from ..processing.zscore_processor import ZScoreProcessor
from .stream_processor import StreamProcessor
from ..data.symbol_data_manager import SymbolDataManager
from ..data.historical_data_fetcher import HistoricalDataFetcher
from ..parallel_processor import ParallelProcessor

class DataStreamManager:
    def __init__(
        self,
        symbols: Set[str],
        processor: ZScoreProcessor,
        parallel_processor: ParallelProcessor,
        ndays: int,
        calculate_start_date: Callable,
        test_mode: bool = False,
        days_ago: int = 1,
        stream_type: str = 'trades'
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fetcher = HistoricalDataFetcher(config.API_KEY, config.API_SECRET, config.BASE_URL)
        self.symbol_manager = SymbolDataManager(symbols, ndays, calculate_start_date, test_mode, days_ago)
        self.processor = processor
        self.parallel_processor = parallel_processor
        
        self.stream_processor = StreamProcessor(
            processor=self.process_data,
            stream_type=stream_type,
            config_manager=config_manager
        )
        
        self.test_mode = test_mode
        self.stream_type = stream_type

    async def run(self):
        try:
            self.logger.info("Initializing data...")
            await self.symbol_manager.initialize_data(self.fetcher)
            self.logger.info("Data initialization complete.")
            
            if self.test_mode:
                self.logger.info("Starting simulation...")
                await self.stream_processor.simulate()
                self.logger.info("Simulation complete.")
            else:
                self.logger.info(f"Starting real-time {self.stream_type} stream...")
                await self.stream_processor.run(self.symbol_manager)
        except Exception as e:
            self.logger.error(f"An error occurred during execution: {str(e)}", exc_info=True)
            raise
        finally:
            await self.cleanup()

    def process_data(self, symbol_data_list, new_prices):
        if self.parallel_processor:
            return self.parallel_processor.process_symbols(symbol_data_list, new_prices)
        else:
            results = []
            for symbol_data in symbol_data_list:
                if symbol_data.symbol in new_prices:
                    result = self.processor.process(symbol_data, new_prices[symbol_data.symbol])
                    results.append(result)
            return results

    async def cleanup(self):
        self.logger.info("Cleaning up resources...")
        if self.stream_processor.websocket:
            await self.stream_processor.websocket.close()
        if self.parallel_processor:
            self.parallel_processor.shutdown()
