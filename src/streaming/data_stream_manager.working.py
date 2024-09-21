# src/streaming/data_stream_manager.py

import logging
from typing import Set, Callable
from ..core.config import config
from ..utils.config_manager import config_manager  # Correctly import the instance
from ..processing.zscore_processor import ZScoreProcessor
from .stream_processor import StreamProcessor
from ..data.symbol_data_manager import SymbolDataManager
from ..data.historical_data_fetcher import HistoricalDataFetcher

class DataStreamManager:
    def __init__(
        self,
        symbols: Set[str],
        processor: ZScoreProcessor,
        ndays: int,
        calculate_start_date: Callable,
        test_mode: bool = False,
        days_ago: int = 1,
        stream_type: str = 'trades'
    ):
        """
        Initializes the DataStreamManager with necessary components.

        Args:
            symbols (Set[str]): A set of stock symbols to monitor.
            processor (ZScoreProcessor): Processor for handling z-score calculations.
            ndays (int): Number of days for historical data.
            calculate_start_date (Callable): Function to calculate the start date for data fetching.
            test_mode (bool, optional): Flag to enable test mode. Defaults to False.
            days_ago (int, optional): Number of days ago to start data fetching. Defaults to 1.
            stream_type (str, optional): Type of stream ('trades', 'bars', etc.). Defaults to 'trades'.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fetcher = HistoricalDataFetcher(config.API_KEY, config.API_SECRET, config.BASE_URL)
        self.symbol_manager = SymbolDataManager(symbols, ndays, calculate_start_date, test_mode, days_ago)
        
        # Instantiate StreamProcessor without 'paper_url'
        self.stream_processor = StreamProcessor(
            processor=processor,
            stream_type=stream_type,
            config_manager=config_manager  # Ensure config_manager is correctly imported
        )
        
        self.test_mode = test_mode
        self.stream_type = stream_type

    async def run(self):
        """
        Runs the DataStreamManager to initialize data and start streaming or simulation.
        """
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

    async def cleanup(self):
        """
        Cleans up resources when the DataStreamManager is shutting down.
        """
        self.logger.info("Cleaning up resources...")
        # Implement any necessary cleanup operations here, such as closing WebSocket connections
        # Example:
        # if self.stream_processor.websocket:
        #     await self.stream_processor.websocket.close()
        pass

