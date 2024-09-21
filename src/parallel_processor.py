# src/parallel_processor.py

import multiprocessing as mp
from typing import List, Dict
import numpy as np
from src.processing.zscore_processor import ZScoreProcessor
from src.core.symbol_data import SymbolData

class ParallelProcessor:
    def __init__(self, num_processes: int, zscore_processor: ZScoreProcessor):
        self.num_processes = min(num_processes, mp.cpu_count())
        self.zscore_processor = zscore_processor
        self.pool = mp.Pool(processes=self.num_processes)

    def process_symbols(self, symbol_data_list: List[SymbolData], new_prices: Dict[str, float]) -> List[Dict]:
        # Split the work into chunks
        chunks = np.array_split(symbol_data_list, self.num_processes)
        
        # Prepare arguments for each process
        args = [(chunk, new_prices, self.zscore_processor) for chunk in chunks]
        
        # Use pool.map to distribute the work
        results = self.pool.map(self._process_chunk, args)
        
        # Flatten the results
        return [item for sublist in results for item in sublist]

    @staticmethod
    def _process_chunk(args):
        chunk, new_prices, processor = args
        results = []
        for symbol_data in chunk:
            if symbol_data.symbol in new_prices:
                result = processor.process(symbol_data, new_prices[symbol_data.symbol])
                results.append(result)
        return results

    def shutdown(self):
        self.pool.close()
        self.pool.join()
