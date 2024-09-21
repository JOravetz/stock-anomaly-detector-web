from abc import ABC, abstractmethod
from typing import Dict
from .symbol_data import SymbolData

class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: SymbolData, new_price: float) -> Dict:
        pass


