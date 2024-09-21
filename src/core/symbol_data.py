from dataclasses import dataclass
import numpy as np
import pandas as pd

@dataclass
class SymbolData:
    symbol: str
    historical_data: pd.DataFrame
    baseline_prices: np.ndarray = None
    baseline_mean: float = 0.0
    baseline_std: float = 1.0
    full_prices: np.ndarray = None
    timeframe: str = "1Min"

    def __post_init__(self):
        if self.full_prices is None:
            self.full_prices = self.historical_data['close'].values

    def append_price(self, price: float):
        self.full_prices = np.append(self.full_prices, price)


