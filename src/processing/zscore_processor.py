import numpy as np
import pandas as pd
from typing import Dict
from scipy.stats import zscore
from scipy.signal import find_peaks
from statsmodels.tsa.filters.hp_filter import hpfilter
from ..core.data_processor import DataProcessor
from ..core.symbol_data import SymbolData
from ..utils.config_manager import config_manager

class ZScoreProcessor(DataProcessor):
    def __init__(self):
        self.config_manager = config_manager

    def process(self, data: SymbolData, new_price: float) -> Dict:
        if data.baseline_std == 0:
            zscore_value = np.nan
        else:
            zscore_value = (new_price - data.baseline_mean) / data.baseline_std

        sigma_thresh = self.config_manager.get('sigma_thresh', 30.0)
        zscore_trend_thresh = self.config_manager.get('zscore_trend_thresh', 2.0)

        result = {
            "zscore": zscore_value,
            "alert": abs(zscore_value) > sigma_thresh if not np.isnan(zscore_value) else False,
            "latest_price": new_price
        }

        if abs(result['zscore']) > sigma_thresh:
            processed_data = self.process_data(data, new_price)
            result.update({
                "symbol": data.symbol,
                "num_samples": len(data.full_prices),
                "lambda": processed_data['lambda'],
                "action": processed_data['action'],
                "price": processed_data['price'],
                "current_price": new_price,
                "samples_ago": processed_data['samples_ago'],
                "zscore_trend": processed_data['zscore_trend'][-1] if processed_data['zscore_trend'] is not None else None,
            })

            # Add zscore_trend_alert to the result
            result["zscore_trend_alert"] = abs(result['zscore_trend']) > zscore_trend_thresh if result['zscore_trend'] is not None else False

        return result

    def process_data(self, data: SymbolData, new_price: float):
        prices = np.append(data.full_prices, new_price)
        num_samples = len(prices)
        
        lambda_multipliers = self.config_manager.get('lambda_multiplier', {'1Min': 12, '1Day': 0.0436})
        lambda_multiplier = lambda_multipliers.get(data.timeframe, 12)
        
        lamb = round(lambda_multiplier * num_samples)
        
        df = pd.DataFrame({'c': prices})
        df['velocity'] = df['c'].diff()
        df['zscore_velocity'] = zscore(df['velocity'].dropna(), nan_policy='omit')
        df['trend'], df['cycle'] = hpfilter(df['c'], lamb=lamb)
        df['zscore_trend'] = zscore(df['trend'].dropna(), nan_policy='omit')
        
        cycle = df['cycle'].to_numpy()
        peaks, _ = find_peaks(cycle)
        troughs, _ = find_peaks(-cycle)
        df['peaks'] = pd.Series(df.iloc[peaks]['c'].values, index=df.iloc[peaks].index)
        df['troughs'] = pd.Series(df.iloc[troughs]['c'].values, index=df.iloc[troughs].index)
        
        last_action = self.get_last_action(df)
        
        return {
            'lambda': lamb,
            'zscore_trend': df['zscore_trend'].values if not df['zscore_trend'].empty else None,
            'action': last_action['type'],
            'price': last_action['price'],
            'samples_ago': last_action['samples_ago']
        }

    def get_last_action(self, data):
        peaks = data['peaks'].dropna()
        troughs = data['troughs'].dropna()
        
        if troughs.empty and peaks.empty:
            return {"type": "None", "price": 0.0, "samples_ago": 'N/A'}
        
        if not peaks.empty and (troughs.empty or peaks.index[-1] > troughs.index[-1]):
            last_action = peaks.index[-1]
            action_type = 'Sell'
        else:
            last_action = troughs.index[-1]
            action_type = 'Buy'
        
        action_price = data.loc[last_action, 'c']
        samples_ago = len(data) - data.index.get_loc(last_action) - 1
        
        return {"type": action_type, "price": action_price, "samples_ago": samples_ago}
