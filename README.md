# Stock Anomaly Detector

## Overview

The Stock Anomaly Detector is a command-line tool designed to identify and analyze unusual patterns in stock price data. It processes real-time stock information and applies statistical methods to detect anomalies, providing investors and analysts with timely insights for decision-making.

## Features

- Real-time processing of stock data
- Customizable anomaly detection parameters
- Support for multiple stock symbols
- Flexible data input options (file-based or direct symbol input)
- Configurable historical data range
- Optional test mode for simulation using historical data

## Requirements

- Python 3.8+
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/stock-anomaly-detector.git
   cd stock-anomaly-detector
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   - APCA_API_KEY_ID: Your Alpaca API key
   - APCA_API_SECRET_KEY: Your Alpaca API secret key

## Usage

The program is executed from the command line with various parameters:

```
python main.py [OPTIONS]
```

### Options:

- `--file FILE`: File containing stock symbols (one per line)
- `--symbols SYMBOLS`: Comma-separated list of stock symbols
- `--ndays NDAYS`: Number of days of historical data to fetch (default: 2)
- `--debug`: Enable debug logging
- `--test`: Run in test mode using historical data
- `--days_ago DAYS_AGO`: Number of days ago to start simulation in test mode (default: 1)
- `--stream_type {trades,bars}`: Choose data to subscribe to (default: trades)
- `--sigma_thresh SIGMA_THRESH`: Sigma threshold for alerts
- `--zscore_trend_thresh ZSCORE_TREND_THRESH`: Z-score trend threshold for alerts

### Examples:

1. Process data for symbols in a file with default settings:
   ```
   python main.py --file symbols.txt
   ```

2. Process specific symbols with custom thresholds:
   ```
   python main.py --symbols AAPL,GOOGL,MSFT --sigma_thresh 3.0 --zscore_trend_thresh 2.5
   ```

3. Run in test mode with historical data:
   ```
   python main.py --file symbols.txt --test --days_ago 5 --ndays 30
   ```

## Output

The program outputs log messages to the console and a log file (`app.log`). Alerts for detected anomalies are displayed in the following format:

```
ALERT: SYMBOL | Price: PRICE | Z-Score: ZSCORE | Act: ACTION | Samples Ago: SAMPLES | Z-Trend: ZTREND | Lambda: LAMBDA | Ext. Price: EXT_PRICE
```

## Configuration

Adjust the `config/config.json` file to modify default parameters:

- `sigma_thresh`: Default sigma threshold for alerts
- `zscore_trend_thresh`: Default Z-score trend threshold for alerts
- `lambda_multiplier`: Multipliers for different timeframes
- `timeframe`: Default timeframe for data analysis

## Troubleshooting

- Ensure your Alpaca API credentials are correctly set in the environment variables.
- Check the `app.log` file for detailed error messages and debugging information.
- Verify that the input file (if used) contains valid stock symbols, one per line.

## Notes

- The program uses the Alpaca API for real-time and historical stock data. Ensure you have a valid Alpaca account and API credentials.
- Performance may vary based on the number of symbols processed and the chosen parameters.
- Use the `--debug` option for verbose logging during troubleshooting.

For further assistance or to report issues, please contact Joe_O.
