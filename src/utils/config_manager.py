# src/utils/config_manager.py

import os
import json
import time
import logging
from pathlib import Path
from threading import Thread

class ConfigManager:
    def __init__(self, config_file='config/config.json'):
        self.config_file = Path(__file__).parent.parent.parent / config_file
        self.config = self.load_config()
        self.last_modified = os.path.getmtime(self.config_file)
        self.watch_thread = Thread(target=self.watch_config, daemon=True)
        self.watch_thread.start()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Configuration file not found: {self.config_file}")
            return {}
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from config file: {e}")
            return {}
        except Exception as e:
            logging.error(f"Unexpected error loading config: {e}")
            return {}

    def get(self, key, default=None):
        return self.config.get(key, default)

    def update(self, key, value):
        """
        Update a configuration key with a new value.

        Args:
            key (str): The configuration key to update.
            value (Any): The new value to assign to the key.
        """
        self.config[key] = value
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logging.info(f"Configuration updated: {key} = {value}")
            self.last_modified = os.path.getmtime(self.config_file)
        except Exception as e:
            logging.error(f"Failed to update configuration: {e}")

    def watch_config(self):
        while True:
            time.sleep(1)  # Check every second
            try:
                current_mtime = os.path.getmtime(self.config_file)
                if current_mtime > self.last_modified:
                    old_config = self.config.copy()
                    self.config = self.load_config()
                    self.last_modified = current_mtime
                    self.log_changes(old_config, self.config)
            except FileNotFoundError:
                logging.error(f"Configuration file not found: {self.config_file}")
            except Exception as e:
                logging.error(f"Error watching config file: {e}", exc_info=True)

    def log_changes(self, old_config, new_config):
        for key in new_config:
            if key in old_config and old_config[key] != new_config[key]:
                if key == 'sigma_thresh':
                    logging.info(f"Sigma threshold updated: {old_config[key]} -> {new_config[key]}")
                elif key == 'zscore_trend_thresh':
                    logging.info(f"Z-score trend threshold updated: {old_config[key]} -> {new_config[key]}")
                elif key == 'lambda_multiplier':
                    logging.info(f"Lambda multiplier updated: {old_config[key]} -> {new_config[key]}")
                else:
                    logging.info(f"Configuration updated: {key}")

# Instantiate a single ConfigManager instance to be used across the application
config_manager = ConfigManager()
