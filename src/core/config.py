import os
from dataclasses import dataclass

@dataclass
class Config:
    API_KEY: str = os.getenv('APCA_API_KEY_ID')
    API_SECRET: str = os.getenv('APCA_API_SECRET_KEY')
    BASE_URL: str = 'https://data.alpaca.markets'
    PAPER_URL: str = 'https://paper-api.alpaca.markets'

config = Config()


