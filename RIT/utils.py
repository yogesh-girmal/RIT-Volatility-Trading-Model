import signal
import requests
from time import sleep
import pandas as pd
import numpy as np

        
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

async def get_tick(session):
    async with session.get('http://localhost:9999/v1/case') as resp:
        if resp.status == 200:
            case = await resp.json()
            return case['tick']
        
async def get_period(session):
    async with session.get('http://localhost:9999/v1/case') as resp:
        if resp.status == 200:
            case = await resp.json()
            return case['period']

async def get_status(session):
    async with session.get('http://localhost:9999/v1/case') as resp:
        if resp.status == 200:
            case = await resp.json()
            return case['status']

# code that gets the securities via json
async def get_s(session):
    async with session.get('http://localhost:9999/v1/securities') as price_act:
        if price_act.status == 200:
            prices = await price_act.json()
            return [price['last'] for price in prices], [price['ticker'] for price in prices]

async def get_news(session):
    async with session.get('http://localhost:9999/v1/news') as news_book:
        if news_book.status == 200:
            news_data = await news_book.json()
            news_tick = news_data['tick']
            news_ticker = news_data['ticker']

async def get_news(session):
    async with session.get('http://localhost:9999/v1/news') as resp:
        if resp.status == 200:
            news_data = await resp.json()
            return news_data
        else:
            print("Failed to fetch news data")
            return []