import asyncio
import os
import pandas as pd
import json
from api import get_lin_perp_info_asc, get_klines_asc
from utils import klines_to_df, split_list
from dotenv import load_dotenv
from db.futures_klines import FutureKlinesOperations
from ws import SocketBybit
from multiprocessing import Process

load_dotenv()

# Global settings
IF_TEST = 1
WINDOW = 30
KLINE_INTERVAL = 1
RISK_LIMIT = 0.8
TP_RATE = 0.01
START_TRADE = False
KLINE_PERIOD = 5

START_BUDGET = 0
TRADING_PAIRS = []
PAIRS_QUANTITY = 0
usdt_budget = 0

MAIN_TEST = 'https://api-testnet.bybit.com'
MAIN_REAL = 'https://api.bybit.com'

# Set API keys based on environment
if IF_TEST:
    print('THIS IS TEST ONLY')
    MAIN_URL = MAIN_TEST
    API_KEY = str(os.getenv('test_bybit_api_key'))
    SECRET_KEY = str(os.getenv('test_bybit_secret_key'))
else:
    print('BE CAREFUL REAL MARKET IN FORCE!!!')
    API_KEY = str(os.getenv('bybit_api_key'))
    SECRET_KEY = str(os.getenv('bybit_secret_key'))
    MAIN_URL = MAIN_REAL

async def get_variables():
    tasks = [
        asyncio.create_task(get_lin_perp_info_asc()),
        # Add more tasks as needed
    ]
    results = await asyncio.gather(*tasks)
    return results

async def get_initial_klines(pairs, kline_period, kline_interval):
    tasks = [
        asyncio.create_task(get_klines_asc(symbol, kline_interval, kline_period + 1)) for symbol in pairs
    ]
    results = await asyncio.gather(*tasks)
    return results

async def custom_on_message(ws, msg):
    db_futures = FutureKlinesOperations()
    try:
        data = json.loads(msg.data)
        if 'data' in data and data.get('data')[0].get('confirm') is True:
            ohlc = data.get('data')[0]
            kline = (
                ohlc['start'],
                str(data.get('topic').split('.')[-1]),
                str(ohlc['interval']),
                float(ohlc['open']),
                float(ohlc['close']),
                float(ohlc['high']),
                float(ohlc['low']),
                float(ohlc['volume'])
            )
            try:
                await db_futures.upsert_kline(*kline)
            except Exception as e:
                print(f"Failed to insert data {kline}: {e}")
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON in custom handler: {e}")

async def run_socket(topic, url):
    socket = SocketBybit(url, topic, on_message=custom_on_message)
    await socket.connect()

async def main():
    try:
        # Fetch initial variables
        result = await get_variables()
        trade_list = result[0][1]
        pairs_params = result[0][2]

        # Fetch initial klines
        #### !!!!!!! REDUCED QUANTITY
        symbols = trade_list   # [:50]
        result_klines = await get_initial_klines(symbols, KLINE_PERIOD, KLINE_INTERVAL)
        result_klines = [element.get('result') for element in result_klines if element.get('retMsg') == 'OK']

        # Prepare klines data
        klines_data = pd.DataFrame()
        for element in result_klines:
            df = klines_to_df(element.get('list')[1:], element.get('symbol'))
            klines_data = pd.concat([klines_data, df])

        # Filter out symbols with zero volume
        zero_volume_counts = klines_data[klines_data['volume'] == 0].groupby('symbol').size()
        symbols_to_remove = zero_volume_counts[zero_volume_counts > 1].index
        klines_join_data = klines_data[~klines_data['symbol'].isin(symbols_to_remove)]

        TRADING_PAIRS = klines_join_data['symbol'].unique()
        # PAIRS_QUANTITY = len(TRADING_PAIRS)

        # Initialize database
        db_futures = FutureKlinesOperations()
        await db_futures.create_table()

        url_futures = 'wss://stream.bybit.com/v5/public/linear'
        topics = [ f'kline.{KLINE_INTERVAL}.{pair}' for pair in TRADING_PAIRS ]
        print(f"Total number of topics: {len(topics)}")

        ws_amount = 4
        topics_groups = split_list(topics, ws_amount)

        # Start WebSockets
        processes = []
        for topic_group in topics_groups:
            p = Process(target=run_socket_sync, args=(topic_group, url_futures))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

    except Exception as e:
        print(f"An error occurred in main: {e}")

def run_socket_sync(topics, url):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_socket(topics, url))

if __name__ == '__main__':
    asyncio.run(main())


