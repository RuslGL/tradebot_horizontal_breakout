import asyncio
import os
import pandas as pd
import json
from api import get_lin_perp_info_asc, get_klines_asc
from utils import klines_to_df, split_list
from dotenv import load_dotenv
from db.futures_klines import FutureKlinesOperations
from db.settings_vars import SettingsVarsOperations
from ws import SocketBybit
from multiprocessing import Process
import schedule
import time
from datetime import datetime, timedelta
from strategy import joined_resistance_support
import pytz  # Для работы с часовыми поясами

load_dotenv()


IF_TEST = 1
WINDOW = 30  # to calculate days price levels
KLINE_INTERVAL = 1
RISK_LIMIT = 0.8
TP_RATE = 0.01
START_TRADE = False
KLINE_PERIOD = 5  # to calculate sma

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
    db_settings_vars = SettingsVarsOperations()
    try:
        data = json.loads(msg.data)

        if 'data' in data and data.get('data')[0].get('confirm') is True:
            ohlc = data.get('data')[0]
            kline = (
                ohlc['start'],
                str(data.get('topic').split('.')[-1]),
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


def run_socket_sync(topics, url):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_socket(topics, url))


async def fetch_and_update_klines(klines_join_data):
    db_futures = FutureKlinesOperations()
    db_settings_vars = SettingsVarsOperations()
    while True:
        new_klines = await db_futures.select_and_delete_all_klines()
        if not new_klines.empty:
            new_klines_df = pd.DataFrame(new_klines)
            klines_join_data = pd.concat([klines_join_data, new_klines_df])
            klines_join_data = klines_join_data.drop_duplicates()

            # Sort and keep only the last KLINE_PERIOD * 2 entries per symbol
            klines_join_data = klines_join_data.sort_values(by=['symbol', 'start'], ascending=[True, True])
            klines_join_data = klines_join_data.groupby('symbol').apply(lambda group: group.iloc[-(KLINE_PERIOD * 2):])
            klines_join_data = klines_join_data.reset_index(drop=True)

            # FOR DEBUG ONLY
            klines_join_data.to_csv('klines_join_data.csv', index=False)
            print("Updated data loaded and saved to klines_join_data.csv")
            settings = {row.name: row.value for row in await db_settings_vars.select_all()}
            days_levels = json.loads(settings['days_levels'].replace("'", '"'))
            print(days_levels.get('BTCUSDT'))
        await asyncio.sleep(10)


def start_fetch_and_update_klines_process(klines_join_data):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_and_update_klines(klines_join_data))


async def main():
    global TRADING_PAIRS
    try:
        result = await get_variables()
        trade_list = result[0][1]
        pairs_params = result[0][2]

        # Get initial klines
        symbols = trade_list
        result_klines = await get_initial_klines(symbols, KLINE_PERIOD * 2, KLINE_INTERVAL)
        result_klines = [element.get('result') for element in result_klines if element.get('retMsg') == 'OK']

        klines_data = pd.DataFrame()
        for element in result_klines:
            df = klines_to_df(element.get('list')[1:], element.get('symbol'))
            klines_data = pd.concat([klines_data, df])

        klines_data = klines_data.drop('turnover', axis=1)
        # print(klines_data.head())

        # Delete symbols with zero volume
        zero_volume_counts = klines_data[klines_data['volume'] == 0].groupby('symbol').size()
        symbols_to_remove = zero_volume_counts[zero_volume_counts > 1].index
        klines_join_data = klines_data[~klines_data['symbol'].isin(symbols_to_remove)]

        TRADING_PAIRS = klines_join_data['symbol'].unique()

        days_levels = await joined_resistance_support(TRADING_PAIRS, WINDOW, debug=False)

        db_futures = FutureKlinesOperations()
        await db_futures.create_table()

        db_settings_vars = SettingsVarsOperations()
        await db_settings_vars.create_table()
        await db_settings_vars.upsert_settings('days_levels', str(days_levels))



        url_futures = 'wss://stream.bybit.com/v5/public/linear'
        topics = [f'kline.{KLINE_INTERVAL}.{pair}' for pair in TRADING_PAIRS]
        print(f"Total number of topics: {len(topics)}")

        ws_amount = 4
        topics_groups = split_list(topics, ws_amount)

        # Start WebSockets
        processes = []
        for topic_group in topics_groups:
            p = Process(target=run_socket_sync, args=(topic_group, url_futures))
            processes.append(p)
            p.start()

        # Start strategy
        fetch_process = Process(target=start_fetch_and_update_klines_process, args=(klines_join_data,))
        fetch_process.start()

        for p in processes:
            p.join()

        fetch_process.join()
    except Exception as e:
        print(f"An error occurred in main: {e}")


if __name__ == '__main__':
    asyncio.run(main())