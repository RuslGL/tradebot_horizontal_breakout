import asyncio
import os
import time
import pandas as pd

from api import get_lin_perp_info_asc, get_klines_asc
from utils import klines_to_df

import aiohttp

from dotenv import load_dotenv

load_dotenv()


# ###  SETTINGS ####

# TO GET VIA TELEGRAM
IF_TEST = 1
WINDOW = 30
KLINE_INTERVAL = 5  # Kline interval. 1,3,5,15,30,60,120,240,360,720,D,M,W
RISK_LIMIT = 0.8  # максимальная просадка начального депозита
TP_RATE = 0.01
START_TRADE = False
KLINE_PERIOD = 5    # amount of kline to calculate average volume

# TO REQUEST THROUGH API
START_BUDGET = 0
TRADING_PAIRS = []
PAIRS_QUANTITY = 0
usdt_budget = 0


# ###  END OF SETTINGS ####


MAIN_TEST = 'https://api-testnet.bybit.com'
MAIN_REAl = 'https://api.bybit.com'


if IF_TEST:
    print('THIS IS TEST ONLY')

    MAIN_URL = MAIN_TEST
    API_KEY = str(os.getenv('test_bybit_api_key'))
    SECRET_KEY = str(os.getenv('test_bybit_secret_key'))

else:
    print('BE CAREFUL REAL MARKET IN FORCE!!!')

    API_KEY = str(os.getenv('bybit_api_key'))
    SECRET_KEY = str(os.getenv('bybit_secret_key'))
    MAIN_URL = MAIN_REAl


# GETTING INITIAL DATA THROUGH API
async def get_variables():
    tasks = [
        asyncio.create_task(get_lin_perp_info_asc()),
        # get initial vallet balance
        # get usdt vallet balance

    ]

    results = await asyncio.gather(*tasks)
    return results

while True:
    try:
        result = asyncio.run(get_variables())
        break  # Если запрос успешен, выйти из цикла
    except Exception as e:
        print(f"An error occurred during getting variables: {e}. Retrying in 5 seconds...")
        time.sleep(5)

# pairs_quantity = result[0][0]
trade_list = result[0][1]
pairs_params = result[0][2]


# GETTING INITIAL KLINES THROUGH API

async def get_initial_klines(pairs, kline_period, kline_interval):
    tasks = [
        asyncio.create_task(get_klines_asc(symbol, kline_interval, kline_period+1)) for symbol in pairs
    ]

    results = await asyncio.gather(*tasks)
    return results

# !!!!!!!!!! REDUCED LIST
symbols = trade_list[:5]
print(symbols)

while True:
    try:
        result = asyncio.run(get_initial_klines(symbols, KLINE_PERIOD, KLINE_INTERVAL))
        break  # Если запрос успешен, выйти из цикла
    except Exception as e:
        print(f"An error occurred: {e}. Retrying in 5 seconds...")
        time.sleep(5)

result = [ element.get('result') for element in result if element.get('retMsg') == 'OK']
# print(*result, sep='\n\n')

klines_join_data = pd.DataFrame()
for element in result:
    # убираем последнюю свечу, так как она не закрыта
    df = klines_to_df(element.get('list')[1:], element.get('symbol'))
    klines_join_data = pd.concat([klines_join_data, df])

# часть символов не имеет трейдов в свечах, поэтому для дальнейшей работы будем использовать только пары с данными в свечах
zero_volume_counts = klines_join_data[klines_join_data['volume'] == 0].groupby('symbol').size()
symbols_to_remove = zero_volume_counts[zero_volume_counts > 1].index
klines_join_data = klines_join_data[~klines_join_data['symbol'].isin(symbols_to_remove)]

TRADING_PAIRS = klines_join_data['symbol'].unique()
PAIRS_QUANTITY = len(TRADING_PAIRS)

print(klines_join_data)



