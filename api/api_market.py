import asyncio
import hashlib
import hmac
import json
import math
import os
import requests
import time

import aiohttp

from dotenv import load_dotenv

load_dotenv()

API_KEY = str(os.getenv('test_01_bybit_api_key'))
SECRET_KEY = str(os.getenv('test_01_bybit_secret_key'))

MAIN_URL = 'https://api-testnet.bybit.com'

ENDPOINTS_BYBIT = {
    # trade
    'place_order': '/v5/order/create',
    'cancel_order': '/v5/order/cancel',
    'open_orders': '/v5/order/realtime',

    # market
    'server_time': '/v5/market/time',
    'get_kline': '/v5/market/kline',
    'instruments-info': '/v5/market/instruments-info',
    'get_tickers': '/v5/market/tickers',

    # account
    'wallet-balance': '/v5/account/wallet-balance'
}


# CREATE SIGNATURE AND SEND REQUESTS FUNCTIONS


def gen_signature_get(params, timestamp, API_KEY, SECRET_KEY):
    """
    Returns signature for get request
    """
    param_str = timestamp + API_KEY + '5000' + '&'.join(
        [f'{k}={v}' for k, v in params.items()])
    signature = hmac.new(
        bytes(SECRET_KEY, "utf-8"), param_str.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return signature


def get_signature_post(data, timestamp, recv_wind, API_KEY, SECRET_KEY):
    """
    Returns signature for post request
    """
    query = f'{timestamp}{API_KEY}{recv_wind}{data}'
    return hmac.new(SECRET_KEY.encode('utf-8'), query.encode('utf-8'),
                    hashlib.sha256).hexdigest()


async def post_data(url, data, headers):
    """
    Makes asincio post request (used in post_bybit_signed)
    """
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, headers=headers) as response:
            return await response.json()


async def post_bybit_signed(endpoint, API_KEY, SECRET_KEY, **kwargs):
    """
    Sends signed post requests with aiohttp
    """
    timestamp = int(time.time() * 1000)
    recv_wind = 5000
    data = json.dumps({key: str(value) for key, value in kwargs.items()})
    signature = get_signature_post(data, timestamp, recv_wind, API_KEY, SECRET_KEY)
    headers = {
        'Accept': 'application/json',
        'X-BAPI-SIGN': signature,
        'X-BAPI-API-KEY': API_KEY,
        'X-BAPI-TIMESTAMP': str(timestamp),
        'X-BAPI-RECV-WINDOW': str(recv_wind)
    }

    url = MAIN_URL + ENDPOINTS_BYBIT[endpoint]

    return await post_data(
        url,
        data,
        headers)


# GET MARKET DATA
async def get_klines_asc(symbol, interval, limit):
    """
    Returns list of last klines from market
    """
    url = MAIN_URL + ENDPOINTS_BYBIT.get('get_kline')

    params = {
        'category': 'linear',
        'symbol': symbol,
        'interval': interval,
        'limit': limit,
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            return await response.json()

async def get_lin_perp_info_asc():
    """
    Returns:
    - quantity of actual trading pairs
    - list of actual trading pairs
    - list of trade oarams on each trading pair in format
    [{'10000000AIDOGEUSDT': {'base_coin_prec': 100.0,'price_tick': 1e-06,'minOrderQty': 100.0}}]
    """
    url = 'https://api.bybit.com' + ENDPOINTS_BYBIT.get('instruments-info')

    params = {
        'category': 'linear'
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()

    result = data.get('result').get('list')

    pairs_params = [
        element for element in result if element.get('quoteCoin') == 'USDT' and element.get('status') == 'Trading' and element.get('contractType') == 'LinearPerpetual'
    ]

    short_params = {}
    for element in pairs_params:
        pass
        short_params[element.get('symbol')] = {
            'base_coin_prec': float(element.get('lotSizeFilter').get('qtyStep')),
            'price_tick': float(element.get('priceFilter').get('tickSize')),
            'minOrderQty': float(element.get('lotSizeFilter').get('minOrderQty')),
        }

    futures_list = [element.get('symbol') for element in pairs_params]

    return len(futures_list), futures_list, short_params


# ASYNCIO TRADE REQUESTS



async def main():
    RISK_LIMIT = 0.8
    TP_RATE = 0.01

    START_BUDGET = 8
    USDT_BUDGET = 8
    TOTAL_BUDGET = 8





if __name__ == '__main__':
    asyncio.run(main())

