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

    symbol = 'BTCUSDT'
    quantity = 0.003

    #await post_bybit_signed(endpoint, API_KEY, SECRET_KEY, **kwargs)
    await post_bybit_signed('place_order', API_KEY, SECRET_KEY, orderType='Limit',
                            category='linear', symbol=symbol,
                            side='Buy', qty=quantity)



if __name__ == '__main__':
    asyncio.run(main())

# SERVICE FUNCTIONS

# def round_price_by_coin_params(price, coin_params):
#     """
#     Returns rounded quantity
#     requires:
#     1. price to round
#     2. coin params in format given by get_instruments_info
#     {'symbol': 'BTCUSDT', 'base_coin_prec': '0.000001',
#     'price_tick': '0.01', 'minOrderQty': '0.000048'}
#     """
#     price_tick = float(coin_params.get('price_tick'))
#     precision = int(math.log10(1 / price_tick))
#     factor = 10 ** precision
#     return int(price * factor) / factor
#
#
# def round_quantity_by_coin_params(quantity, coin_params):
#     """
#     Returns rounded quantity
#     requires:
#     1. price to round
#     2. coin params in format given by get_instruments_info
#     {'symbol': 'BTCUSDT', 'base_coin_prec': '0.000001',
#     'price_tick': '0.01', 'minOrderQty': '0.000048'}
#     """
#     quote_coin_prec = float(coin_params.get('base_coin_prec'))
#     precision = int(math.log10(1 / quote_coin_prec))
#     factor = 10 ** precision
#     return int(quantity * factor) / factor
#
#
# # ASYNCIO MARKET REQUESTS
# async def get_klines_asc(category='linear', symbol='BTCUSDT',
#                          interval=60, limit=10):
#     """
#     Returns last klines from market
#     On default:
#     category = 'spot', also available spot,linear,inverse
#     symbol = 'BTCUSDT'
#     interval = 60, , also available  # 1,3,5,15,30,60,120,240,360,720,D,M,W
#     limit = 10  # , also available[1, 1000]
#     """
#     url = MAIN_URL + ENDPOINTS_BYBIT.get('get_kline')
#
#     params = {
#         'category': category,
#         'symbol': symbol,
#         'interval': interval,
#         'limit': limit,
#     }
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url, params=params) as response:
#             return await response.json()
#
#
# async def get_instruments_info_asc(category='linear'):
#     """
#     Returns info on pair(s) from market
#     On default:
#     category = 'spot', also linear, inverse, option, spot
#     symbol = 'BTCUSDT', not mandatory
#     Retunrs result as
#     {'symbol': 'BTCUSDT', 'base_coin_prec': '0.000001',
#     'price_tick': '0.01', 'minOrderQty': '0.000048'}
#     """
#     url = MAIN_URL + ENDPOINTS_BYBIT.get('instruments-info')
#
#     params = {
#         'category': category
#     }
#
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url, params=params) as response:
#             data = await response.json()
#
#     result = data.get('result').get('list')[0]
#     # print(result)
#     dic = {
#         'symbol': result.get('symbol'),
#         'base_coin_prec': float(
#             result.get('lotSizeFilter').get('basePrecision')),
#         'price_tick': float(result.get('priceFilter').get('tickSize')),
#         'minOrderQty': float(result.get('lotSizeFilter').get('minOrderQty')),
#     }
#     return dic
#
#
# async def get_pair_price_asc(symbol='BTCUSDT', category='spot'):
#     """
#     Returns last price on given pair
#     By default spot market
#     """
#     url = MAIN_URL + ENDPOINTS_BYBIT.get('get_tickers')
#
#     params = {
#         'category': category,
#         'symbol': symbol
#     }
#
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url, params=params) as response:
#             data = await response.json()
#
#     result = data.get(
#         'result').get('list')[0].get('lastPrice')
#     return result
#
#
# # SIMPLE ACCOUNT REQUESTS
#
# def get_wallet_balance(coin=None, accountType='UNIFIED'):
#     """
#     Returns wallet balane
#     If coin provided in agruments, returns balancve on given coin
#     """
#     url = MAIN_URL + ENDPOINTS_BYBIT.get('wallet-balance')
#
#     timestamp = str(int(time.time() * 1000))
#
#     headers = {
#         'X-BAPI-API-KEY': API_KEY,
#         'X-BAPI-TIMESTAMP': timestamp,
#         'X-BAPI-RECV-WINDOW': '5000'
#     }
#
#     params = {
#         'accountType': accountType,
#     }
#
#     if coin:
#         params['coin'] = coin
#
#     headers['X-BAPI-SIGN'] = gen_signature_get(params, timestamp)
#     response = requests.get(url, headers=headers, params=params)
#
#     return response.json().get('result').get(
#         'list')[0].get('coin')[0].get('walletBalance')
#
#
#
# ###### TOTAL BUDGET REQUIRED
# def get_pair_budget(base_coin, symbol):
#     """
#     Returns total budget on given pair
#     Request: get_pair_budget(base_coin='BTC', symbol='BTCUSDT')
#     Result: {'total_budget': 993.523590982, 'budget_usdt': '991.38534782',
#     'quantity_base_coin': '0.00004873', 'price_base_coin': '43879.4'}
#     """
#
#     quantity_base_coin = get_wallet_balance(coin=base_coin)
#     budget_usdt = get_wallet_balance(coin='USDT')
#     price_base_coin = get_pair_price(symbol=symbol)
#
#     total_budget = float(budget_usdt) + float(
#         quantity_base_coin) * float(price_base_coin)
#     budget_usdt
#
#     result = {
#         'total_budget': total_budget,
#         'budget_usdt': float(budget_usdt),
#         'quantity_base_coin': float(quantity_base_coin),
#         'price_base_coin': float(price_base_coin),
#     }
#
#     return result
#
#
# # ASYNCIO ACCOUNT REQUESTS
#
#
# async def get_wallet_balance_asc(coin=None, accountType='UNIFIED'):
#     """
#     Returns wallet balane
#     If coin provided in agruments, returns balancve on given coin
#     """
#     url = MAIN_URL + ENDPOINTS_BYBIT.get('wallet-balance')
#
#     timestamp = str(int(time.time() * 1000))
#
#     headers = {
#         'X-BAPI-API-KEY': API_KEY,
#         'X-BAPI-TIMESTAMP': timestamp,
#         'X-BAPI-RECV-WINDOW': '5000'
#     }
#
#     params = {
#         'accountType': accountType,
#     }
#
#     if coin:
#         params['coin'] = coin
#
#     headers['X-BAPI-SIGN'] = gen_signature_get(params, timestamp)
#
#     async with aiohttp.ClientSession() as session:
#         async with session.get(
#                 url, headers=headers, params=params) as response:
#             data = await response.json()
#
#     return data.get('result').get(
#         'list')[0].get('coin')[0].get('walletBalance')
#
#
# # async def get_pair_budget_asc(base_coin, symbol):
# #     """
# #     Returns total budget on given pair
# #     Request: get_pair_budget(base_coin='BTC', symbol='BTCUSDT')
# #     Result: {'total_budget': 993.523590982, 'budget_usdt': '991.38534782',
# #     'quantity_base_coin': '0.00004873', 'price_base_coin': '43879.4'}
# #     """
# #
# #     my_requests = [asyncio.create_task(get_wallet_balance_asc(coin=base_coin)),
# #                    asyncio.create_task(get_wallet_balance_asc(coin='USDT')),
# #                    asyncio.create_task(get_pair_price_asc(symbol=symbol)), ]
# #     result = await asyncio.gather(*my_requests)
# #
# #     quantity_base_coin = result[0]
# #     budget_usdt = result[1]
# #     price_base_coin = result[2]
# #
# #     total_budget = float(budget_usdt) + float(
# #         quantity_base_coin) * float(price_base_coin)
# #     budget_usdt
# #
# #     result = {
# #         'total_budget': total_budget,
# #         'budget_usdt': float(budget_usdt),
# #         'quantity_base_coin': float(quantity_base_coin),
# #         'price_base_coin': float(price_base_coin),
# #     }
# #
# #     return result
#
#
# # ##########################################
#
# # ASYNCIO TRADE REQUESTS
#
#
# async def place_market_order_sell_linear(symbol, quantity):
#     result = await post_bybit_signed('place_order', orderType='Market',
#                                      category='linear', symbol=symbol,
#                                      side='Sell', qty=quantity,
#                                      marketUnit='baseCoin')
#     return result
#
#
# async def place_market_order_buy_linear(symbol, quantity):
#     result = await post_bybit_signed('place_order', orderType='Market',
#                                      category='linear', symbol=symbol,
#                                      side='Buy', qty=quantity,
#                                      marketUnit='baseCoin')
#     return result
#
#
# async def place_conditional_order_sell_linear(symbol, quantity, triggerPrice, price):
#     result = await post_bybit_signed('place_order', orderType='Limit',
#                                      category='linear', symbol=symbol,
#                                      side='Sell', qty=quantity,
#                                      price=price, triggerPrice=triggerPrice,
#                                      orderFilter='StopOrder')
#     return result
#
#
# async def place_conditional_order_buy_linear(symbol, quantity,
#                                            triggerPrice, price):
#     result = await post_bybit_signed('place_order', orderType='Limit',
#                                      category='spot', symbol=symbol,
#                                      side='Buy', qty=quantity,
#                                      price=price, triggerPrice=triggerPrice,
#                                      orderFilter='StopOrder')
#     return result
#
#
# async def cancel_linear_order(order_id, symbol):
#     """
#     Cancells spot order by order_id and pair symbol
#     """
#     result = await post_bybit_signed('cancel_order', category='linear',
#                                      symbol=symbol, orderId=order_id)
#     return result
#
#
# async def get_open_linear_orders(symbol):
#     """
#     Returns list of open spot ordersId
#     Input - pair symbol
#     """
#
#     url = MAIN_URL + ENDPOINTS_BYBIT.get('open_orders')
#
#     timestamp = str(int(time.time() * 1000))
#
#     headers = {
#         'X-BAPI-API-KEY': API_KEY,
#         'X-BAPI-TIMESTAMP': timestamp,
#         'X-BAPI-RECV-WINDOW': '5000'
#     }
#
#     params = {
#         'symbol': symbol,
#         'category': 'linear'
#     }
#
#     headers['X-BAPI-SIGN'] = gen_signature_get(params, timestamp)
#
#     async with aiohttp.ClientSession() as session:
#         async with session.get(
#                 url, headers=headers, params=params) as response:
#             data = await response.json()
#         data = await response.json()
#         data = data.get('result').get('list')
#         result = [element.get('orderId') for element in data]
#     return result
