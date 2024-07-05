import asyncio
import os
import time
import hmac
import hashlib
import json
from decimal import Decimal, ROUND_DOWN
import aiohttp
from dotenv import load_dotenv

load_dotenv()


class BybitTradeClientLinear:
    MAIN_TEST = 'https://api-testnet.bybit.com'
    MAIN_REAL = 'https://api.bybit.com'
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

    # ok
    def __init__(self, api_key, secret_key, testnet=True, risk_limit=0.8, tp_rate=0.05, sl_rate=0.03):
        self.api_key = api_key
        self.secret_key = secret_key
        self.main_url = self.MAIN_TEST if testnet else self.MAIN_REAL
        self.risk_limit = risk_limit
        self.tp_rate = tp_rate
        self.sl_rate = sl_rate
        self.start_budget = None
        if testnet:
            print('TESTNET MODE')
        else:
            print('BE CAREFULL PRODUCTION MODE')

    # start_budget will appear only after calling this function
    async def initialize_start_budget(self):
        balance = await self.get_wallet_balance(coin='USDT')
        self.start_budget = float(balance.get('totalWalletBalance'))

    @staticmethod
    def gen_signature_get(params, timestamp, api_key, secret_key):
        param_str = timestamp + api_key + '5000' + '&'.join([f'{k}={v}' for k, v in params.items()])
        return hmac.new(
            bytes(secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    @staticmethod
    def get_signature_post(data, timestamp, recv_wind, api_key, secret_key):
        query = f'{timestamp}{api_key}{recv_wind}{data}'
        return hmac.new(secret_key.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()

    @staticmethod
    async def post_data(url, data, headers):
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, headers=headers) as response:
                return await response.json()

    async def post_bybit_signed(self, endpoint, **kwargs):
        timestamp = int(time.time() * 1000)
        recv_wind = 5000
        data = json.dumps({key: str(value) for key, value in kwargs.items()})
        signature = self.get_signature_post(data, timestamp, recv_wind, self.api_key, self.secret_key)
        headers = {
            'Accept': 'application/json',
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-TIMESTAMP': str(timestamp),
            'X-BAPI-RECV-WINDOW': str(recv_wind)
        }

        url = self.main_url + self.ENDPOINTS_BYBIT[endpoint]
        return await self.post_data(url, data, headers)

    # ok
    async def get_klines_asc(self, symbol, interval, limit):
        url = self.main_url + self.ENDPOINTS_BYBIT.get('get_kline')
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': interval,
            'limit': limit,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                return await response.json()

    # ok
    async def create_market_linear_buy(self, symbol, quantity, takeProfit, stopLoss):
        return await self.post_bybit_signed('place_order',
                                            orderType='Market',
                                            category='linear',
                                            symbol=symbol,
                                            side='Buy',
                                            qty=quantity,
                                            takeProfit=takeProfit,
                                            stopLoss=stopLoss)

    # ok
    async def create_market_linear_sell(self, symbol, quantity, takeProfit='50000', stopLoss='70000'):
        return await self.post_bybit_signed('place_order',
                                            orderType='Market',
                                            category='linear',
                                            symbol=symbol,
                                            side='Sell',
                                            qty=quantity,
                                            takeProfit=takeProfit,
                                            stopLoss=stopLoss)

    @staticmethod
    def calculate_purchase_volume(sum_amount, price, min_volume, tick):
        sum_amount = Decimal(str(sum_amount))
        price = Decimal(str(price))
        min_volume = Decimal(str(min_volume))
        tick = Decimal(str(tick))

        volume = sum_amount / price
        rounded_volume = (volume // tick) * tick

        if rounded_volume < min_volume:
            return -1

        return float(rounded_volume.quantize(tick, rounding=ROUND_DOWN))

    @staticmethod
    def round_price(price, tick_size):
        price = Decimal(str(price))
        tick_size = Decimal(str(tick_size))
        return float((price // tick_size) * tick_size)

    async def get_wallet_balance(self, coin=None):
        url = self.main_url + '/v5/account/wallet-balance'
        timestamp = str(int(time.time() * 1000))
        headers = {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': '5000'
        }
        params = {'accountType': 'UNIFIED'}
        if coin:
            params['coin'] = coin
        headers['X-BAPI-SIGN'] = self.gen_signature_get(params, timestamp, self.api_key, self.secret_key)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                data = await response.json()
        return data.get('result').get('list')[0]

    async def get_pair_price_asc(self, symbol):
        url = self.main_url + '/v5/market/tickers'
        params = {
            'category': 'linear',
            'symbol': symbol
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
        return data.get('result').get('list')[0].get('lastPrice')

    ########## combined trade functions ##########
    ########## START  ###########
    async def place_long(self, pair):
        if self.start_budget is None:
            await self.initialize_start_budget()

        # refactor to task-gather
        balance = await self.get_wallet_balance(coin='USDT')
        para = await self.get_lin_perp_info_asc()
        para = para[2].get(pair)
        current_price = await self.get_pair_price_asc(pair)
        price_tick = para.get('price_tick')
        total_balance = float(balance.get('totalWalletBalance'))
        avail_usdt = float(balance.get('coin')[0].get('walletBalance'))

        quantity = self.calculate_purchase_volume(avail_usdt, current_price, para.get('minOrderQty'),
                                                  para.get('base_coin_prec'))
        fail_message = 'Not enough budget available, m.b. other positions ocupied trade balance'
        success_message = 'Position has been placed successfully'
        if quantity == -1:
            print(fail_message)
            return
        if total_balance <= self.risk_limit * self.start_budget:
            print('Budget drop to minimum, stop trading!!!')
            return

        tp_price = self.round_price((float(current_price) * (1 + self.tp_rate)), price_tick)
        sl_price = self.round_price((float(current_price) * (1 - self.sl_rate)), price_tick)

        res = await self.create_market_linear_buy(pair, quantity, tp_price, sl_price)
        if res.get('retMsg') == 'ab not enough for new order':
            print(fail_message)
        if res.get('retMsg') == 'OK':
            print(success_message)

    async def place_short(self, pair):
        if self.start_budget is None:
            await self.initialize_start_budget()

        # refactor to task-gather
        balance = await self.get_wallet_balance(coin='USDT')
        para = await self.get_lin_perp_info_asc()
        para = para[2].get(pair)
        current_price = await self.get_pair_price_asc(pair)

        price_tick = para.get('price_tick')
        total_balance = float(balance.get('totalWalletBalance'))
        avail_usdt = float(balance.get('coin')[0].get('walletBalance'))

        quantity = self.calculate_purchase_volume(avail_usdt, current_price, para.get('minOrderQty'),
                                                  para.get('base_coin_prec'))
        fail_message = 'Not enough budget available, m.b. other positions ocupied trade balance'
        success_message = 'Position has been placed successfully'
        if quantity == -1:
            print(fail_message)
            return
        if total_balance <= self.risk_limit * self.start_budget:
            print('Budget drop to minimum, stop trading!!!')
            return

        tp_price = self.round_price((float(current_price) * (1 - self.tp_rate)), price_tick)
        sl_price = self.round_price((float(current_price) * (1 + self.sl_rate)), price_tick)

        res = await self.create_market_linear_sell(pair, quantity, tp_price, sl_price)
        if res.get('retMsg') == 'ab not enough for new order':
            print(fail_message)
        else:
            print(res)
        if res.get('retMsg') == 'OK':
            print(success_message)



        ########## END  ###########
        ########## combined trade functions ##########

    async def get_lin_perp_info_asc(self):
        url = self.main_url + self.ENDPOINTS_BYBIT.get('instruments-info')
        params = {'category': 'linear'}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
        result = data.get('result').get('list')
        pairs_params = [element for element in result if
                        element.get('quoteCoin') == 'USDT' and element.get('status') == 'Trading' and element.get(
                            'contractType') == 'LinearPerpetual']
        short_params = {element.get('symbol'): {'base_coin_prec': float(element.get('lotSizeFilter').get('qtyStep')),
                                                'price_tick': float(element.get('priceFilter').get('tickSize')),
                                                'minOrderQty': float(element.get('lotSizeFilter').get('minOrderQty'))}
                        for element in pairs_params}
        futures_list = [element.get('symbol') for element in pairs_params]
        return len(futures_list), futures_list, short_params


async def main():

    api_key_test = str(os.getenv('test_bybit_api_key'))
    secret_key_test = str(os.getenv('test_bybit_secret_key'))

    symbol = 'BTCUSDT'
    interval = 1
    limit = 2
    quantity = 0.001

    unreal_high = 70000
    unreal_low = 40000

    client = BybitTradeClientLinear(api_key_test, secret_key_test,
                                    testnet=True, risk_limit=0.8,
                                    tp_rate=0.05, sl_rate=0.03)

    await client.initialize_start_budget()
    await client.place_long(symbol)
    #await client.place_short(symbol)
    #res = await client.get_klines_asc('BTCUSDT', 5, 2)
    #print(res)



if __name__ == '__main__':
    asyncio.run(main())




