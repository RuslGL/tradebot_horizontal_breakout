import asyncio
import os
import json
import time
from datetime import datetime, timezone
from multiprocessing import Process

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from api.api_market import get_lin_perp_info_asc, get_klines_asc
from utils import split_list
from db.futures_klines import FutureKlinesOperations
from db.settings_vars import SettingsVarsOperations
from api.ws import SocketBybit
from strategy import joined_resistance_support
from api.api_private import BybitTradeClientLinear
from telegram import start_bot

load_dotenv()


async def custom_on_message(ws, msg):
    db_futures = FutureKlinesOperations()
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


async def perform_strategy(trading_pairs):

    print('Strategy performance started')
    db_futures = FutureKlinesOperations()
    await db_futures.create_table()

    db_get_vars = SettingsVarsOperations()
    await db_get_vars.create_table()

    # получаем настройки из базы, до получения необходимого минимума ждем
    while True:
        settings = {row.name: row.value for row in await db_get_vars.select_all()}
        WINDOW = int(settings.get('window'))
        KLINE_INTERVAL = settings.get('kline')  # KLINES LENGTH
        KLINE_PERIOD = int(settings.get('sma_period'))  # to calculate sma
        TP_RATE = float(settings.get('tp_rate'))
        SL_RATE = float(settings.get('sl_rate'))
        RISK_LIMIT = float(settings.get('risk_limit'))
        VOLUME_MULTIPLICATOR = int(settings.get('multiplicator'))  # to defane SMA * N

        if all(
            var is not None
            for var in [WINDOW, KLINE_INTERVAL, KLINE_PERIOD, TP_RATE, SL_RATE, RISK_LIMIT, VOLUME_MULTIPLICATOR]
        ):
            break
        print("Ожидание инициализации всех переменных пользователем...")
        await asyncio.sleep(1)  # Подождать 1 секунду перед повторной проверкой
    print("Все переменные инициализированы", settings)

    # MAIN_TEST = 'https://api-testnet.bybit.com'
    # MAIN_REAL = 'https://api.bybit.com'

    # отладочная настройка, меняется только вручную
    IF_TEST = True
    # IF_TEST = False


    if IF_TEST:
        print('THIS IS TEST ONLY')
        # MAIN_URL = MAIN_TEST
        API_KEY = str(os.getenv('test_bybit_api_key'))
        SECRET_KEY = str(os.getenv('test_bybit_secret_key'))
    else:
        print('BE CAREFUL REAL MARKET IN FORCE!!!')
        API_KEY = str(os.getenv('bybit_api_key'))
        SECRET_KEY = str(os.getenv('bybit_secret_key'))
        # MAIN_URL = MAIN_REAL


    # DB connectors, trade class instance created
    client = BybitTradeClientLinear(API_KEY, SECRET_KEY,
                                    testnet=IF_TEST, risk_limit=RISK_LIMIT,
                                    tp_rate=TP_RATE, sl_rate=SL_RATE)

    await client.initialize_start_budget()

    # получаем и сохраняем дневные уровни
    days_levels_created = datetime(2024, 1, 1, tzinfo=timezone.utc)


    # получаем исторические свечии
    print('Kline interval', KLINE_INTERVAL, 'kline period', KLINE_PERIOD)
    tasks = [
        asyncio.create_task(get_klines_asc(symbol, KLINE_INTERVAL, KLINE_PERIOD + 1)) for symbol in trading_pairs
    ]
    results = await asyncio.gather(*tasks)
    results = [element.get('result') for element in results if element.get('retMsg') == 'OK']

    def klines_to_df(klines, symbol):
        columns = ['start', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        df = pd.DataFrame(klines, columns=columns)
        df['symbol'] = symbol
        return df

    klines_df = pd.DataFrame(columns=['start', 'open', 'high', 'low', 'close', 'volume', 'symbol'])

    for element in results:
        symbol = element.get('symbol')
        klines_list = element.get('list')[1:]
        df = klines_to_df(klines_list, symbol)
        klines_df = pd.concat([klines_df, df], ignore_index=True)

    if 'turnover' in klines_df.columns:
        klines_df = klines_df.drop('turnover', axis=1)


    klines_df['start'] = pd.to_datetime(klines_df['start'], unit='ms')
    klines_df = klines_df.set_index(klines_df['start'])
    await asyncio.sleep(5)  # to not exeed amount of requests


    # MAIN CTRATEGY CYCLE

    while True:

        settings = {row.name: row.value for row in await db_get_vars.select_all()}
        tm = int(settings.get('start_trade'))
        if tm == 1:
            TRADE_MODE = True
        else:
            TRADE_MODE = False

        now_utc = datetime.now(timezone.utc)
        start_of_today_utc = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)

        if days_levels_created < start_of_today_utc:
            days_levels_created = now_utc
            days_levels = await joined_resistance_support(trading_pairs, WINDOW, debug=False)
            print('days_levels_created', now_utc)

        # получаем из бд свежие свечи - получены из сокетов
        new_klines = await db_futures.select_and_delete_all_klines()

        # если свечи получены - преобразуем их читаемый в датафрейм и присоединяем к уже имеющимся данным
        if not new_klines.empty:
            new_klines_df = pd.DataFrame(new_klines)

            new_klines_df['start'] = pd.to_datetime(new_klines_df['start'], unit='ms')
            new_klines_df = new_klines_df.set_index(new_klines_df['start'])

            klines_df = klines_df.sort_index().sort_values(by='symbol', kind='mergesort')

            klines_df = pd.concat([klines_df, new_klines_df])
            klines_df = klines_df.drop_duplicates()

            # Рассчитываем SMA
            klines_df['SMA'] = klines_df.groupby('symbol')['volume'].transform(
                lambda x: x.rolling(window=KLINE_PERIOD).mean())

            # cдвигаем SMA на одну строку вниз, чтобы текущее значение не учитывалось
            # обрезаем максимальное количество строк по каждому символу
            klines_df['SMA'] = klines_df.groupby('symbol')['SMA'].shift(1)
            klines_df = klines_df.groupby('symbol').apply(lambda group: group.iloc[-(KLINE_PERIOD * 2):]).reset_index(
                drop=True)


            grouped = klines_df.groupby('symbol')
            for symbol, group in grouped:
                # Получаем последнюю строку в группе (самую свежую)
                last_row = group.iloc[-1]

                # Проверяем сигналы -> превышение среднего объема -> пробитие уровня

                # Проверяем условие: volume > SMA * x
                if not np.isnan(last_row['SMA']) and last_row['SMA'] != 0:
                    if last_row['volume'] > last_row['SMA'] * VOLUME_MULTIPLICATOR:
                        if last_row['symbol'] in new_klines_df['symbol'].values:
                            symbol_levels = (days_levels.get(symbol))


                            # ###################### MAIN STRATEGY LOGIC ######################
                                  # ###################### START ######################
                                                # #####################

                            # пробитие верхнего уровня - прошлая свеча закрылась (новая открылась) ниже верхнего уровня
                            # а последняя свеча закрылась выше уровня, то есть произошел прокол/пробитие
                            if last_row['close'] > symbol_levels[0] and last_row['open'] < symbol_levels[0]:
                                print('Signal for long received',
                                      datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                                print(f"{symbol}, Close: {last_row['close']}, Volume: {last_row['volume']}, Latest SMA: {last_row['SMA']}")
                                print(symbol_levels)
                                if TRADE_MODE:
                                    print('Open long')
                                    try:
                                        await client.place_long(symbol)
                                    except Exception as e:
                                        print(e)
                            # пробитие нижнего уровня - прошлая свеча закрылась (новая открылась) выше нижнего уровня
                            # а последняя свеча закрылась ниже уровня, то есть произошел прокол/пробитие
                            if last_row['close'] < symbol_levels[1] and last_row['open'] > symbol_levels[1]:
                                print('Signal for short received',
                                      datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                                print(f"{symbol}, Close: {last_row['close']}, Volume: {last_row['volume']}, Latest SMA: {last_row['SMA']}, time: {last_row['start']}")
                                print(symbol_levels)
                                if TRADE_MODE:
                                    print('Open short')
                                    try:
                                        await client.place_short(symbol)
                                    except Exception as e:
                                        print(e)
                                                # #####################
                                  # ###################### END ######################
                            # ###################### MAIN STRATEGY LOGIC ######################


            # FOR DEBUG
            klines_df.to_csv('klines_join_data.csv', index=False)
            print("Updated data loaded and saved to klines_join_data.csv")

        await asyncio.sleep(1)



def start_perform_strategy(trading_pairs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(perform_strategy(trading_pairs))


#async def start_bot_async():
#    await start_bot()

def start_bot_process():
    asyncio.run(start_bot())


async def main():

    try:

        # запускаем бота
        bot_process = Process(target=start_bot_process)
        bot_process.start()
        db_get_vars = SettingsVarsOperations()
        await db_get_vars.create_table()

        # ждем установки через бота размер свечей
        while True:
            settings = {row.name: row.value for row in await db_get_vars.select_all()}
            KLINE_INTERVAL = settings.get('kline')  # KLINES LENGTH
            if KLINE_INTERVAL:
                break

            await asyncio.sleep(1)  # Подождать 1 секунду перед повторной проверкой

        # собираем список торговых пар
        find_pairs = await get_lin_perp_info_asc()
        trading_pairs = find_pairs[1]


        # рыночные данные всегда собираем на реальном рынке, трейды в зависимости от настроек IF_TEST
        url_futures = 'wss://stream.bybit.com/v5/public/linear'

        # собираем топики для сокетов
        topics = [f'kline.{KLINE_INTERVAL}.{pair}' for pair in trading_pairs]
        print(f"Total number of topics: {len(topics)}")

        ws_amount = 3
        topics_groups = split_list(topics, ws_amount)

        # запускаем сокеты
        processes = []
        for topic_group in topics_groups:
            p = Process(target=run_socket_sync, args=(topic_group, url_futures))
            processes.append(p)
            p.start()

        # запускаем стратегию
        fetch_process = Process(target=start_perform_strategy, args=(trading_pairs,))
        fetch_process.start()

        for p in processes:
            p.join()

        fetch_process.join()

    except Exception as e:
        print(f"An error occurred in main: {e}")

if __name__ == '__main__':
    asyncio.run(main())
