import asyncio
import plotly.graph_objects as go

from utils import klines_to_df
from api import get_klines_asc
import time


async def find_resistance_support(symbol, limit, debug=False):
    # получаем дневные свечи за период равный WINDOW
    klines_store = await get_klines_asc(symbol, 'D', limit + 1)
    klines_list = klines_store.get('result').get('list')

    # преобразуем в датафрейм и убираем текущий день
    data = klines_to_df(klines_list).iloc[:-1]
    data['resistance'] = data[['open', 'close']].max(axis=1)
    data['support'] = data[['open', 'close']].min(axis=1)
    resistance = data['resistance'].max()
    support = data['support'].min()
    if debug:
        print(data)
        return resistance, support, data
    return [resistance, support]

async def joined_resistance_support(trading_pairs, window, debug=False):
    """
    Returns dict with time of creation
    On each pair (key = symbol) returns (resistance, support)
    {'time': 1720072835.04777, '10000000AIDOGEUSDT': (0.005384, 0.002922)}
    """
    start_time = time.time()
    levels = {
        'time': start_time,
    }

    # Initial attempt to get resistance and support levels
    tasks = [asyncio.create_task(find_resistance_support(pair, window)) for pair in trading_pairs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Collect pairs that need retrying
    pairs_to_retry = []
    for pair, result in zip(trading_pairs, results):
        if isinstance(result, Exception):
            pairs_to_retry.append(pair)
        else:
            levels[pair] = result

    # Wait 2 seconds before retrying
    await asyncio.sleep(2)

    # Perform retries sequentially for pairs that failed
    pairs_to_retry_again = []
    for pair in pairs_to_retry:
        try:
            result = await find_resistance_support(pair, window)
            levels[pair] = result
        except Exception:
            pairs_to_retry_again.append(pair)

    # Perform final retries sequentially for pairs that still failed
    failed_pairs = []
    for pair in pairs_to_retry_again:
        try:
            result = await find_resistance_support(pair, window)
            levels[pair] = result
        except Exception:
            failed_pairs.append(pair)

    if failed_pairs:
        print(f"Не удалось получить уровни для: {', '.join(failed_pairs)}")

    end_time = time.time()
    if debug:
        print(levels)
        print(f"Время выполнения функции: {end_time - start_time:.2f} секунд")

    return levels


# ####### Для визуализации и дебага ########
async def show_resistance_support(window):
    resistance, support, data = await find_resistance_support('BTCUSDT', window, True)

    fig = go.Figure()

    # Добавление свечей
    fig.add_trace(go.Candlestick(
        x=data.index,
        open=data['open'],
        high=data['high'],
        low=data['low'],
        close=data['close'],
        name='Days Klines',  # Имя для легенды свечей
    ))

    # Добавление линии сопротивления (resistance)
    fig.add_shape(
        type='line',
        x0=data.index[0],
        y0=resistance,
        x1=data.index[-1],
        y1=resistance,
        line=dict(
            color='blue',
            width=2,
            dash='dashdot',
        ),
        name='Resistance'
    )

    # Добавление линии поддержки (support)
    fig.add_shape(
        type='line',
        x0=data.index[0],
        y0=support,
        x1=data.index[-1],
        y1=support,
        line=dict(
            color='black',
            width=2,
            dash='dashdot',
        ),
        name='Support'
    )

    # Добавление названий для линий в легенду
    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='lines',
        line=dict(color='blue', width=2, dash='dashdot'),
        showlegend=True,
        name='Resistance'
    ))

    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='lines',
        line=dict(color='black', width=2, dash='dashdot'),
        showlegend=True,
        name='Support'
    ))

    fig.show()

async def main():


    TRADING_PAIRS = ['GLMUSDT', 'LPTUSDT', 'MAGICUSDT', 'MNTUSDT', 'MOVRUSDT', 'MTLUSDT', 'MYRIAUSDT']
    WINDOW = 30

    print(await joined_resistance_support(TRADING_PAIRS, WINDOW, debug=True))

    #await show_resistance_support(WINDOW)


if __name__ == '__main__':
    asyncio.run(main())
