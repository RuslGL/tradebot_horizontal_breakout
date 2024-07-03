import asyncio
import plotly.graph_objects as go

from utils import klines_to_df
from api import get_klines_asc


def find_support_resistance(symbol, limit, debug=False):
    # получаем дневные свечи за период равный WINDOW
    klines_store = asyncio.run(get_klines_asc(symbol, 'D', limit+1))
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
    return resistance, support


# ####### Для визуализации и дебага ########
def show_support_resistance(window):
    resistance, support, data = find_support_resistance('BTCUSDT', window, True)

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

if __name__ == '__main__':
    show_support_resistance(30)