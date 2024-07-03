import pandas as pd


def klines_to_df(raw_klines_store, symbol=None):
    """
    Gets list of lists as input
    Returns sorted dataframe
    """
    df_klines_store = pd.DataFrame(
        raw_klines_store, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    ).astype(float)

    df_klines_store['time'] = pd.to_datetime(df_klines_store['time'], unit='ms')

    df_klines_store = df_klines_store.set_index('time')
    df_klines_store = df_klines_store.sort_index(ascending=True)

    if symbol is not None:
        df_klines_store['symbol'] = symbol

    return df_klines_store
