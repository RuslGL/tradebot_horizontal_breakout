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


def split_list(data, size):
    """
    Gets list of data + size
    Returns list of list with len = size
    Last batch contains remainder of division on size

    """
    part_size = len(data) // size
    remainder = len(data) % size
    result = [ [] for _ in range(size) ]

    pointer = 0
    for i in range(size):
        result[i] = data[pointer : pointer + part_size]
        pointer += part_size

    if remainder > 0:
        result[size -1].extend(data[-remainder:])
    return result


def prepare_ws_topics_futures(base_for_topics, ws_amount, pairs_to_trade):
    """
    Requires base for ws topic needed
    And ws processes quantity
    returns list of prepared topics
    """

    ws_prepared_pairs = split_list(pairs_to_trade, ws_amount)
    for numb, element in enumerate(ws_prepared_pairs):
        ws_prepared_pairs[numb] = [(base_for_topics + position) for position in element]

    return ws_prepared_pairs
