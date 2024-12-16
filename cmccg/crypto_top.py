import requests
import pandas as pd

def read_symbols_from_file(file_path):
    """
    Читает символы из файла и возвращает их в виде списка.

    Аргументы:
        file_path (str): Путь к файлу.

    Возвращает:
        list: Список символов.
    """
    with open(file_path, 'r') as file:
        symbols = file.read().splitlines()
    return [symbol.strip().upper() for symbol in symbols]

def get_coingecko_top(limit=720):  # С запасом 20%
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit,
        "page": 1
    }
    response = requests.get(url, params=params)
    data = response.json()
    df = pd.DataFrame(data, columns=["id", "symbol", "name", "market_cap_rank"])
    df.rename(columns={"market_cap_rank": "rank"}, inplace=True)
    df['symbol'] = df['symbol'].str.upper()  # Приведение символов к верхнему регистру
    return df

def get_coinmarketcap_top(limit=720):  # С запасом 20%
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    headers = {
        "X-CMC_PRO_API_KEY": "2985dea3-7f02-4069-b30c-b38829b39c5d"
    }
    params = {
        "start": "1",
        "limit": str(limit),
        "convert": "USD"
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()["data"]
    df = pd.DataFrame(data, columns=["id", "symbol", "name", "cmc_rank"])
    df.rename(columns={"cmc_rank": "rank"}, inplace=True)
    df['symbol'] = df['symbol'].str.upper()  # Приведение символов к верхнему регистру
    return df

def get_filtered_data(limit=720):  # С запасом 20%
    # Получаем данные с CoinGecko
    coingecko_df = get_coingecko_top(limit)
    # Получаем данные с CoinMarketCap
    coinmarketcap_df = get_coinmarketcap_top(limit)

    # Объединение данных
    merged_df = pd.concat([coingecko_df, coinmarketcap_df], ignore_index=True)

    # Чтение исключенных символов и стейблкойнов из файлов
    excluded_symbols = read_symbols_from_file('excludet.properties')
    stables = read_symbols_from_file('stables.properties')
    all_excluded = set(excluded_symbols + stables)

    # Удаление дубликатов по 'symbol', сохранение ранжирования по минимальному значению ранга
    merged_df = merged_df.sort_values(by="rank").drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)

    # Исключение стейблкойнов и обернутых токенов
    filtered_df = merged_df[~merged_df['symbol'].isin(all_excluded)].reset_index(drop=True)

    return filtered_df
