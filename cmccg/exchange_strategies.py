import requests
from abc import ABC, abstractmethod
from cache_utils import load_from_cache, save_to_cache

# Функция для генерации форматов символов с различными разделителями
def generate_symbol_formats(symbol, delimiters):
    """
    Генерирует форматы символов с различными разделителями.

    Аргументы:
        symbol (str): Символ криптовалюты.
        delimiters (list): Список разделителей.

    Возвращает:
        list: Список форматов символов.
    """
    base_pairs = ['USDT', 'USDC', 'USD', 'EUR', 'BTC', 'ETH', 'SOL']
    formats = []
    for delimiter in delimiters:
        formats.extend([f'{symbol}{delimiter}{pair}' for pair in base_pairs])
    return formats

class ExchangeStrategy(ABC):
    """
    Абстрактный базовый класс для стратегий получения данных с различных бирж.

    Атрибуты:
        name (str): Название биржи.
        cache_key (str): Ключ для кэширования данных.
        api_url (str): URL API для получения данных.
        parse_params (dict): Параметры для парсинга данных.
        symbol_formats (list): Форматы символов для проверки.
    """
    def __init__(self, name, cache_key, api_url, parse_params, delimiters):
        self.name = name
        self.cache_key = cache_key
        self.api_url = api_url
        self.parse_params = parse_params
        self.symbol_formats = generate_symbol_formats('{symbol}', delimiters)
        self.previous_markets = set()

    def fetch_markets(self):
        """
        Получает данные с API и парсит их.

        Возвращает:
            list: Список торговых пар.
        """
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            data = response.json()
            return self.parse_markets(data)
        except requests.RequestException as e:
            print(f"Error fetching markets from {self.name}: {e}")
            return []

    def parse_markets(self, data):
        """
        Парсит данные с API.

        Аргументы:
            data (dict): Данные, полученные с API.

        Возвращает:
            list: Список торговых пар.
        """
        markets = data
        try:
            for key in self.parse_params['keys']:
                markets = markets[key]
            if isinstance(markets, dict):  # Если markets это словарь (например, как в случае с Kraken)
                markets = markets.values()
            return [market[self.parse_params['symbol_key']].upper() for market in markets]
        except (KeyError, TypeError) as e:
            print(f"Error parsing markets from {self.name}: {e}")
            print(f"Data received: {data}")
            return []

    def get_markets(self):
        """
        Получает список торговых пар, используя кэширование.

        Возвращает:
            list: Список новых торговых пар.
        """
        cached_data = load_from_cache(self.cache_key)
        if cached_data is not None:
            cached_markets = set(cached_data)
        else:
            cached_markets = set()

        current_markets = set(self.fetch_markets())
        new_markets = current_markets - cached_markets
        self.previous_markets = current_markets

        save_to_cache(self.cache_key, list(current_markets))
        return list(new_markets)

    def first_symbol_listed(self, symbol, markets):
        """
        Проверяет, котируется ли символ на бирже и возвращает первую найденную пару.

        Аргументы:
            symbol (str): Символ криптовалюты.
            markets (list): Список торговых пар.

        Возвращает:
            str: Первая найденная пара или None, если символ не котируется.
        """
        for fmt in self.symbol_formats:
            formatted_symbol = fmt.format(symbol=symbol)
            if formatted_symbol in markets:
                return formatted_symbol
        return None

# Конфигурация для каждой биржи
exchange_configs = {
    'BINANCE': {
        'api_url': 'https://api.binance.com/api/v3/exchangeInfo',
        'parse_params': {
            'keys': ['symbols'],
            'symbol_key': 'symbol'
        },
        'delimiters': ['']
    },
    'GATEIO': {
        'api_url': 'https://api.gateio.ws/api/v4/spot/currency_pairs',
        'parse_params': {
            'keys': [],
            'symbol_key': 'id'
        },
        'delimiters': ['_']
    },
    'MEXC': {
        'api_url': 'https://www.mexc.com/open/api/v2/market/symbols',
        'parse_params': {
            'keys': ['data'],
            'symbol_key': 'symbol'
        },
        'delimiters': ['_']
    },
    'BITGET': {
        'api_url': 'https://api.bitget.com/api/spot/v1/public/products',
        'parse_params': {
            'keys': ['data'],
            'symbol_key': 'symbolName'
        },
        'delimiters': ['']
    },
    'KUCOIN': {
        'api_url': 'https://api.kucoin.com/api/v1/market/allTickers',
        'parse_params': {
            'keys': ['data', 'ticker'],
            'symbol_key': 'symbol'
        },
        'delimiters': ['-']
    },
    'BYBIT': {
        'api_url': 'https://api.bybit.com/v5/market/tickers?category=spot',
        'parse_params': {
            'keys': ['result', 'list'],
            'symbol_key': 'symbol'
        },
        'delimiters': ['']
    },
    'OKX': {
        'api_url': 'https://www.okx.com/api/v5/public/instruments?instType=SPOT',
        'parse_params': {
            'keys': ['data'],
            'symbol_key': 'instId'
        },
        'delimiters': ['-']
    },
    'HTX': {
        'api_url': 'https://api.htx.com/v1/common/symbols',
        'parse_params': {
            'keys': ['data'],
            'symbol_key': 'symbol'
        },
        'delimiters': ['']
    },
    'KRAKEN': {
        'api_url': 'https://api.kraken.com/0/public/AssetPairs',
        'parse_params': {
            'keys': ['result'],
            'symbol_key': 'altname'
        },
        'delimiters': ['']
    }
}

# Словарь для хранения стратегий
exchange_strategies = {
    name: ExchangeStrategy(
        name,
        f"{name.lower()}_markets",
        config['api_url'],
        config['parse_params'],
        config['delimiters']
    ) for name, config in exchange_configs.items()
}

# Функция для получения всех рынков с каждой биржи один раз
def get_all_markets(strategies):
    """
    Получает все рынки с каждой биржи.

    Аргументы:
        strategies (dict): Словарь стратегий для каждой биржи.

    Возвращает:
        dict: Словарь с рынками для каждой биржи.
    """
    all_markets = {}
    for name, strategy in strategies.items():
        markets = strategy.get_markets()
        all_markets[name] = markets
        # Печать информации о первых пяти торговых парах
        sample_markets = ', '.join(markets[:5])
        print(f"Fetched {len(markets)} markets from {name} [{sample_markets}]")
    return all_markets
