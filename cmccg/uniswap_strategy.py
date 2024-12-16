import requests
from cache_utils import load_from_cache, save_to_cache

class UniswapStrategy:
    def __init__(self):
        self.name = 'UNISWAP'
        self.cache_key = 'uniswap_markets'
        self.chains = ['ethereum', 'bsc']
        self.previous_markets = set()

    def get_api_url(self, chain):
        return f'https://open-api.openocean.finance/v4/{chain}/tokenList'

    def parse_markets(self, data):
        return [market['symbol'].upper() for market in data['data']]

    def is_symbol_listed(self, symbol, markets):
        return symbol in markets

    def fetch_markets(self, chain):
        url = self.get_api_url(chain)
        response = requests.get(url)
        try:
            response.raise_for_status()
            data = response.json()
            return self.parse_markets(data)
        except requests.RequestException as e:
            print(f"Error fetching markets from Uniswap ({chain}): {e}")
            return []

    def get_markets(self):
        cached_data = load_from_cache(self.cache_key)
        if cached_data is not None:
            cached_markets = set(cached_data)
        else:
            cached_markets = set()

        all_markets = set()
        for chain in self.chains:
            all_markets.update(self.fetch_markets(chain))

        new_markets = all_markets - cached_markets
        self.previous_markets = all_markets

        save_to_cache(self.cache_key, list(all_markets))
        return list(new_markets)
