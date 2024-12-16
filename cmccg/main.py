import sys
import os
import pandas as pd
import datetime
from crypto_top import get_filtered_data, read_delisted_symbols, read_rebranded_symbols
from exchange_strategies import exchange_strategies, get_all_markets

OUTPUT_DIR = 'out'
LIMIT = 1500
TIMER = 5

# Создание выходного каталога, если он не существует
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Получение всех рынков с каждой биржи
all_markets = get_all_markets(exchange_strategies)

# Инициализация статистики по биржам
exchange_stats = {exchange: 0 for exchange in exchange_strategies.keys()}
market_samples = {exchange: [] for exchange in exchange_strategies.keys()}
processed_symbols = set()  # Множество для отслеживания обработанных символов

# Чтение делистнутых символов из файла
delisted_symbols = read_delisted_symbols('delisted.properties')

# Чтение информации о ребрендинге символов из файла
rebranding_symbols = read_rebranded_symbols('rebranded.properties')

def get_exchange_for_symbol(symbol, all_markets, rebranded_symbols):
    """
    Определяет биржу, на которой торгуется символ, и возвращает тип пары,
    учитывая ребрендинг тикеров.

    Аргументы:
        symbol (str): Символ криптовалюты.
        all_markets (dict): Словарь с рынками для каждой биржи.
        rebranded_symbols (dict): Словарь с ребрендингами тикеров.

    Возвращает:
        tuple: Название биржи и тип пары или ('NOTLISTED', None).
    """
    if symbol in processed_symbols:
        return 'PROCESSED', None  # Пропустить уже обработанный символ

    for exchange_name, markets in all_markets.items():
        # Пропустить делистнутые символы
        if exchange_name in delisted_symbols and symbol in delisted_symbols[exchange_name]:
            continue
        # Проверяем ребрендинг для текущей биржи
        if exchange_name in rebranded_symbols and symbol in rebranded_symbols[exchange_name]:
            symbol = rebranded_symbols[exchange_name][symbol]

        strategy = exchange_strategies[exchange_name]
        pair = strategy.first_symbol_listed(symbol, markets)
        if pair:
            processed_symbols.add(symbol)  # Добавить символ в множество обработанных символов
            return exchange_name, pair
    return 'NOTLISTED', None

def put(s='', n=1, timer_interval=TIMER):
    """
    Возвращает функцию для записи строк и таймкодов в файлы.

    Аргументы:
        s (str): Начальная строка для записи.
        n (int): Номер файла.
        timer_interval (int): Интервал времени для таймкодов.

    Возвращает:
        function: Функция для записи строк и таймкодов.
    """
    directory = 'out'
    os.makedirs(directory, exist_ok=True)
    file = open(f"{directory}/A_{n}.txt", 'a')
    yt = open(f"{directory}/YT_{n}.txt", 'a')
    time = 1

    def inner(s):
        nonlocal time
        file.write(s + '\n')
        timecode = str(datetime.timedelta(seconds=time))
        yt.write(f'{timecode} {s} \n')
        time += timer_interval

    return inner

def generate_file_name(base_name, index):
    """
    Генерирует имя файла.

    Аргументы:
        base_name (str): Базовое имя файла.
        index (int): Индекс файла.

    Возвращает:
        str: Сгенерированное имя файла.
    """
    return os.path.join(OUTPUT_DIR, f"{base_name}_{index}.txt")

def save_not_listed(df):
    """
    Сохраняет символы, которые не торгуются на биржах, в отдельный файл.

    Аргументы:
        df (pd.DataFrame): Данные для сохранения.
    """
    not_listed_df = df[df['exchange'] == 'NOTLISTED'][['symbol', 'name', 'rank']]
    if not not_listed_df.empty:
        not_listed_file = generate_file_name('NOTLISTED', 'all')
        not_listed_df.to_csv(not_listed_file, index=False, header=True)
        print(f"Результаты с NOTLISTED сохранены в файл {not_listed_file}")

def save_chunks(df, chunk_size):
    """
    Разбивает данные на части и сохраняет их в файлы.

    Аргументы:
        df (pd.DataFrame): Данные для сохранения.
        chunk_size (int): Размер части.
    """
    total_rows = len(df)
    num_chunks = total_rows // chunk_size
    remainder = total_rows % chunk_size

    for idx in range(num_chunks):
        chunk = df.iloc[idx * chunk_size : (idx + 1) * chunk_size]
        writer = put(n=chunk_size * (idx + 1))
        for _, row in chunk.iterrows():
            pair = row['pair'].replace('-', '').replace('_', '')
            writer(f"{row['exchange']}:{pair}")

        update_exchange_stats(chunk)

    if remainder > 0:
        last_chunk = df.iloc[num_chunks * chunk_size :]
        writer = put(n=chunk_size * num_chunks + remainder)
        for _, row in last_chunk.iterrows():
            pair = row['pair'].replace('-', '').replace('_', '')
            writer(f"{row['exchange']}:{pair}")

        update_exchange_stats(last_chunk)

def update_exchange_stats(df):
    """
    Обновляет статистику по биржам.

    Аргументы:
        df (pd.DataFrame): Данные для обновления статистики.
    """
    for _, row in df.iterrows():
        exchange = row['exchange']
        if exchange in exchange_stats:
            exchange_stats[exchange] += 1
            if len(market_samples[exchange]) < 5:
                market_samples[exchange].append(row['pair'])

def save_to_file(limit=LIMIT):
    """
    Основная функция для получения данных и сохранения их в файлы.

    Аргументы:
        limit (int): Лимит на количество символов.
    """
    CHUNK_SIZE = 120
    filtered_df = get_filtered_data(limit)

    # Чтение ребрендингов
    rebranded_symbols = read_rebranded_symbols('rebranded.properties')

    # Применение ребрендинга
    filtered_df['exchange'], filtered_df['pair'] = zip(*filtered_df['symbol'].apply(
        get_exchange_for_symbol, args=(all_markets, rebranded_symbols)))

    filtered_df = filtered_df[filtered_df['exchange'] != 'PROCESSED']  # Исключить обработанные символы
    save_not_listed(filtered_df)
    filtered_df = filtered_df[filtered_df['exchange'] != 'NOTLISTED']

    save_chunks(filtered_df, CHUNK_SIZE)

    print_exchange_stats()

def print_exchange_stats():
    """
    Печатает статистику по биржам.
    """
    print("\nСтатистика по биржам:")
    total_assets = 0
    for exchange, count in exchange_stats.items():
        samples = ", ".join(market_samples[exchange])
        print(f"{exchange}: {count} активов [{samples}]")
        total_assets += count
    print(f"Контрольная сумма активов: {total_assets}")

if __name__ == "__main__":
    limit = LIMIT
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            print(f"Invalid limit parameter, using default value of {LIMIT}.")

    save_to_file(limit)
