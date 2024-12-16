import os
import json
import time

CACHE_DIR = 'cache'
CACHE_EXPIRATION = 3600  # 1 час

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_file_path(key):
    return os.path.join(CACHE_DIR, f"{key}.json")

def is_cache_valid(file_path):
    if not os.path.exists(file_path):
        return False
    file_mtime = os.path.getmtime(file_path)
    current_time = time.time()
    return (current_time - file_mtime) < CACHE_EXPIRATION

def load_from_cache(key):
    file_path = get_cache_file_path(key)
    if is_cache_valid(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    return None

def save_to_cache(key, data):
    file_path = get_cache_file_path(key)
    with open(file_path, 'w') as file:
        json.dump(data, file)

def clear_cache():
    for filename in os.listdir(CACHE_DIR):
        file_path = os.path.join(CACHE_DIR, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")
