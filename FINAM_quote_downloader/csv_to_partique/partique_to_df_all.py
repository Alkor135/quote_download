from time import time
from pathlib import Path

import pandas as pd

# Весь partique ---------------------------------------------------------------
# start_time = time()  # Время начала запуска скрипта
# # Путь к папке с Parquet-файлами
# parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# # Переменная для хранения всех данных
# all_data = pd.DataFrame()

# # Чтение всех партиций
# for year_folder in parquet_folder.glob('year=*'):
#     for month_folder in year_folder.glob('month=*'):
#         # Чтение всех файлов в текущей партиции
#         for parquet_file in month_folder.glob('*.parquet'):
#             df = pd.read_parquet(parquet_file, engine='pyarrow')
#             all_data = pd.concat([all_data, df], ignore_index=True)

# print(f"Загружено {len(all_data)} записей.")
# print(f'Скрипт выполнен за {(time() - start_time):.2f} с')

# Весь partique с выбранными колонками ----------------------------------------
start_time = time()  # Время начала запуска скрипта
# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Переменная для хранения всех данных
all_data = pd.DataFrame()

# Чтение всех партиций
for year_folder in parquet_folder.glob('year=*'):
    for month_folder in year_folder.glob('month=*'):
        # Чтение всех файлов в текущей партиции
        for parquet_file in month_folder.glob('*.parquet'):
            df = pd.read_parquet(
                parquet_file, columns=['datetime', '<LAST>', '<VOL>'], engine='pyarrow'
                )
            all_data = pd.concat([all_data, df], ignore_index=True)

print(f"Загружено {len(all_data)} записей.")
print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
