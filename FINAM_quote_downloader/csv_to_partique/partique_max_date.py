from time import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import dask.dataframe as dd

start_time = time()  # Время начала запуска скрипта
# Путь к файлам Parquet
path = r'c:\data_quote\parquet_finam_RTS_tick'

# Чтение всех файлов Parquet в Dask DataFrame
df = dd.read_parquet(path, engine='pyarrow')
# print(df)

# Преобразование колонки datetime в тип datetime
df['datetime'] = dd.to_datetime(df['datetime'])

# Получение максимальной даты
max_date = df['datetime'].max().compute().date()

print("Максимальная дата:", max_date)
print(f'Скрипт выполнен за {(time() - start_time):.2f} с')

# Более быстрая версия --------------------------------------------------------
start_time = time()  # Время начала запуска скрипта
# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Переменная для хранения максимальной даты
max_datetime = None

# Проход по всем партициям
for year_folder in parquet_folder.glob('year=*'):
    for month_folder in year_folder.glob('month=*'):
        # Читаем все файлы в текущей партиции
        for parquet_file in month_folder.glob('*.parquet'):
            # Загружаем только нужный столбец
            df = pd.read_parquet(parquet_file, columns=['datetime'])  

            # Определяем максимальную дату в текущем файле
            current_max = df['datetime'].max()

            # Обновляем глобальное значение максимальной даты
            if max_datetime is None or (current_max is not pd.NaT and current_max > max_datetime):
                max_datetime = current_max

print(f"Максимальная дата: {max_datetime}")
print(f'Скрипт выполнен за {(time() - start_time):.2f} с')

# Еще быстрей -----------------------------------------------------------------
start_time = time()  # Время начала запуска скрипта
# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Переменная для хранения максимальной даты
max_datetime = None

# Получение списка годов и месяцев из структуры партиций
for year_folder in parquet_folder.glob('year=*'):
    year = int(year_folder.name.split('=')[1])
    for month_folder in year_folder.glob('month=*'):
        month = int(month_folder.name.split('=')[1])

        # Читаем только первую строку каждого файла в текущей партиции
        for parquet_file in month_folder.glob('*.parquet'):
            df = pd.read_parquet(parquet_file, columns=['datetime'], engine='pyarrow')

            # Определяем максимальную дату в текущем файле
            current_max = df['datetime'].max()

            # Обновляем глобальное значение максимальной даты
            if max_datetime is None or (current_max is not pd.NaT and current_max > max_datetime):
                max_datetime = current_max

print(f"Максимальная дата: {max_datetime}")
print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
