import pandas as pd
from pathlib import Path

# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Укажите диапазон дат (в формате 'YYYY-MM-DD')
start_date = '2023-01-10'
end_date = '2023-01-20'

# Преобразуем в pandas.Timestamp
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# DataFrame для объединения данных
result_df = pd.DataFrame()

# Проход по годам и месяцам
for year_folder in parquet_folder.glob('year=*'):
    year = int(year_folder.stem.split('=')[1])
    if year < start_date.year or year > end_date.year:
        continue

    for month_folder in year_folder.glob('month=*'):
        month = int(month_folder.stem.split('=')[1])
        if (year == start_date.year and month < start_date.month) or \
           (year == end_date.year and month > end_date.month):
            continue

        # Считываем все Parquet-файлы внутри папки
        for parquet_file in month_folder.glob('*.parquet'):
            df = pd.read_parquet(parquet_file)

            # Фильтруем строки по диапазону дат
            filtered_df = df[(df['datetime'] >= start_date) & (df['datetime'] <= end_date)]

            if not filtered_df.empty:
                result_df = pd.concat([result_df, filtered_df], ignore_index=True)

# Вывод результата
print(result_df)
