import pandas as pd
from pathlib import Path

# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Укажите дату, которую нужно считать (в формате 'YYYY-MM-DD')
date_to_load = '2023-01-03'

# Преобразуем дату в формат pandas. Timestamp для удобства обработки
date_to_load = pd.to_datetime(date_to_load)

# Извлекаем год и месяц из указанной даты
year = date_to_load.year
month = date_to_load.month

# Путь к нужной партиции
partition_path = parquet_folder / f"year={year}" / f"month={month}"

# DataFrame для объединения данных
result_df = pd.DataFrame()

if partition_path.exists():
    # Читаем все Parquet-файлы в указанной партиции
    for parquet_file in partition_path.glob('*.parquet'):
        df = pd.read_parquet(parquet_file)

        # Фильтруем строки по указанному дню
        filtered_df = df[df['datetime'].dt.date == date_to_load.date()]

        if not filtered_df.empty:
            result_df = pd.concat([result_df, filtered_df], ignore_index=True)

# Вывод результата
print(result_df)
