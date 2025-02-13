import pandas as pd
from pathlib import Path

# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Укажите даты, которые нужно считать (в формате 'YYYY-MM-DD')
dates_to_load = ['2023-01-19', '2023-01-20']

# Преобразуем даты в формат pandas.Timestamp для удобства обработки
dates_to_load = pd.to_datetime(dates_to_load)

# Извлекаем уникальные годы и месяцы из указанных дат
years_months = dates_to_load.to_frame(index=False, name='datetime')
years_months['year'] = years_months['datetime'].dt.year
years_months['month'] = years_months['datetime'].dt.month
unique_years_months = years_months[['year', 'month']].drop_duplicates()

# DataFrame для объединения данных
result_df = pd.DataFrame()

# Считываем данные только из нужных партиций
for _, row in unique_years_months.iterrows():
    year = row['year']
    month = row['month']
    partition_path = parquet_folder / f"year={year}" / f"month={month}"

    if not partition_path.exists():
        continue

    # Читаем все Parquet-файлы в указанной партиции
    for parquet_file in partition_path.glob('*.parquet'):
        df = pd.read_parquet(parquet_file)

        # Фильтруем строки по указанным дням
        filtered_df = df[df['datetime'].dt.date.isin(dates_to_load.date)]

        if not filtered_df.empty:
            result_df = pd.concat([result_df, filtered_df], ignore_index=True)

# Вывод результата
print(result_df)

