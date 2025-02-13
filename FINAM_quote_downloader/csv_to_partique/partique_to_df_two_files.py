import pandas as pd
from pathlib import Path

# Путь к папке с Parquet-файлами
parquet_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Список для хранения путей ко всем Parquet-файлам
parquet_files = []

# Чтение всех партиций
for year_folder in parquet_folder.glob('year=*'):
    for month_folder in year_folder.glob('month=*'):
        # Сохранение путей ко всем файлам в текущей партиции
        for parquet_file in month_folder.glob('*.parquet'):
            parquet_files.append(parquet_file)

# Вывод списка файлов
print(f"Найдено {len(parquet_files)} файлов:")
# for file in parquet_files:
#     print(file)

# Пример чтения двух файлов Parquet
if len(parquet_files) >= 2:
    # Чтение первого файла
    df1 = pd.read_parquet(
        parquet_files[0], columns=['datetime', '<LAST>', '<VOL>'], engine='pyarrow'
        )
    # Чтение второго файла
    df2 = pd.read_parquet(
        parquet_files[1], columns=['datetime', '<LAST>', '<VOL>'], engine='pyarrow'
        )  

    # Объединение данных из двух файлов
    combined_df = pd.concat([df1, df2], ignore_index=True)

    print("Пример данных из объединенных файлов:")
    print(combined_df)
else:
    print("Недостаточно файлов для чтения двух файлов.")
    