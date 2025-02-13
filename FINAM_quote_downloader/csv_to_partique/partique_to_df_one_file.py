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

# Пример чтения одного файла Parquet
if parquet_files:
    example_file = parquet_files[0]  # Выберите первый файл или любой другой
    df = pd.read_parquet(example_file, columns=['datetime', '<LAST>', '<VOL>'], engine='pyarrow')
    print("Пример данных из файла:")
    print(df)
