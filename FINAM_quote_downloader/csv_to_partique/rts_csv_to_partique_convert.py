from pathlib import Path
import pandas as pd

# Путь к папке с исходными CSV-файлами
input_folder = Path(r'c:\data_quote\data_finam_RTS_tick')
# Путь к папке для сохранения Parquet-файлов
output_folder = Path(r'c:\data_quote\parquet_finam_RTS_tick')

# Создаем папку для Parquet-файлов, если она не существует
output_folder.mkdir(parents=True, exist_ok=True)

# Получаем список всех CSV-файлов в папке
csv_files = [f for f in input_folder.glob('*.csv')]

for csv_file in csv_files:
    # Читаем CSV-файл
    df = pd.read_csv(csv_file)

    # Проверяем наличие нужных колонок
    required_columns = {'<DATE>', '<TIME>', '<LAST>', '<VOL>'}
    if not required_columns.issubset(df.columns):
        print(f"Файл {csv_file.name} пропущен: отсутствуют нужные колонки.")
        continue

    # Преобразуем <DATE> и <TIME> в один столбец datetime для удобной фильтрации
    df['datetime'] = pd.to_datetime(
        df['<DATE>'].astype(str) + df['<TIME>'].astype(str).str.zfill(6),
        format='%Y%m%d%H%M%S',
        errors='coerce'
    )

    # Удаляем строки с некорректной датой/временем
    df = df.dropna(subset=['datetime'])

    # Если DataFrame пустой, пропускаем файл
    if df.empty:
        print(f"Файл {csv_file.name} пропущен: все строки некорректны.")
        continue

    # Добавляем колонки для года и месяца
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month

    # Определяем путь для сохранения
    output_path = output_folder / f"year={df['year'].iloc[0]}" / f"month={df['month'].iloc[0]}"
    output_path.mkdir(parents=True, exist_ok=True)

    # Имя Parquet-файла
    parquet_file = output_path / csv_file.name.replace('.csv', '.parquet')

    # Сохраняем в формате Parquet
    df.to_parquet(parquet_file, index=False, engine='pyarrow')

    print(f"Файл {csv_file.name} преобразован и сохранен в {parquet_file}")

print("Все файлы обработаны и сохранены.")
