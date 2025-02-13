from time import time
from pathlib import Path
import pandas as pd


def load_zip_csv(path_load):
    return pd.read_csv(path_load, compression='zip', parse_dates=['datetime'])


if __name__ == "__main__":
    start_time = time()  # Время начала запуска скрипта
    # Папка для чтения файлов котировок
    dir_data: str = r'c:\data_quote\data_finam_RTS_tick_zip'

    date_load = '20150105'

    path_file = Path(Path(fr'{dir_data}\{date_load}.zip'))

    if path_file.exists():
        print("Файл существует")

    df = load_zip_csv(path_file)
    print(df)
    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
