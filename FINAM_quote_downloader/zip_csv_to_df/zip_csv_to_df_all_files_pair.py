from time import time
from pathlib import Path
import pandas as pd


# def load_zip_csv(path_load):
#     return pd.read_csv(path_load, compression='zip')


if __name__ == "__main__":
    start_time = time()  # Время начала запуска скрипта

    directory = Path(r'c:\data_quote\data_finam_RTS_tick_zip')
    # files = [file for file in directory.iterdir() if file.is_file()]
    files = list(directory.glob('*.zip'))

    for file_1, file_2 in zip(files, files[1:]):
        print(file_1, file_2)

    # print(files)

    # print(df)
    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
