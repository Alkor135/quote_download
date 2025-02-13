"""
Скрипт из файлов с тиковыми данными делает файл с базой данных
Очень большой получается файл БД, не используется
"""

import re
from datetime import datetime, timedelta, time
from typing import Any
# import datetime
from pathlib import Path
import sqlite3

import pandas as pd


class DateTimeConverter:
    """
    Преобразует дату и время в формат datetime и добавляет доли секунды.
    """
    def __init__(self):
        self.previous_time = None
        self.commul_msec = 0

    def convert(self, date_cell: str, time_cell: str) -> datetime:
        """ Преобразует дату и время """
        if self.previous_time == int(time_cell):
            self.commul_msec += 1
        else:
            self.commul_msec = 0

        date_cell_dt = datetime.strptime(f'{int(date_cell)}', '%Y%m%d')
        time_cell_dt = datetime.strptime(f'{int(time_cell)}', '%H%M%S').time()
        delta = timedelta(microseconds=self.commul_msec)
        # delta = timedelta(milliseconds=self.commul_msec)
        tick_datetime = datetime.combine(date_cell_dt, time_cell_dt) + delta  # Составление datetime

        self.previous_time = int(time_cell)

        return tick_datetime


def read_file(tick_files: list, path_file_db, year_tick):
    """  """
    for ind_file, tick_file in enumerate(tick_files, start=1):  # Итерация по файлам
        datetime_tick = DateTimeConverter()
        df: pd = pd.read_csv(tick_file, delimiter=',')  # Считываем тиковые данные в DF
        df['date_time'] = df.apply(lambda x: datetime_tick.convert(x['<DATE>'], x['<TIME>']), axis=1)
        # df = df.set_index('date_time').sort_index(ascending=True)
        df = df[['date_time', '<LAST>', '<VOL>']]
        df.rename(columns={'<LAST>': 'price', '<VOL>': 'volume'}, inplace=True)
        print(df)

        connection: Any = sqlite3.connect(path_file_db, check_same_thread=True)
        cursor: Any = connection.cursor()
        df.to_sql(year_tick, con=connection, schema='dbo', if_exists='append', index=False)  #, index=False


if __name__ == "__main__":
    ticker: str = 'RTS'
    year_tick: str = '2022'
    source_dir_tick: Path = Path(fr'c:/data_quote/data_finam_{ticker}_tick')  # Путь к ресурсному каталогу с тиками
    path_file_db = Path(fr'd:/data_quote/{ticker}_db/{ticker}_tick_futures.db')

    # Создание списка путей к файлам с тиками
    tick_files: list = list(source_dir_tick.glob(fr'*_{year_tick}*.csv'))

    read_file(tick_files, path_file_db, year_tick)
