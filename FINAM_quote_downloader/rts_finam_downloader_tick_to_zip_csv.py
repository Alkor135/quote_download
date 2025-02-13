"""
Закачивает исторические данные с ФИНАМа, за указанный период, в указанном формате.
Сохраняет каждый день в своем файле zip
Настройки внизу.
"""
import datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
import urllib.request
import time
import zipfile
from io import StringIO
from pathlib import Path

import pandas as pd

from settings import *


def make_timestamps_unique(df, time_column='datetime'):
    """
    Делает временные метки уникальными, добавляя миллисекунды к повторяющимся.

    Parameters:
        df (pd.DataFrame): Дата фрейм, в котором нужно сделать уникальные временные метки.
        time_column (str): Название колонки с временными метками.

    Returns:
        pd.DataFrame: Дата фрейм с уникальными временными метками.
    """
    df = df.copy()

    # Считаем количество повторений для каждой временной метки
    duplicates = df[time_column].duplicated(keep=False)
    duplicate_counts = df[time_column][duplicates].groupby(df[time_column]).cumcount()

    # Добавляем миллисекунды к повторяющимся временным меткам
    df.loc[duplicates, time_column] += pd.to_timedelta(duplicate_counts * 1, unit='ms')

    # Удаляем дубликаты, оставляя только первое вхождение (контрольное), если больше 999
    df = df.drop_duplicates(subset='datetime', keep='first')

    return df


class DownloadFinam:
    def __init__(self, ticker: str, dir_data: str, market: int, daft: int, period: int = 3):
        self.dir_data: str = dir_data
        self.ticker: str = ticker
        self.period: int = period
        self.market: int = market
        self.datf: int = daft
        self.url: str = ''
        self.req: Any = None

    def create_request_finam(self, download_date: str) -> None:
        """ Метод составляет запрос на сервер FINAMa """
        # Переводим формат в datetime.date()
        start_date = datetime.datetime.strptime(download_date, "%Y%m%d").date()
        end_date = start_date  # Для закачки одного дня в один файл

        # Все параметры упаковываем в единую структуру.
        # Здесь есть дополнительные параметры, кроме тех, которые заданы в шапке. См. комментарии.
        params = urlencode([
            ('market', self.market),  # на каком рынке торгуется бумага
            ('em', TICKERS[ticker]),  # вытягиваем цифровой символ, который соответствует бумаге.
            ('code', self.ticker),  # тикер фин инструмента
            ('apply', 0),  # не нашёл что это значит.
            ('df', start_date.day),  # Начальная дата, номер дня (1-31)
            ('mf', start_date.month - 1),  # Начальная дата, номер месяца (0-11)
            ('yf', start_date.year),  # Начальная дата, год
            ('from', start_date),  # Начальная дата полностью
            ('dt', end_date.day),  # Конечная дата, номер дня
            ('mt', end_date.month - 1),  # Конечная дата, номер месяца
            ('yt', end_date.year),  # Конечная дата, год
            ('to', end_date),  # Конечная дата
            ('p', self.period),  # Таймфрейм
            ('f', self.ticker + "_" + download_date),  # Имя сформированного файла
            ('e', ".csv"),  # Расширение сформированного файла
            ('cn', self.ticker),  # ещё раз тикер
            # См.страницу  # https://www.finam.ru/profile/moex-akcii/sberbank/export/
            ('dtf', 1),  # В каком формате брать даты. Выбор из 5 возможных.
            ('tmf', 1),  # В каком формате брать время. Выбор из 4 возможных.
            ('MSOR', 0),  # Время свечи (0 - open; 1 - close)
            ('mstime', "on"),  # Московское время
            ('mstimever', 1),  # Коррекция часового пояса
            # Разделитель полей	(1 - запятая, 2 - точка, 3 - точка с запятой,
            # 4 - табуляция, 5 - пробел)
            ('sep', 1),
            ('sep2', 1),  # Разделитель разрядов
            # Формат записи в файл. Выбор из 11 возможных (1-для минутных баров, 7-для тиков).
            ('datf', self.datf),
            ('at', 1)  # Нужны ли заголовки столбцов
        ])

        self.url = f'{FINAM_URL}{ticker}_{download_date}.csv?{params}'  # урл составлен
        # Кроме url, в запрос подставляем заголовок, чтобы сервер думал,
        # что к нему обращается браузер
        self.req = urllib.request.Request(
            self.url,
            data=None,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) '
                                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                                   'Chrome/35.0.1916.47 Safari/537.36'}
        )
        # print(f'Запрос составлен: {self.req}')

    def path_file(self, file_name_date: str) -> Path:
        """ Метод создает имя файла и путь его сохранения """
        self.file_name: str = fr'{file_name_date}.zip'  # Имя выходного файла

        dir_path = Path(self.dir_data)  # Папка для сохранения
        if not dir_path.exists():  # Проверяем существует ли папка
            dir_path.mkdir(parents=True)  # Создаем папку при её отсутствии

        # Создаем пути для сохранения файла и возвращаем
        return Path(f'{self.dir_data}/{self.file_name}')

    def run(self, download_date: str) -> None:
        # Вызываем функцию составления путей и имени файла
        file_path: Path = self.path_file(download_date)

        if not file_path.exists():  # Если файла не существует
            # Вызываем функцию составления запроса
            self.create_request_finam(download_date)

            # Получаем в txt массив данных с Финама.
            txt: Any = urlopen(self.req).readlines()

            # Преобразуем массив строк в текст
            decoded_data = [line.decode('utf-8').strip() for line in txt]

            # Преобразуем текст в DataFrame
            data_text = "\n".join(decoded_data)
            df = pd.read_csv(StringIO(data_text))  # Преобразование в DataFrame

            # Проверка на пустой DF
            if df.empty:
                print(f"Дата {download_date} пропущена: пустой ответ.")
                return

            # Переименование колонок
            df.rename(
                columns={
                    '<DATE>': 'date', '<TIME>': 'time', '<LAST>': 'last', '<VOL>': 'volume'
                    }, inplace=True
                )

            # Преобразуем date и time в один столбец datetime для удобной фильтрации
            df['datetime'] = pd.to_datetime(
                df['date'].astype(str) + df['time'].astype(str).str.zfill(6),
                format='%Y%m%d%H%M%S',
                errors='coerce'
            )

            # Делаем временные метки уникальными, добавляя миллисекунды к повторяющимся.
            df = make_timestamps_unique(df[['datetime', 'last', 'volume']], time_column='datetime')

            # Сохраняем DataFrame в CSV с ZIP-сжатием
            zip_filename = fr"{file_path}"  # zip
            csv_filename = fr"{download_date}.csv"

            with zipfile.ZipFile(zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                with zf.open(csv_filename, mode="w") as buffer:
                    df.to_csv(buffer, index=False)

            print(f'Готово. Проверьте файл {file_path} по указанному пути')
            time.sleep(2)  # Сон в 2 секунды
        else:
            print(f'Файл {file_path} уже существует')


if __name__ == "__main__":
    """ 
    Основные настройки параметров загрузки котировок.
    Проверьте наличие тикера в файле settings.py
    """
    # ------------------------------------------------------------------------
    # Папка для сохранения файлов котировок (папка c:/data_quote/data_finam_RTS_tick/)
    dir_data: str = r'c:\data_quote\data_finam_RTS_tick_zip'
    # dir_data: str = r'c:\data_quote\data_finam_BR_tick'

    # задаём тикер (проверить наличие тикера в settings.py)
    ticker: str = "SPFB.RTS"
    # ticker: str = "SPFB.BR"

    # задаём период. Выбор из: 'tick': 1, 'min': 2, '5min': 3,
    # '10min': 4, '15min': 5, '30min': 6, 'hour': 7
    period: int = 1

    # 14 - non-expired futures, 0 - для акций
    market: int = 14

    # Формат записи в файл. Выбор из 11 возможных (5-для минутных баров, 9-для тиков).
    # 5 - Формат записи для минутных баров 'DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL'
    # 9 - <DATE>,<TIME>,<LAST>,<VOL>
    daft: int = 9

    start_date_range = datetime.date(2025, 1, 1)  # Дата начала закачки
    # end_date_range = datetime.date(2025, 1, 27)  # Дата окончания закачки
    # Дата окончания закачки — текущая дата минус 1 день
    end_date_range = datetime.date.today() - datetime.timedelta(days=1)
    # ------------------------------------------------------------------------

    # Далее идет исполняемый код (не настройки)
    # Делаем преобразования дат:
    date_range: pd = pd.date_range(start_date_range, end_date_range)  # Список дат в диапазоне

    # Создаем экземпляр класса
    downloader: DownloadFinam = DownloadFinam(ticker, dir_data, market, daft, period)
    for single_date in date_range:  # Итерируемся по диапазону дат
        # Дата для закачки котировок в строковом формате
        download_date: str = single_date.strftime('%Y%m%d')  
        print(download_date)

        downloader.run(download_date)
