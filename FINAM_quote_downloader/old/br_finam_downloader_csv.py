"""
Закачивает исторические данные с ФИНАМа, за указанный период, в указанном формате
Каждый день в своем файле
Настройки внизу.
"""
import datetime as dt
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
import urllib.request
import time
import os

from pathlib import Path
import pandas as pd

from settings import *


class DownloadFinam:
    def __init__(self, ticker: str, dir_data: str, market: int, daft: int, period: int = 3) -> None:
        self.dir_data: str = dir_data
        self.ticker: str = ticker
        self.period: int = period
        self.market: int = market
        self.datf: int = daft
        self.url: str = ''
        self.req: Any = None

    def create_request_finam(self, download_date: str) -> None:
        """ Метод составляет запрос на сервер FINAMa """
        start_date: dt = dt.datetime.strptime(download_date, "%Y%m%d").date()  # Переводим формат в datetime.date()
        end_date: dt = start_date  # Для закачки одного дня в один файл

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
            ('sep', 1),  # Разделитель полей	(1 - запятая, 2 - точка, 3 - точка с запятой, 4 - табуляция, 5 - пробел)
            ('sep2', 1),  # Разделитель разрядов
            ('datf', self.datf),  # Формат записи в файл. Выбор из 11 возможных (1-для минутных баров, 7-для тиков).
            ('at', 1)  # Нужны ли заголовки столбцов
        ])

        self.url = f'{FINAM_URL}{ticker}_{download_date}.csv?{params}'  # урл составлен
        # Кроме url, в запрос подставляем заголовок, чтобы сервер думал, что к нему обращается браузер
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
        period_txt: str = PERIODS[period]  # Для добавления к имени файла периода
        file_name: str = f'{self.ticker}_{period_txt}_{file_name_date}.csv'  # Имя выходного файла

        dir_path = Path(self.dir_data)  # Папка для сохранения
        if not dir_path.exists():  # Проверяем существует ли папка
            dir_path.mkdir(parents=True)  # Создаем папку при её отсутствии

        return Path(f'{self.dir_data}/{file_name}')  # Создаем пути для сохранения файла и возвращаем

    def run(self, download_date: str, file_name_date: str) -> None:
        self.create_request_finam(download_date)  # Вызываем функцию составления запроса
        file_path: Path = self.path_file(file_name_date)  # Вызываем функцию составления путей и имени файла
        if not file_path.exists():  # Если файла не существует
            txt: Any = urlopen(self.req).readlines()  # Получаем в txt массив данных с Финама.

            with open(file_path, 'w', encoding='utf-8') as file_out:  # задаём файл, в который запишем котировки.
                for line in txt:  # записываем файл строку за строкой.
                    file_out.write(line.strip().decode("utf-8") + '\n')

            print(f'Готово. Проверьте файл {file_path} по указанному пути')
            time.sleep(2)  # Сон в 2 секунды
        else:
            print(f'Файл {file_path} уже существует')


if __name__ == "__main__":
    """ 
    Основные настройки параметров загрузки котировок.
    Проверьте наличие тикера в файле settings.py
    """
    # Папка для сохранения файлов котировок (папка c:/data_quote/data_finam_RTS_tick/)
    # dir_data: str = r'c:\data_quote\data_finam_RTS_tick'
    dir_data: str = r'c:\data_quote\data_finam_BR_tick'

    # задаём тикер (проверить наличие тикера в settings.py)
    # ticker: str = "SPFB.RTS"
    ticker: str = "SPFB.BR"

    # задаём период. Выбор из: 'tick': 1, 'min': 2, '5min': 3, '10min': 4, '15min': 5, '30min': 6, 'hour': 7
    period: int = 1

    # 14 - non-expired futures, 0 - для акций
    market: int = 14

    # Формат записи в файл. Выбор из 11 возможных (5-для минутных баров, 9-для тиков).
    # 5 - Формат записи для минутных баров 'DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL'
    # 9 - <DATE>,<TIME>,<LAST>,<VOL>
    daft: int = 9

    start_date_range: dt = dt.date(2022, 1, 24)  # Дата начала закачки
    end_date_range: dt = dt.date(2025, 1, 19)  # Дата окончания закачки

    # Далее идет исполняемый код (не настройки)
    # Делаем преобразования дат:
    date_range: pd = pd.date_range(start_date_range, end_date_range)  # Список дат в диапазоне

    data: DownloadFinam = DownloadFinam(ticker, dir_data, market, daft, period)  # Создаем экземпляр класса
    for single_date in date_range:  # Итерируемся по диапазону дат
        download_date: str = single_date.strftime('%Y%m%d')  # Дата для закачки котировок
        print(download_date)
        file_name_date: str = single_date.strftime('%Y%m%d')  # Дата которую будем подставлять в имя файла
        data.run(download_date, file_name_date)
    print('Закачка котировок завершена. Удалите пустые файлы.')
