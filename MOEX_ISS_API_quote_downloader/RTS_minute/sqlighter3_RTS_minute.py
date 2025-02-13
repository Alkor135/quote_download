"""
Создание БД с таблицей минутных котировок фьючерса RTS
При доступе из других модулей получает доступ к БД.
"""
from pathlib import Path
from typing import Any
import sqlite3

import pandas as pd


def create_tables(connection, cursor):
    """ Функция создания таблиц  в БД если их нет"""
    try:
        with connection:
            # cursor.execute('''DROP TABLE Day''')
            # print("Удалена таблица 'Day' из БД")
            cursor.execute('''CREATE TABLE if not exists Minute (
                            TRADEDATE         DATE PRIMARY KEY UNIQUE NOT NULL,
                            SECID             TEXT NOT NULL,
                            OPEN              REAL NOT NULL,
                            LOW               REAL NOT NULL,
                            HIGH              REAL NOT NULL,
                            CLOSE             REAL NOT NULL,
                            VOLUME            INTEGER NOT NULL,
                            LSTTRADE          DATE NOT NULL)'''
                           )
        print('Taблицы в БД созданы')
    except sqlite3.OperationalError as exception:
        print(f"Ошибка при создании БД: {exception}")


def non_empty_table_futures(connection, cursor):
    """Проверяем, есть ли записи в таблице 'Minute' в БД"""
    with connection:
        return cursor.execute("SELECT count(*) FROM (select 1 from Minute limit 1)").fetchall()[0][0]


def tradedate_futures_exists(connection, cursor, tradedate):
    """Проверяем, есть ли указанная дата в таблице 'Minute' в БД"""
    with connection:
        result = cursor.execute('SELECT * FROM `Minute` WHERE `TRADEDATE` = ?', (tradedate,)).fetchall()
        return bool(len(result))


def add_row(connection, cursor, tradedatetime, secid, open, low, high, close, volume, lsttrade):
    """Добавляет строку в таблицу Minute """
    with connection:
        return cursor.execute(
            "INSERT INTO `Minute` (`TRADEDATE`, `SECID`, `OPEN`, `LOW`, `HIGH`, `CLOSE`, `VOLUME`, "
            "`LSTTRADE`) VALUES(?,?,?,?,?,?,?,?)",
            (tradedatetime, secid, open, low, high, close, volume, lsttrade))


def get_tradedate_future(connection):  # Используется для перебора дат в
    """ Возвращает Dataframe с: дата торгов, короткое имя, последний день торгов из БД SQL по фьючерсам """
    with connection:
        return pd.read_sql('SELECT `TRADEDATE`, `SHORTNAME`, `LSTTRADE` FROM `Day`', connection)


def get_tradedate_future_update(connection, start_date):
    """ Получение дат из обновленной таблицы Day, для обновления таблицы Options """
    with connection:
        return pd.read_sql(f'SELECT TRADEDATE, SHORTNAME FROM Day WHERE TRADEDATE >= "{start_date}"', connection)


def get_tradedate_future_date(connection, cursor, datedraw):
    """ Возвращает: дата торгов, low, high, close, короткое имя, последний день торгов из БД SQL на дату """
    with connection:
        return cursor.execute('SELECT LOW, HIGH, CLOSE, SHORTNAME, LSTTRADE '
                              'FROM Day WHERE TRADEDATE = ?', (datedraw,)).fetchall()[0]


def get_max_date_futures(connection, cursor):
    """ Получение максимальной даты по фьючерсам """
    with connection:
        return cursor.execute('SELECT MAX (TRADEDATE) FROM Minute').fetchall()[0][0]


if __name__ == '__main__':  # Создание БД, если её не существует
    # Настройка базы данных
    tiker: str = 'RTS'
    path_bd: Path = Path(r'c:\Users\Alkor\gd\data_quote_db')  # Папка с БД
    file_bd: str = f'{tiker}_futures_minute.db'

    if not path_bd.is_dir():  # Если не существует папка под БД
        try:
            path_bd.mkdir()  # Создание папки под БД
        except Exception as err:
            print(f'Ошибка создания каталога "{path_bd}": {err}')

    connection: Any = sqlite3.connect(fr'{path_bd}\{file_bd}', check_same_thread=True)
    cursor: Any = connection.cursor()
    create_tables(connection, cursor)  # Создание таблиц в БД если их нет