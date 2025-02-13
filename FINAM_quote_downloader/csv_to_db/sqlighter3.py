"""
Создание БД с таблицами Futures и Options при запуске скрипта.
При доступе из других модулей получает доступ к БД.
"""
from pathlib import Path
from typing import Any
import sqlite3

import pandas as pd


def create_tables(connection, cursor, year):
    """ Функция создания таблиц в БД если их нет"""
    try:
        with connection:
            # cursor.execute('''DROP TABLE Futures''')
            # print("Удалена таблица 'Futures' из БД")
            cursor.execute('''CREATE TABLE if not exists `{}` (
                           `date_time`          DATETIME PRIMARY KEY UNIQUE NOT NULL,
                           `price`              INT NOT NULL,
                           `volume`             INT NOT NULL)'''.format(year)
                           )
        print('Taблица в БД созданы')
    except sqlite3.OperationalError as exception:
        print(f"Ошибка при создании БД: {exception}")


def non_empty_table_futures(connection, cursor):
    """Проверяем, есть ли записи в таблице 'Futures' в базе"""
    with connection:
        return cursor.execute("SELECT count(*) FROM (select 1 from Futures limit 1)").fetchall()[0][0]


def tradedate_futures_exists(connection, cursor, tradedate):
    """Проверяем, есть ли дата в таблице 'Futures' в базе"""
    with connection:
        result = cursor.execute('SELECT * FROM `Futures` WHERE `TRADEDATE` = ?', (tradedate,)).fetchall()
        return bool(len(result))


def tradedate_options_exists(connection, cursor, tradedate):
    """Проверяем, есть ли дата в таблице 'Options' в базе"""
    with connection:
        result = cursor.execute('SELECT * FROM `Options` WHERE `TRADEDATE` = ?', (tradedate,)).fetchall()
        return bool(len(result))


def add_tradedate_future(connection, cursor, tradedate, secid, open, low, high, close, volume, openposition, shortname,
                         lsttrade):
    """Добавляет строку в таблицу Futures """
    with connection:
        return cursor.execute(
            "INSERT INTO `Futures` (`TRADEDATE`, `SECID`, `OPEN`, `LOW`, `HIGH`, `CLOSE`, `VOLUME`, `OPENPOSITION`, "
            "`SHORTNAME`, `LSTTRADE`) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (tradedate, secid, open, low, high, close, volume, openposition, shortname, lsttrade))


def add_tradedate_option(connection, cursor, tradedate, secid, openposition, name, lsttrade, optiontype, strike):
    """Добавляет строку в таблицу Options """
    with connection:
        return cursor.execute(
            "INSERT INTO `Options` (`TRADEDATE`, `SECID`, `OPENPOSITION`, `NAME`, `LSTTRADE`, `OPTIONTYPE`, "
            "`STRIKE`) VALUES(?,?,?,?,?,?,?)",
            (tradedate, secid, openposition, name, lsttrade, optiontype, strike))


def get_tradedate_future(connection):  # Используется для перебора дат в
    """ Возвращает Dataframe с: дата торгов, короткое имя, последний день торгов из БД SQL по фьючерсам """
    with connection:
        return pd.read_sql('SELECT `TRADEDATE`, `SHORTNAME`, `LSTTRADE` FROM `Futures`', connection)


def get_tradedate_future_update(connection, start_date):
    """ Получение дат из обновленной таблицы Futures, для обновления таблицы Options """
    with connection:
        return pd.read_sql(f'SELECT TRADEDATE, SHORTNAME FROM Futures WHERE TRADEDATE >= "{start_date}"', connection)


def get_tradedate_future_date(connection, cursor, datedraw):
    """ Возвращает: дата торгов, low, high, close, короткое имя, последний день торгов из БД SQL на дату """
    with connection:
        return cursor.execute('SELECT LOW, HIGH, CLOSE, SHORTNAME, LSTTRADE '
                              'FROM Futures WHERE TRADEDATE = ?', (datedraw,)).fetchall()[0]


def get_df_datedraw(connection, datedraw):
    """ Возвращает выборку соответствующую дате построения графика """
    with connection:
        return pd.read_sql(f'SELECT * FROM Options '
                           f'WHERE TRADEDATE = "{datedraw}" AND TRADEDATE < LSTTRADE', connection)


def get_max_date_futures(connection, cursor):
    """ Получение максимальной даты по фьючерсам """
    with connection:
        return cursor.execute('SELECT MAX (TRADEDATE) FROM Futures').fetchall()[0][0]


def delete_options_bag(connection, cursor):
    """ Удаление опционов где дата торгов больше даты экспирации опционов """
    with connection:
        return cursor.execute('DELETE FROM Options WHERE TRADEDATE > LSTTRADE')


def checkTableExists(connection, cursor, year):
    """ Не работает """
    try:
        cursor.execute('''SELECT * FROM {}'''.format(year))
        cursor.close()
        return True
    except sqlite3.OperationalError as exception:
        print(f"Ошибка при проверке наличия таблицы в БД: {exception}")
        return False
    finally:
        cursor.close()


if __name__ == '__main__':  # Создание БД, если её не существует
    # Настройка базы данных
    year = 2022
    tiker: str = 'RTS'
    path_db: Path = Path(fr'd:/data_quote/{tiker}_db')  # Путь к целевому каталогу  # Папка с БД
    file_db: str = f'{tiker}_tick_futures.db'

    if not path_db.is_dir():  # Если не существует папка под БД
        try:
            path_db.mkdir()  # Создание папки под БД
        except Exception as err:
            print(f'Ошибка создания каталога "{path_db}": {err}')

    path_file = Path(fr'{path_db}\{file_db}')
    connection: Any = sqlite3.connect(path_file, check_same_thread=True)
    cursor: Any = connection.cursor()

    # if checkTableExists(connection, cursor, year):  # Если не существует таблица в БД
    create_tables(connection, cursor, year)  # Создание таблиц в БД если их нет
