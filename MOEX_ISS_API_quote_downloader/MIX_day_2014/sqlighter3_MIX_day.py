"""
Создание БД с таблицей Futures при запуске скрипта.
При доступе из других модулей получает доступ к БД.
"""
from pathlib import Path
import sqlite3
import pandas as pd


def create_tables(connection, cursor):
    """ Функция создания таблицы в БД если её нет"""
    with connection:
        try:
            cursor.execute('''CREATE TABLE if not exists Futures (
                            TRADEDATE         DATE PRIMARY KEY UNIQUE NOT NULL,
                            SECID             TEXT NOT NULL,
                            OPEN              REAL NOT NULL,
                            LOW               REAL NOT NULL,
                            HIGH              REAL NOT NULL,
                            CLOSE             REAL NOT NULL,
                            LSTTRADE          DATE NOT NULL)'''
                           )
            print('Taблица в БД создана или уже существует')
        except sqlite3.OperationalError as e:
            print(f"Ошибка при создании таблицы Futures: {e}")


# def non_empty_table_futures(connection, cursor):
#     """Проверяем, есть ли записи в таблице 'Futures' в базе"""
#     with connection:
#         return cursor.execute("SELECT count(*) FROM (select 1 from Futures limit 1)").fetchall()[0][0]
    
def non_empty_table_futures(connection, cursor):
    with connection:
        return cursor.execute("SELECT 1 FROM Futures LIMIT 1").fetchone() is not None


def tradedate_futures_exists(connection, cursor, tradedate):
    """Проверяем, есть ли дата в таблице 'Futures' в базе"""
    with connection:
        result = cursor.execute('SELECT * FROM `Futures` WHERE `TRADEDATE` = ?', (tradedate,)).fetchall()
        return bool(len(result))


def add_tradedate_future(connection, cursor, tradedate, secid, open, low, high, close, lsttrade):
    """Добавляет строку в таблицу Futures"""
    try:
        with connection:
            return cursor.execute(
                "INSERT INTO `Futures` (`TRADEDATE`, `SECID`, `OPEN`, `LOW`, `HIGH`, `CLOSE`, `LSTTRADE`) VALUES(?,?,?,?,?,?,?)",
                (tradedate, secid, open, low, high, close, lsttrade)
            )
    except sqlite3.IntegrityError as e:
        print(f"Ошибка вставки данных в таблицу Futures: {e}")


def get_max_date_futures(connection, cursor):
    """ Получение максимальной даты по фьючерсам """
    with connection:
        return cursor.execute('SELECT MAX (TRADEDATE) FROM Futures').fetchall()[0][0]
    

def get_max_lsttrade(connection, cursor):
    """ Получение максимальной даты последних торгов по фьючерсам """
    with connection:
        return cursor.execute('SELECT MAX (LSTTRADE) FROM Futures').fetchall()[0][0]


if __name__ == '__main__':  # Создание БД, если её не существует
    # Настройка базы данных
    ticker: str = 'MIX'
    path_bd: Path = Path(r'c:\Users\Alkor\gd\data_quote_db')  # Папка с БД
    file_bd: str = f'{ticker}_day_2014.db'

    if not path_bd.is_dir():  # Если не существует папка под БД
        try:
            path_bd.mkdir(parents=True, exist_ok=True)  # Создание папки под БД
        except PermissionError as e:
            print(f"Недостаточно прав для создания каталога {path_bd}: {e}")

    with sqlite3.connect(fr'{path_bd}\{file_bd}', check_same_thread=True) as connection:
        cursor = connection.cursor()
        create_tables(connection, cursor)
