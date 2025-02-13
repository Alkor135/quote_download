"""
Создание БД с таблицей range баров фьючерса RTS
При доступе из других модулей получает доступ к БД.
"""
from pathlib import Path
from typing import Any
import sqlite3


def create_tables(connection, cursor):
    """ Функция создания таблиц в БД если их нет"""
    try:
        with connection:
            # cursor.execute('''DROP TABLE Range''')
            # print("Удалена таблица 'Range' из БД")
            cursor.execute('''CREATE TABLE if not exists Range (
                            datetime          DATE PRIMARY KEY UNIQUE NOT NULL,
                            open              REAL NOT NULL,
                            high              REAL NOT NULL,
                            low               REAL NOT NULL,
                            close             REAL NOT NULL,
                            volume            INTEGER NOT NULL,
                            size              INTEGER NOT NULL)'''
                           )
        print('Taблицы в БД созданы')
    except sqlite3.OperationalError as exception:
        print(f"Ошибка при создании БД: {exception}")


def non_empty_table_futures(connection, cursor):
    """Проверяем, есть ли записи в таблице 'Range' в БД"""
    with connection:
        return cursor.execute("SELECT count(*) FROM (select 1 from Range limit 1)").fetchall()[0][0]


def tradedate_futures_exists(connection, cursor, tradedate):
    """Проверяем, есть ли указанная дата в таблице 'Range' в БД"""
    with connection:
        result = cursor.execute('SELECT * FROM `Range` WHERE `datetime` = ?', (tradedate,)).fetchall()
        return bool(len(result))


def add_row(connection, cursor, tradedate, open, high, low, close, volume, size):
    """Добавляет строку в таблицу Range """
    with connection:
        return cursor.execute(
            "INSERT INTO `Range` (`datetime`, `open`, `high`, `low`, `close`, `volume`, `size`) VALUES(?,?,?,?,?,?,?)",
            (tradedate, open, high, low, close, volume, size)
        )


def get_max_date_futures(connection, cursor):
    """ Получение максимальной даты по фьючерсам """
    with connection:
        return cursor.execute('SELECT MAX (datetime) FROM Range').fetchall()[0][0]


def get_end_size(connection, cursor):
    """ Получение size последней записи """
    with connection:
        return cursor.execute("SELECT size FROM Range ORDER BY ROWID DESC LIMIT 1").fetchone()[0]  # ROWID


def get_count_lines_date(connection, cursor, specific_date):
    """ Получение количества строк за дату """
    with connection:
        # SQL-запрос
        query = """
        SELECT COUNT(*) 
        FROM Range 
        WHERE date(datetime) = ?
        """

        # Выполнение запроса
        cursor.execute(query, (specific_date,))
        count = cursor.fetchone()[0]
        return count


if __name__ == '__main__':  # Создание БД, если её не существует
    # Настройка базы данных
    tiker: str = 'RTS'
    path_bd: Path = Path(r'c:\Users\Alkor\gd\data_quote_db')  # Папка с БД
    file_bd: str = f'{tiker}_Range.db'

    if not path_bd.is_dir():  # Если не существует папка под БД
        try:
            path_bd.mkdir()  # Создание папки под БД
        except Exception as err:
            print(f'Ошибка создания каталога "{path_bd}": {err}')

    connection = sqlite3.connect(fr'{path_bd}\{file_bd}', check_same_thread=True)
    cursor = connection.cursor()

    # Создание таблиц в БД если их нет
    create_tables(connection, cursor)

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()
