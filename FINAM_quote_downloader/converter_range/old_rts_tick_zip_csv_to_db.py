"""
С ошибкой

Создает дата фрейм range баров и записывает в БД (нет зазора между барами)
"""

from pathlib import Path
from datetime import datetime
import sqlite3

import pandas as pd

import sqlighter3


def create_range_bars(tick_df, range_size):
    """
    Создает Range бары из тикового дата фрейма.

    Parameters:
        tick_df (pd.DataFrame): Дата фрейм с тиковыми данными
        (колонки: 'datetime', 'last', 'volume').
        range_size (float): Размер диапазона для Range баров.

    Returns:
        pd.DataFrame: Дата фрейм с Range барами
        (колонки: 'datetime', 'open', 'high', 'low', 'close', 'volume').
    """
    # Инициализация переменных
    range_bars = []
    open_price = None
    high_price = None
    low_price = None
    price = None
    vol = 0
    bar_start_time = None  # Время открытия текущего бара

    for _, row in tick_df.iterrows():
        price = row['last']
        volume = row['volume']
        date_time = row['datetime']

        # Если новый бар, инициализируем его
        if open_price is None:
            open_price = price
            high_price = price
            low_price = price
            bar_start_time = date_time  # Устанавливаем время первого тика

        # Обновляем high, low и объем
        high_price = max(high_price, price)
        low_price = min(low_price, price)
        vol += volume

        # Проверяем, превышен ли range_size
        if high_price - low_price >= range_size:
            # Закрываем текущий бар
            range_bars.append({
                'datetime': bar_start_time,  # Используем время первого тика в баре
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': price,
                'volume': vol
            })
            # Инициализируем следующий бар
            open_price = price
            high_price = price
            low_price = price
            vol = 0
            bar_start_time = date_time  # Обновляем время начала нового бара

    # Добавляем последний бар, если он не завершен
    if open_price is not None:
        range_bars.append({
            'datetime': bar_start_time,  # Используем время первого тика в баре
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': price,
            'volume': vol
        })

    df = pd.DataFrame(range_bars)
    df['size'] = range_size
    return df


def zip_csv_convert_to_db(files_lst, quantity_bars):

    for file_path in files_lst:
        size_dic = {}  # Создание словаря для подбора размера range бара

        # Чтение данных из файла в DF
        df_tick = pd.read_csv(file_path, compression='zip')

        # Удаляем дубликаты строк по 'datetime', оставляя только первое вхождение.
        df_tick = df_tick.drop_duplicates(subset='datetime', keep='first')

        # Максимальная дата в базе данных
        end_db_date = datetime.strptime(
            sqlighter3.get_max_date_futures(connection, cursor), "%Y-%m-%d %H:%M:%S.%f"
        ).date()

        # Последняя размерность range баров
        end_range_size = sqlighter3.get_end_size(connection, cursor)
        # print(end_range_size)
        # print(file_date)

        # Заполнение словаря размерностей range баров
        size_dic[end_range_size] = sqlighter3.get_count_lines_date(
            connection, cursor, end_db_date
        )
        size_dic[end_range_size - 50] = len(create_range_bars(df_tick, end_range_size - 50))
        size_dic[end_range_size + 50] = len(create_range_bars(df_tick, end_range_size + 50))

        # Вычисление размерности range баров к заданному количеству
        closest_key = min(size_dic, key=lambda k: abs(size_dic[k] - quantity_bars))

        # Получение DF с range барами
        df = create_range_bars(df_tick, closest_key)

        # Перебираем строки для занесения в БД
        for row in df.itertuples():
            sqlighter3.add_row(
                connection, cursor, row[1], row[2], row[3], row[4], row[5], row[6], row[7]
            )
        print(f'Файл {file_path} записан в БД. Размерность range баров: {closest_key}')


if __name__ == "__main__":
    # Параметры
    ticker = 'RTS'  # Тикер
    quantity_bars = 300  # Приблизительное количество range баров в день (влияет на размерность)
    path_zip = Path(r"C:\data_quote\data_finam_RTS_tick_zip")  # Путь к папке с zip архивами csv
    path_db = Path(fr'c:\Users\Alkor\gd\data_quote_db\{ticker}_Range.db')
    # Задайте дату в формате ГГГГММДД
    start_date = datetime.strptime("20150101", "%Y%m%d")
    # --------------------------------------------------------------------------------------------

    connection = sqlite3.connect(path_db, check_same_thread=True)
    cursor = connection.cursor()

    if sqlighter3.non_empty_table_futures(connection, cursor):  # Если таблица Range не пустая
        # Меняем стартовую дату на дату последней записи
        start_date = datetime.strptime(
            sqlighter3.get_max_date_futures(connection, cursor), "%Y-%m-%d %H:%M:%S.%f"
        ).date()

    # Список файлов
    files_with_paths = []

    # Обход папки
    for file in path_zip.glob("*.zip"):  # Ищем файлы с расширением .zip
        try:
            # Извлекаем дату из имени файла
            file_date = datetime.strptime(file.stem, "%Y%m%d").date()
            # Если дата файла > начальной даты, добавляем в список
            if file_date > start_date:  #
                files_with_paths.append(file)
        except ValueError:
            # Пропускаем файлы с неправильным форматом имени
            continue

    zip_csv_convert_to_db(files_with_paths, quantity_bars)

    # Выполняем команду VACUUM
    cursor.execute("VACUUM;")

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()
