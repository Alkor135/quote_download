"""
Получение исторических данных по фьючерсам RTS с MOEX ISS API и занесение записей в БД.
Загружать от 2015-01-01.
Загружает и последний день торгов фьючерса, при этом OPENPOSITION равен 0.
"""

from pathlib import Path
import requests
from datetime import datetime, timedelta, date
from typing import Any

import apimoex
import pandas as pd
import sqlite3

import sqlighter3_MIX_day


def get_info_future(session: Any, security: str):
    """
    Запрашивает у MOEX информацию по инструменту
    :param session: Подключение к MOEX
    :param security: Тикер инструмента
    :return: Дата последних торгов (если её нет то возвращает дату удаления тикера,
    если её нет то возвращает 2130.01.01), короткое имя
    """
    security_info = apimoex.find_security_description(session, security)
    df = pd.DataFrame(security_info)

    # Удаляем строку с 'DELIVERYTYPE', слишком длинная
    df.drop(df[(df['name'] == 'DELIVERYTYPE')].index, inplace=True)
    # print(df.to_string(max_rows=20, max_cols=15), '\n')

    name_lst = list(df['name'])  # Поле 'name' в список
    if 'SHORTNAME' in name_lst:
        shortname = df.loc[df[df['name'] == 'SHORTNAME'].index]['value'].values[0]
        # print(type(shortname))
        # print(shortname, '\n')
    else:
        shortname = float('nan')

    if 'LSTTRADE' in name_lst:
        lsttrade = df.loc[df[df['name'] == 'LSTTRADE'].index]['value'].values[0]
    elif 'LSTDELDATE' in name_lst:
        lsttrade = df.loc[df[df['name'] == 'LSTDELDATE'].index]['value'].values[0]
    else:
        lsttrade = '2130-01-01'

    return pd.Series([shortname, lsttrade])


def get_future_date_results(tradedate: date, ticker: str):
    today_date = datetime.now().date()  # Текущая дата и время

    arguments = {'securities.columns': (
        "BOARDID, TRADEDATE, SECID, OPEN, LOW, HIGH, CLOSE, OPENPOSITIONVALUE, VALUE, "
        "VOLUME, OPENPOSITION, SETTLEPRICE"
    )}

    with requests.Session() as session:
        # print(f'{trade_date=}, {start_date=}')

        while tradedate != today_date:
            # Нет записи с такой датой
            if not sqlighter3_MIX_day.tradedate_futures_exists(connection, cursor, tradedate):
                request_url = (
                    f'https://iss.moex.com/iss/history/engines/futures/markets/forts/'
                    f'securities.json?date={tradedate.strftime("%Y-%m-%d")}&assetcode={ticker}'
                )
                print(f'{request_url=}')
                iss = apimoex.ISSClient(session, request_url, arguments)
                data = iss.get()
                df = pd.DataFrame(data['history'])
                # print(df.to_string(max_rows=20, max_cols=15), '\n')
                if len(df) != 0:  # Если полученный ответ не нулевой, чтобы исключить выходные дни
                    # Создаем новые колонки 'SHORTNAME', 'LSTTRADE' и заполняем
                    df[['SHORTNAME', 'LSTTRADE']] = df.apply(
                        lambda x: get_info_future(session, x['SECID']), axis=1
                    )
                    df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"]).dt.date
                    # Убираем строки, где дата последних торгов больше даты экспирации
                    df = df.loc[df['LSTTRADE'] >= tradedate]
                    # Удаление строк с пустыми OPEN, LOW, HIGH, CLOSE
                    df.dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'], inplace=True)
                    # Выборка строк с минимальной датой
                    df = (df[df['LSTTRADE'] == df['LSTTRADE'].min()]).reset_index(drop=True)

                    print(df.to_string(max_rows=20, max_cols=15))
                    # print(df.loc[0]['OPENPOSITION'])
                    if len(df) == 1:  # Если одна строка в DF
                        # Проверка на пустые значения поля 'OPEN'
                        if not df['OPEN'].isnull().values.any():
                            # Запись строки в БД
                            sqlighter3_MIX_day.add_tradedate_future(
                                connection, cursor, df.loc[0]['TRADEDATE'], df.loc[0]['SECID'],
                                float(df.loc[0]['OPEN']), float(df.loc[0]['LOW']),
                                float(df.loc[0]['HIGH']), float(df.loc[0]['CLOSE']),
                                int(df.loc[0]['VOLUME']), int(df.loc[0]['OPENPOSITION']),
                                df.loc[0]['SHORTNAME'], df.loc[0]['LSTTRADE']
                            )
                            print('Строка записана в БД', '\n')

            tradedate += timedelta(days=1)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    ticker: str = 'MIX'
    path_db: Path = Path(fr'c:\Users\Alkor\gd\data_quote_db\{ticker}_futures_day.db')
    start_date: date = datetime.strptime('2015-01-01', "%Y-%m-%d").date()

    connection: Any = sqlite3.connect(path_db, check_same_thread=True)
    cursor: Any = connection.cursor()

    # Если таблица Futures не пустая
    if sqlighter3_MIX_day.non_empty_table_futures(connection, cursor):
        # Меняем стартовую дату на дату последней записи
        start_date = datetime.strptime(
            sqlighter3_MIX_day.get_max_date_futures(connection, cursor), "%Y-%m-%d"
        ).date()

    get_future_date_results(start_date, ticker)

    # with connection:
    #     cursor.execute(
    #         "DELETE FROM Day WHERE TRADEDATE >= '2016-12-15' AND TRADEDATE <= '2016-12-28'"
    #     )
    #     cursor.execute('DELETE FROM Day WHERE TRADEDATE > LSTTRADE')

    # Выполняем команду VACUUM
    cursor.execute("VACUUM;")

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()
