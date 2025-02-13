"""
Получение исторических минутных данных по фьючерсам BR с MOEX ISS API и занесение записей в БД
Загружать от 2015-01-01
"""

from pathlib import Path
import sqlite3
from datetime import datetime, timedelta, date
from typing import Any

import requests
import apimoex
import pandas as pd

import sqlighter3_BR_minute  # !!!


def get_info_future(session: Any, security: str):
    """
    Запрашивает у MOEX информацию по инструменту
    :param session: Подключение к MOEX
    :param security: Тикер инструмента
    :return: Дата последних торгов (если её нет то возвращает дату удаления тикера, если её нет 
    то возвращает 2130.01.01), короткое имя
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


def get_future_date_results(tradedate, tiker, session):
# def get_future_date_results(tradedate, tiker):

    arguments = {'securities.columns': (
        "BOARDID, TRADEDATE, SECID, OPEN, LOW, HIGH, CLOSE, OPENPOSITIONVALUE, VALUE, "
        "VOLUME, OPENPOSITION, SETTLEPRICE"
        )}

    # with requests.Session() as session:
    #     # print(f'{trade_date=}, {start_date=}')

    # Нет записи с такой датой
    # if not sqlighter3_MIX_minute.tradedate_futures_exists(connection, cursor, tradedate):
    request_url = (
        f'http://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?'
        f'date={tradedate.strftime("%Y-%m-%d")}&assetcode={tiker}'
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
        # Убираем строки где дата последних торгов больше даты экспирации
        df = df.loc[df['LSTTRADE'] >= tradedate]
        # Удаление строк с пустыми OPEN, LOW, HIGH, CLOSE
        df.dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'], inplace=True)
        # Выборка строк с минимальной датой
        df = (df[df['LSTTRADE'] == df['LSTTRADE'].min()]).reset_index(drop=True)
        # print(df.to_string(max_rows=20, max_cols=15), '\n')

        if len(df) != 0:  # Если полученный ответ не нулевой, чтобы исключить выходные дни
            cur_ticker = df.loc[0]['SECID']  # Тикер текущего фьючерса
            cur_lasttrade = df.loc[0]['LSTTRADE']  # Тикер текущего фьючерса
            data = apimoex.get_market_candles(
                session=session,
                security=cur_ticker,
                market="forts",
                engine="futures",
                interval=1,
                start=tradedate.strftime("%Y-%m-%d"),
                end=tradedate.strftime("%Y-%m-%d"),
                columns=('begin', 'open', 'close', 'high', 'low', 'volume',),
            )
            df = pd.DataFrame(data)
            if len(df) != 0:  # Если полученный ответ не нулевой, чтобы исключить выходные дни
                df['secid'] = cur_ticker
                df['lsttrade'] = cur_lasttrade
                add_row(connection, cursor, df)
                print(df.to_string(max_rows=10, max_cols=15), '\n')


def add_row(connection, cursor, df):
    """
    Функция передает DF построчно для записи в БД
    """
    # print(df.to_string(max_rows=20, max_cols=15), '\n')
    for row in df.itertuples():  # Перебираем опционы для занесения в БД
        sqlighter3_BR_minute.add_row(
            connection, cursor, row[1], row[7], row[2], row[5], row[4], row[3], row[6], row[8]
            )


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    tiker: str = 'BR'
    path_db: Path = Path(fr'c:\Users\Alkor\gd\data_quote_db\{tiker}_futures_minute.db')
    tradedate: date = datetime.strptime('2015-01-01', "%Y-%m-%d").date()  # Дата для запроса

    connection: Any = sqlite3.connect(path_db, check_same_thread=True)
    cursor: Any = connection.cursor()

    # Если таблица Futures не пустая
    if sqlighter3_BR_minute.non_empty_table_futures(connection, cursor):  
        # Меняем стартовую дату на дату последней записи +1 день
        tradedate = datetime.strptime(
            sqlighter3_BR_minute.get_max_date_futures(connection, cursor), "%Y-%m-%d %H:%M:%S"
            ).date() + timedelta(days=1)

    today_date = datetime.now().date()  # Текущая дата

    with requests.Session() as session:
        # print(f'{trade_date=}, {start_date=}')

        while tradedate != today_date:
            get_future_date_results(tradedate, tiker, session)
            tradedate += timedelta(days=1)
            # print(df.to_string(max_rows=20, max_cols=15), '\n')

    # get_future_date_results(datetime.strptime('2023-12-06', "%Y-%m-%d").date(), tiker)
