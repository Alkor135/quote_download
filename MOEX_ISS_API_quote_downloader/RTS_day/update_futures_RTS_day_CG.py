"""
Получение исторических данных по фьючерсам RTS с MOEX ISS API и занесение записей в БД.
Загружать от 2015-01-01.
Загружает и последний день торгов фьючерса, при этом OPENPOSITION равен 0.
"""

from pathlib import Path
import requests
from datetime import datetime, timedelta, date
from typing import Any

import pandas as pd
import sqlite3

import sqlighter3_RTS_day


def request_moex(url: str, retries: int = 3, timeout: int = 5):
    """Функция запроса данных с повторными попытками"""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Проверяем ошибки HTTP
            return response
            # return response.json()
        except requests.RequestException as e:
            print(f"Ошибка запроса {url} (попытка {attempt + 1}): {e}")
            if attempt == retries - 1:
                return None


def get_info_future(security: str):
    """Получает информацию по инструменту (дата последних торгов, короткое имя)"""
    url = f'https://iss.moex.com/iss/securities/{security}.json'
    j = request_moex(url).json()

    if not j or 'description' not in j or 'data' not in j['description']:
        print(f"Ошибка получения данных для {security}")
        return pd.Series([None, '2130-01-01'])

    data = [{k: r[i] for i, k in enumerate(j['description']['columns'])} for r in
            j['description']['data']]
    df = pd.DataFrame(data).dropna()

    name_lst = list(df['name'])  # Поле 'name' в список

    shortname = df.loc[df['name'] == 'SHORTNAME', 'value'].values[
        0] if 'SHORTNAME' in name_lst else None
    lsttrade = df.loc[df['name'] == 'LSTTRADE', 'value'].values[0] if 'LSTTRADE' in name_lst else \
        df.loc[df['name'] == 'LSTDELDATE', 'value'].values[
            0] if 'LSTDELDATE' in name_lst else '2130-01-01'

    return pd.Series([shortname, lsttrade])


def get_future_date_results(tradedate: date, ticker: str):
    today_date = datetime.now().date()

    while tradedate <= today_date:
        if not sqlighter3_RTS_day.tradedate_futures_exists(connection, cursor, tradedate):
            url = (
                f'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?'
                f'date={tradedate}&assetcode={ticker}'
            )
            print(url)
            j = request_moex(url).json()

            if not j or 'history' not in j or 'data' not in j['history']:
                print(f"Нет данных для {tradedate}")
                tradedate += timedelta(days=1)
                continue

            data = [{k: r[i] for i, k in enumerate(j['history']['columns'])} for r in j['history']['data']]
            df = pd.DataFrame(data).dropna()

            if df.empty:
                tradedate += timedelta(days=1)
                continue

            # Добавляем информацию о фьючерсе
            df[['SHORTNAME', 'LSTTRADE']] = df.apply(lambda x: get_info_future(x['SECID']), axis=1)
            df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"]).dt.date

            df = df[df['LSTTRADE'] >= tradedate].dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'])

            if not df.empty:
                df = df[df['LSTTRADE'] == df['LSTTRADE'].min()].reset_index(drop=True)

                if len(df) == 1 and not df['OPEN'].isnull().values.any():
                    try:
                        sqlighter3_RTS_day.add_tradedate_future(
                            connection, cursor, df.loc[0]['TRADEDATE'], df.loc[0]['SECID'],
                            float(df.loc[0]['OPEN']), float(df.loc[0]['LOW']),
                            float(df.loc[0]['HIGH']), float(df.loc[0]['CLOSE']),
                            int(df.loc[0]['VOLUME']), int(df.loc[0]['OPENPOSITION']),
                            df.loc[0]['SHORTNAME'], df.loc[0]['LSTTRADE']
                        )
                        print(df.to_string(max_rows=20, max_cols=15))
                        print(f"Записана дата {df.loc[0]['TRADEDATE']}\n")
                    except Exception as e:
                        print(f"Ошибка записи в БД: {e}\n")

        tradedate += timedelta(days=1)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    ticker: str = 'RTS'
    path_db: Path = Path(fr'c:\Users\Alkor\gd\data_quote_db\{ticker}_futures_day.db')
    start_date: date = datetime.strptime('2015-01-01', "%Y-%m-%d").date()

    connection: Any = sqlite3.connect(path_db, check_same_thread=True)
    cursor: Any = connection.cursor()

    # === Удаление из БД строки с максимальной датой (значения могут быть не на конец дня) ===
    cursor.execute("SELECT MAX(TRADEDATE) FROM Day")
    max_trade_date = cursor.fetchone()[0]

    if max_trade_date:
        cursor.execute("DELETE FROM Day WHERE TRADEDATE = ?", (max_trade_date,))
        connection.commit()

    # === Загрузка данных ===
    # Если таблица Futures не пустая
    if sqlighter3_RTS_day.non_empty_table_futures(connection, cursor):
        # Меняем стартовую дату на дату последней записи плюс 1 день
        start_date = datetime.strptime(
            sqlighter3_RTS_day.get_max_date_futures(connection, cursor), "%Y-%m-%d"
        ).date() + timedelta(days=1)

    get_future_date_results(start_date, ticker)

    # Выполняем команду VACUUM
    cursor.execute("VACUUM;")

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()
