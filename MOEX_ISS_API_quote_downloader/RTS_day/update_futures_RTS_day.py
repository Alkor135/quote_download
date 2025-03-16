from pathlib import Path
import requests
from datetime import datetime, timedelta, date
import pandas as pd
import sqlite3
import sqlighter3_RTS_day


def request_moex(session, url: str, retries: int = 3, timeout: int = 5):
    """Функция запроса данных с повторными попытками"""
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Ошибка запроса {url} (попытка {attempt + 1}): {e}")
            if attempt == retries - 1:
                return None


def get_info_future(session, security: str):
    """
    Запрашивает у MOEX информацию по инструменту
    """
    url = f'https://iss.moex.com/iss/securities/{security}.json'
    j = request_moex(session, url)
    if not j:
        return pd.Series([None, '2130-01-01'])

    data = [{k: r[i] for i, k in enumerate(j['description']['columns'])} for r in
            j['description']['data']]
    df = pd.DataFrame(data)
    df.drop(df[df['name'] == 'DELIVERYTYPE'].index, inplace=True, errors='ignore')

    shortname = df.loc[df['name'] == 'SHORTNAME', 'value'].values[0] if 'SHORTNAME' in df[
        'name'].values else None
    lsttrade = df.loc[df['name'] == 'LSTTRADE', 'value'].values[0] if 'LSTTRADE' in df[
        'name'].values else \
        df.loc[df['name'] == 'LSTDELDATE', 'value'].values[0] if 'LSTDELDATE' in df[
            'name'].values else '2130-01-01'

    return pd.Series([shortname, lsttrade])


def get_future_date_results(session, tradedate: date, ticker: str, connection, cursor):
    today_date = datetime.now().date()
    while tradedate <= today_date:
        if not sqlighter3_RTS_day.tradedate_futures_exists(connection, cursor, tradedate):
            url = (
                f'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?'
                f'date={tradedate}&assetcode={ticker}'
            )
            print(url)
            j = request_moex(session, url)
            if not j or 'history' not in j or not j['history'].get('data'):
                print(f"Нет данных для {tradedate}")
                tradedate += timedelta(days=1)
                continue

            data = [{k: r[i] for i, k in enumerate(j['history']['columns'])} for r in
                    j['history']['data']]
            df = pd.DataFrame(data).dropna()

            df[['SHORTNAME', 'LSTTRADE']] = df.apply(
                lambda x: get_info_future(session, x['SECID']), axis=1)
            df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"], errors='coerce').dt.date
            df = df[df['LSTTRADE'] >= tradedate].dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'])
            df = df[df['LSTTRADE'] == df['LSTTRADE'].min()].reset_index(drop=True)

            if len(df) == 1 and not df['OPEN'].isnull().values.any():
                sqlighter3_RTS_day.add_tradedate_future(
                    connection, cursor, df.loc[0]['TRADEDATE'], df.loc[0]['SECID'],
                    float(df.loc[0]['OPEN']), float(df.loc[0]['LOW']),
                    float(df.loc[0]['HIGH']), float(df.loc[0]['CLOSE']),
                    int(df.loc[0]['VOLUME']), int(df.loc[0]['OPENPOSITION']),
                    df.loc[0]['SHORTNAME'], df.loc[0]['LSTTRADE']
                )
                print(df.to_string(max_rows=20, max_cols=15))
                print('Строка записана в БД', '\n')
        tradedate += timedelta(days=1)


if __name__ == '__main__':
    ticker = 'RTS'
    path_db = Path(fr'c:\Users\Alkor\gd\data_quote_db\{ticker}_futures_day.db')
    start_date = datetime.strptime('2015-01-01', "%Y-%m-%d").date()

    connection = sqlite3.connect(path_db, check_same_thread=True)
    cursor = connection.cursor()

    cursor.execute("SELECT MAX(TRADEDATE) FROM Day")
    max_trade_date = cursor.fetchone()[0]
    if max_trade_date:
        cursor.execute("DELETE FROM Day WHERE TRADEDATE = ?", (max_trade_date,))
        connection.commit()

    if sqlighter3_RTS_day.non_empty_table_futures(connection, cursor):
        start_date = datetime.strptime(sqlighter3_RTS_day.get_max_date_futures(connection, cursor),
                                       "%Y-%m-%d").date() + timedelta(days=1)

    with requests.Session() as session:
        get_future_date_results(session, start_date, ticker, connection, cursor)

    cursor.execute("VACUUM;")
    cursor.close()
    connection.close()
