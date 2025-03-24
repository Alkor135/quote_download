"""
Получение исторических данных по опционам RTS с MOEX ISS API
"""
from pathlib import Path
import requests
from datetime import timedelta, datetime
import pandas as pd
import sqlite3
import sqlighter3_RTS_day


def request_moex(session, url, retries=3, timeout=5):
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


def get_info_security(session, security: str):
    """Запрашивает у MOEX информацию по инструменту"""
    url = f'https://iss.moex.com/iss/securities/{security}.json'
    j = request_moex(session, url)

    if not j:
        return pd.Series([float('nan'), '2130-01-01', float('nan'), float('nan')])

    data = [{k: r[i] for i, k in enumerate(j['description']['columns'])} for r in
            j['description']['data']]
    df = pd.DataFrame(data)

    if 'LSTTRADE' in df['name'].values:
        row_lst = ['NAME', 'LSTTRADE', 'OPTIONTYPE', 'STRIKE']
        df.loc[df['name'] == 'NAME', 'value'] = df[df['name'] == 'NAME']['value'].iloc[
            0].split().pop()
        df = df[df['name'].isin(row_lst)]
        return pd.Series(df['value'].tolist())

    return pd.Series([float('nan'), '2130-01-01', float('nan'), float('nan')])


def get_options_date_results(session, tradedate, shortname):
    df_rez = pd.DataFrame()
    page = 0

    while True:
        url = (
            f'https://iss.moex.com/iss/history/engines/futures/markets/options/'
            f'securities.json?date={tradedate}&assetcode=RTS&start={page}'
        )
        print(f'{url=}')
        j = request_moex(session, url)

        if not j or 'history' not in j or not j['history'].get('data'):
            break

        data = [{k: r[i] for i, k in enumerate(j['history']['columns'])} for r in
                j['history']['data']]
        df = pd.DataFrame(data)

        if df.empty:
            break

        df = df[["TRADEDATE", "SECID", "OPENPOSITION"]]  # .dropna()
        df[['NAME', 'LSTTRADE', 'OPTIONTYPE', 'STRIKE']] = df.apply(
            lambda x: get_info_security(session, x['SECID']), axis=1
        )

        df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"], errors='coerce')
        df = df[(df['LSTTRADE'] > tradedate) & (df['NAME'] == shortname)]  # .dropna()

        df['OPENPOSITION'] = df['OPENPOSITION'].fillna(0).astype(int)
        df_rez = pd.concat([df_rez, df], ignore_index=True)

        print(df_rez.to_string(max_rows=6, max_cols=18), '\n')
        page += 100

    return df_rez


def add_row_options_table(connection, cursor, df):
    """Записывает DataFrame в БД"""
    df['OPENPOSITION'] = df['OPENPOSITION'].fillna(0).astype(int)

    for row in df.itertuples():
        sqlighter3_RTS_day.add_tradedate_option(
            connection,
            cursor,
            row.TRADEDATE,
            row.SECID,
            row.OPENPOSITION,
            row.NAME,
            row.LSTTRADE.date(),
            row.OPTIONTYPE,
            row.STRIKE
        )
    print('Опционы за дату записаны в БД.')


if __name__ == '__main__':
    ticker = 'RTS'
    path_db = Path(r'c:\Users\Alkor\gd\data_quote_db\RTS_futures_options_day_2014.db')
    start_date = datetime.strptime('2021-01-01', "%Y-%m-%d").date()

    connection = sqlite3.connect(path_db, check_same_thread=True)
    cursor = connection.cursor()

    # Если таблица Options не пустая
    if sqlighter3_RTS_day.non_empty_table_options(connection, cursor):
        # Удаляем последние записи из БД с опционами
        cursor.execute("SELECT MAX(TRADEDATE) FROM Options")
        max_trade_date = cursor.fetchone()[0]
        if max_trade_date:
            cursor.execute("DELETE FROM Options WHERE TRADEDATE = ?", (max_trade_date,))
            connection.commit()
        # Меняем стартовую дату на дату последней записи плюс 1 день
        start_date = datetime.strptime(sqlighter3_RTS_day.get_max_date_options(connection, cursor),
                                       "%Y-%m-%d").date() + timedelta(days=1)

    df_tradedate = sqlighter3_RTS_day.get_tradedate_future_update(connection, start_date)
    df_tradedate.sort_values(by='TRADEDATE', inplace=True)
    # print(df_tradedate)

    with requests.Session() as session:
        for row in df_tradedate.itertuples():
            print(f'\nИндекс={row.Index}, TRADEDATE={row.TRADEDATE}, SHORTNAME={row.SHORTNAME}')
            if not sqlighter3_RTS_day.tradedate_options_exists(connection, cursor, row.TRADEDATE):
                df = get_options_date_results(session, row.TRADEDATE, row.SHORTNAME)
                if not df.empty:
                    add_row_options_table(connection, cursor, df)

    cursor.execute("VACUUM;")
    cursor.close()
    connection.close()
