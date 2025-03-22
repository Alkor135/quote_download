"""
Получение исторических данных по опционам RTS с MOEX ISS API
После формирования базы удалить строк из БД командой:
DELETE FROM Options WHERE TRADEDATE > LSTTRADE
"""
from pathlib import Path
import requests
from datetime import date, datetime
from typing import Any
import pandas as pd
import sqlite3
import sqlighter3_RTS_day


def get_info_security(security: str):
    """Запрашивает у MOEX информацию по инструменту"""
    url = f'https://iss.moex.com/iss/securities/{security}.json'
    j = requests.get(url).json()
    data = [{k: r[i] for i, k in enumerate(j['description']['columns'])} for r in j['description']['data']]
    df = pd.DataFrame(data)

    name_lst = list(df['name'])
    if 'LSTTRADE' in name_lst:
        row_lst = ['NAME', 'LSTTRADE', 'OPTIONTYPE', 'STRIKE']  # Список необходимых строк из DF
        # Меняем значение в ячейке ['NAME', 'value'] на конец строки 'value',
        # где прописан код фьючерса
        df.loc[df[df['name'] == 'NAME'].index, 'value'] = list(
            df.loc[df[df['name'] == 'NAME'].index]['value']
        )[0].split().pop()
        df = df[df['name'].isin(row_lst)]  # Выборка необходимых строк
        rez_lst: list = list(df.value)  # Колонку 'value' в список
        return pd.Series(rez_lst)
    else:
        return pd.Series([float('nan'), '2130-01-01', float('nan'), float('nan')])


def get_options_date_results(tradedate, shortname):
    df_rez = pd.DataFrame()
    page = 0  # С какой записи стартовать запрос
    while True:  # В цикле отправляем запрос постранично и обрабатываем ответ
        url = (
            f'https://iss.moex.com/iss/history/engines/futures/markets/options/'
            f'securities.json?date={tradedate}&assetcode=RTS&start={page}'
        )
        print(f'{url=}')
        j = requests.get(url).json()

        data = [{k: r[i] for i, k in enumerate(j['history']['columns'])} for r in
                j['history']['data']]
        df = pd.DataFrame(data)
        # print(df.to_string(max_rows=20, max_cols=20))

        if len(df) == 0:  # Больше нет страниц в ответе
            break
        else:
            df = df[["TRADEDATE", "SECID", "OPENPOSITION"]]  # Оставляем нужные поля
            # Создаем новые колонки 'NAME', 'LSTTRADE', 'OPTIONTYPE', 'STRIKE' и заполняем
            df[['NAME', 'LSTTRADE', 'OPTIONTYPE', 'STRIKE']] = df.apply(
                lambda x: get_info_security(x['SECID']), axis=1)
            # print(df.to_string(max_rows=20, max_cols=15), '\n')
            # Меняем формат колонки на дату
            df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"])
            df = df.loc[df['LSTTRADE'] > tradedate]
            df = df.loc[df['NAME'] == shortname]  # Выбор опционов текущего базового актива
            # Заполняем пропущенные значения в столбце OPENPOSITION значением 0.0
            df['OPENPOSITION'] = df['OPENPOSITION'].fillna(0.0)
            # Выборка строк с минимальной датой
            # df = df[df['LSTTRADE'] == df['LSTTRADE'].min()]
            # df_rez = pd.concat([df_rez, df]).reset_index(drop=True)  # Слияние DF
            df_rez = pd.concat([df_rez.dropna(), df.dropna()]).reset_index(drop=True)
            print(df_rez.to_string(max_rows=6, max_cols=18), '\n')
            page += 100  # для запроса следующей страницы со 100 записями

    return df_rez


def add_row_options_table(connection, cursor, df):
    """
    Функция преобразует поле 'OPENPOSITION' в int и передает DF построчно для записи в БД
    """
    df['OPENPOSITION'] = df['OPENPOSITION'].fillna(0.0)  # Nan в 0.0
    df['OPENPOSITION'] = df['OPENPOSITION'].astype(int)  # float в int

    for row in df.itertuples():  # Перебираем опционы для занесения в БД
        sqlighter3_RTS_day.add_tradedate_option(
            connection,
            cursor,
            row.TRADEDATE,
            row.SECID,
            int(row.OPENPOSITION),
            row.NAME,
            row.LSTTRADE.date(),
            row.OPTIONTYPE,
            row.STRIKE
        )
    print('Опционы за дату записаны в БД.')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    ticker = 'RTS'
    path_db = Path(fr'c:\Users\Alkor\gd\data_quote_db\{ticker}_futures_options_day_2014.db')
    start_date: date = datetime.strptime('2014-01-01', "%Y-%m-%d").date()

    connection: Any = sqlite3.connect(path_db, check_same_thread=True)
    cursor: Any = connection.cursor()

    # Удаляем последние записи из БД с опционами
    cursor.execute("SELECT MAX(TRADEDATE) FROM Options")
    max_trade_date = cursor.fetchone()[0]
    if max_trade_date:
        cursor.execute("DELETE FROM Options WHERE TRADEDATE = ?", (max_trade_date,))
        connection.commit()

    # Получаем в DF данные по фьючерсам из БД
    df_tradedate = sqlighter3_RTS_day.get_tradedate_future_update(connection, start_date)
    df_tradedate.sort_values(by='TRADEDATE')
    df = pd.DataFrame()

    # Перебираем даты для запроса торгуемых опционов на эту дату
    for row in df_tradedate.itertuples():
        print(f'\nИндекс={row.Index}, TRADEDATE={row.TRADEDATE}, SHORTNAME={row.SHORTNAME}')
        # Нет записи с такой датой
        if not sqlighter3_RTS_day.tradedate_options_exists(connection, cursor, row.TRADEDATE):
            # Получаем DF по опционам от МОЕХ
            df = get_options_date_results(row.TRADEDATE, row.SHORTNAME)
            # Записываем в БД построчно DF по опционам
            add_row_options_table(connection, cursor, df)

    # Выполняем команду VACUUM
    cursor.execute("VACUUM;")

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()
