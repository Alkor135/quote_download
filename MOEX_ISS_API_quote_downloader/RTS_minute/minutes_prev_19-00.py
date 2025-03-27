import requests
from datetime import datetime
import pandas as pd


def request_moex(session, url, retries = 3, timeout = 5):
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


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    today_date = datetime.now().date()  # Текущая дата
    ticker = 'RTS'

    url = (
        f"https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
    )
    with requests.Session() as session:
        j = request_moex(session, url)
        if not j or 'securities' not in j or not j['securities'].get('data'):
            print(f"Нет данных для {today_date}")
        data = [{k: r[i] for i, k in enumerate(j['securities']['columns'])} for r in
                j['securities']['data']]

    # DF со всеми фьючерсами на сегодня -----------------------------------------------------------
    df = pd.DataFrame(data).drop(['BOARDID', 'SECNAME', 'DECIMALS', 'LOTVOLUME', 'PREVOPENPOSITION'], axis=1)
    print(df.to_string(max_rows=20, max_cols=30), '\n')

    # DF со всеми фьючерсами RTS ------------------------------------------------------------------
    today_date = pd.to_datetime(today_date)  # Преобразование today_date в тип datetime
    df["LASTTRADEDATE"] = pd.to_datetime(df["LASTTRADEDATE"], errors='coerce')
    df = df.loc[df['ASSETCODE'] == f'{ticker}'].sort_values('LASTTRADEDATE').copy()
    print(df.to_string(max_rows=20, max_cols=30), '\n')

    # Фьючерсы с датой экспирации не сегодня ------------------------------------------------------
    df = df[df['LASTTRADEDATE'] > today_date]
    print(df.to_string(max_rows=20, max_cols=30), '\n')

    # Тикер ближайшего фьючерса RTS ---------------------------------------------------------------
    secid = df.iloc[0, 0]
    print(secid)

    # Все минутные данные фьючерса с ближайшим истечением -----------------------------------------
    df_min = pd.DataFrame()
    page = 0
    while True:
        url = (
            f"https://iss.moex.com/iss/engines/futures/markets/forts/securities/{secid}/candles.json?interval=1&start={page}"
        )
        print(f'{url=}')
        j = request_moex(session, url)

        if not j or 'candles' not in j or not j['candles'].get('data'):
            break

        data = [{k: r[i] for i, k in enumerate(j['candles']['columns'])} for r in
                j['candles']['data']]
        df = pd.DataFrame(data)

        if df.empty:
            break

        # print(df_min)
        # print(df)
        df_min = pd.concat([df_min, df], ignore_index=True)

        # print(df_min.to_string(max_rows=6, max_cols=18), '\n')
        page += 500

    print(df_min)

    # Минутные данные с 19:00 прошлого торгового дня ----------------------------------------------
    df_min['begin'] = pd.to_datetime(df_min['begin'])  # Преобразуем колонку begin в формат datetime
    # Находим прошлый торговый день
    last_trading_day = df_min[df_min['begin'] < today_date]['begin'].dt.normalize().max()

    # Устанавливаем критерий времени 19:00:00
    if pd.notna(last_trading_day):  # Если прошлый торговый день найден
        start_time = pd.Timestamp(f"{last_trading_day} 19:00:00")

        # Фильтруем строки начиная с 19:00 прошлого торгового дня
        df_filtered = df_min[df_min['begin'] >= start_time]
        print(df_filtered)
    else:
        print("Прошлый торговый день не найден.")

    # Данные для прогноза направления LSTM модели. ------------------------------------------------
    # Получаем необходимые значения
    max_high = df_filtered['high'].max()  # Максимальное значение столбца 'high'
    min_low = df_filtered['low'].min()  # Минимальное значение столбца 'low'
    first_open = df_filtered['open'].iloc[0]  # Первое значение столбца 'open'
    last_close = df_filtered['close'].iloc[-1]  # Последнее значение столбца 'close'

    # Вывод результатов
    print(f"Максимальное значение 'high': {max_high}")
    print(f"Минимальное значение 'low': {min_low}")
    print(f"Первое значение 'open': {first_open}")
    print(f"Последнее значение 'close': {last_close}")
