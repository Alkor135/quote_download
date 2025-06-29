import requests
import pandas as pd

url = 'https://iss.moex.com/iss/engines/stock/markets/index/securities/RTSI/candles.json'
params = {
    'from': '2010-01-01',
    'till': '2024-04-01',
    'interval': 24,  # Дневные свечи
    # 'limit': 100,
    'start': 0
}

all_data = []

while True:
    response = requests.get(url, params=params)
    data = response.json()

    if 'candles' not in data or not data['candles']['data']:
        break

    columns = data['candles']['columns']
    rows = data['candles']['data']
    all_data.extend(rows)

    # Переход к следующей странице
    params['start'] += len(rows)

# Собираем DataFrame
df = pd.DataFrame(all_data, columns=columns)

# Оставим нужные колонки
print(df[['begin', 'open', 'high', 'low', 'close']])

