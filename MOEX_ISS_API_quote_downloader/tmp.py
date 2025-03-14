"""

"""
import pandas as pd
import requests


# Настройки для отображения широкого df pandas
pd.options.display.width = 1200
pd.options.display.max_colwidth = 100
pd.options.display.max_columns = 100

j = requests.get('https://iss.moex.com/iss/securities/YNDX/aggregates.json?date=2022-09-21').json()
data = [{k : r[i] for i, k in enumerate(j['aggregates']['columns'])} for r in j['aggregates']['data']]
print(pd.DataFrame(data), '\n')

j = requests.get('https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?date=2025-02-19&assetcode=RTS').json()
data = [{k : r[i] for i, k in enumerate(j['history']['columns'])} for r in j['history']['data']]
df = pd.DataFrame(data).dropna()
print(df, '\n')

j = requests.get('https://iss.moex.com/iss/securities/RIH5.json').json()
data = [{k : r[i] for i, k in enumerate(j['description']['columns'])} for r in j['description']['data']]
df = pd.DataFrame(data)
print(df, '\n')
