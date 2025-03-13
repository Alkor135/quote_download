import time
import pandas as pd
import requests


# Настройки для отображения широкого df pandas
pd.options.display.width = 1200
pd.options.display.max_colwidth = 100
pd.options.display.max_columns = 100

# j = requests.get('https://iss.moex.com/iss/securities/YNDX/aggregates.json?date=2022-09-21').json()
# data = [{k : r[i] for i, k in enumerate(j['aggregates']['columns'])} for r in j['aggregates']['data']]
# print(pd.DataFrame(data))

j = requests.get('https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?date=2025-02-19&assetcode=RTS').json()
data = [{k : r[i] for i, k in enumerate(j['history']['columns'])} for r in j['history']['data']]
df = pd.DataFrame(data).dropna()
print(df)

# time.sleep(2)
# j = requests.get('http://iss.moex.com/iss/engines/stock/markets/shares/securities/YNDX/candles.json?from=2023-05-25&till=2023-09-01&interval=24').json()
# data = [{k : r[i] for i, k in enumerate(j['candles']['columns'])} for r in j['candles']['data']]
# frame = pd.DataFrame(data)
# print(frame)
# plt.plot(list(frame['close']))
# plt.savefig("shares.png")

j = requests.get('https://iss.moex.com/iss/securities/RIH5.json').json()
data = [{k : r[i] for i, k in enumerate(j['description']['columns'])} for r in j['description']['data']]
df = pd.DataFrame(data)  # .dropna()
print(df)
