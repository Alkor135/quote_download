import sqlite3
from pathlib import Path

# Путь к базе данных
path_db = Path(r'c:\Users\Alkor\gd\data_quote_db\RTS_futures_options_day_2014.db')

# Подключение к базе данных
connection = sqlite3.connect(path_db)
cursor = connection.cursor()

# Удаление всех записей из таблицы Options
cursor.execute("DELETE FROM Options")

# Подтверждение изменений
connection.commit()

# Закрытие соединения
cursor.close()
connection.close()

print("Таблица Options очищена от всех значений.")
