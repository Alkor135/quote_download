# Удаление всех строк в БД начиная с определенной даты
import sqlite3
from pathlib import Path

# Путь к базе данных
path_db = Path(fr'c:\Users\Alkor\gd\data_quote_db\RTS_day_2014.db')

# Подключение к базе данных
conn = sqlite3.connect(path_db)
cursor = conn.cursor()

# Определите дату, начиная с которой нужно удалить строки
date_to_delete_from = '2025-04-15'  # Пример даты

# SQL запрос для удаления строк
delete_query = f"""
DELETE FROM Futures
WHERE TRADEDATE >= '{date_to_delete_from}'
"""

# Выполнение запроса
cursor.execute(delete_query)
conn.commit()

# Закрытие соединения
conn.close()
