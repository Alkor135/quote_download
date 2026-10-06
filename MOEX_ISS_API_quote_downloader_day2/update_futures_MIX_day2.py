r"""Загрузка двух ближайших фьючерсов MIX за каждый день с 01.01.2018.

MOEX ISS возвращает дневную историю; описания контрактов дают SHORTNAME и
LSTTRADE. Выбираются два контракта с минимальными LSTTRADE >= TRADEDATE.
SECID сохраняется целиком из ISS, включая архивный суффикс вроде _2018.
Календарные спреды, в том числе на RFUD с ASSETCODE=NULL, исключаются после
получения всех страниц: SECID спреда должен состоять из кодов двух разных
контрактов того же актива, присутствующих в ответе. У остальных фьючерсов
ASSETCODE обязателен; неопознанные строки с NULL вызывают ошибку.
При NUMTRADES=0, пустых OHLC и VOLUME NULL/0 в OHLC переносится последнее
доступное закрытие того же SECID из Day до даты торгов; VOLUME становится 0.
OPENPOSITION берётся из текущего ответа ISS. Без предыдущей цены или текущего
OPENPOSITION подстановка запрещена. Операция отмечается в FilledQuotes вместе
с исходной строкой ISS. Две полные строки и отметки записываются атомарно в SQLite.
По умолчанию БД: C:\data_quote\day2\MIX_day2.db; конец периода — вчера
по московскому времени. При каждом запуске проверяется весь заданный период:
Даты с полной парой пропускаются. Подтверждённые пустые субботы и воскресенья
сохраняются в EmptyWeekends и также пропускаются при повторных запусках.
Для их перепроверки служит --recheck-empty-weekends. Пустые будние дни и дни
с неполными котировками проверяются снова; описания кешируются в Contracts.
Текущий день и неполные пары не записываются. Дополнительные пакеты не нужны.
Журналы UTF-8 перезаписываются в logs при каждом запуске загрузки:
MIX_day2.log содержит
ход загрузки, MIX_day2_errors.log — предупреждения, ошибки и исходные данные ISS.
Старые резервные копии этих журналов удаляются. Внутри одного запуска действует
ротация при 5 МиБ: до трёх резервных копий содержат только данные этого запуска.
При отказе проверки истории в журнале сохраняются дата, SECID, URL и исходный JSON.
Для неполной пары сохраняются NUMTRADES и SETTLEPRICE; SETTLEPRICE не подставляется
в OHLC. Сохранённые подстановки перепроверяются с --recheck-filled.

Примеры запуска из папки этого скрипта:
    python update_futures_MIX_day2.py
    python update_futures_MIX_day2.py --start 2018-01-01 --end 2018-12-31
    python update_futures_MIX_day2.py --db C:\data_quote\day2\MIX_day2.db
    python update_futures_MIX_day2.py --timeout 30 --retries 5 --pause 0.1
    python update_futures_MIX_day2.py --log-dir C:\data_quote\day2\logs
    python update_futures_MIX_day2.py --recheck-empty-weekends
    python update_futures_MIX_day2.py --recheck-filled
    python update_futures_MIX_day2.py --help
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from http.client import HTTPException
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


ASSETCODE = "MIX"
DEFAULT_DB = Path(r"C:\data_quote\day2\MIX_day2.db")
DEFAULT_LOG_DIR = Path(__file__).resolve().parent / "logs"
FIRST_DATE = date(2018, 1, 1)
MOSCOW = timezone(timedelta(hours=3), name="Europe/Moscow")
ISS_BASE = "https://iss.moex.com/iss"
HISTORY_PATH = "/history/engines/futures/markets/forts/securities.json"
COLUMNS = ("TRADEDATE", "SECID", "OPEN", "LOW", "HIGH", "CLOSE", "VOLUME",
           "OPENPOSITION", "SHORTNAME", "LSTTRADE")
LOGGER = logging.getLogger(__name__)


class DataError(RuntimeError):
    """Обозначает неполные или несовместимые данные, которые нельзя записывать."""

    def __init__(self, message: str, records: list[dict[str, Any]] | None = None):
        """Сохраняет message и необязательные исходные records для журнала диагностики."""
        super().__init__(message)
        self.records = records if records is not None else []


class ISSRequestError(RuntimeError):
    """Обозначает сетевой отказ либо некорректный ответ ISS вместо таблицы."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        """Сохраняет message и необязательные details с исходным ответом и контекстом запроса для журнала."""
        super().__init__(message)
        self.details = details


@dataclass
class DownloadResult:
    """Содержит счётчики дней загрузки и количества записанных свечей с подставленными OHLC."""

    existing_days: int = 0
    written_days: int = 0
    empty_days: int = 0
    cached_empty_days: int = 0
    failed_days: int = 0
    filled_quotes: int = 0


def yesterday_moscow(now: datetime | None = None) -> date:
    """Возвращает вчерашнюю московскую дату; now — необязательный момент с часовым поясом."""
    instant = now if now is not None else datetime.now(MOSCOW)
    return instant.astimezone(MOSCOW).date() - timedelta(days=1)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Разбирает argv или командную строку и возвращает проверенные параметры запуска."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, default=FIRST_DATE,
                        help="Начало проверяемого периода включительно, ГГГГ-ММ-ДД (2018-01-01).")
    parser.add_argument("--end", type=date.fromisoformat, default=yesterday_moscow(),
                        help="Конец периода включительно, ГГГГ-ММ-ДД (вчера по Москве).")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help=f"Путь SQLite БД (по умолчанию {DEFAULT_DB}).")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR,
                        help="Папка журналов последнего запуска (logs рядом со скриптом); файлы перезаписываются.")
    parser.add_argument("--recheck-empty-weekends", action="store_true",
                        help="Повторно запросить подтверждённые пустые субботы и воскресенья в заданном периоде.")
    parser.add_argument("--recheck-filled", action="store_true",
                        help="Перепроверить все дни с подставленными OHLC в заданном периоде и обновить пару.")
    parser.add_argument("--timeout", type=float, default=20,
                        help="Тайм-аут сетевой операции в секундах (20).")
    parser.add_argument("--retries", type=int, default=4,
                        help="Максимальное число попыток одного запроса, включая первую (4).")
    parser.add_argument("--pause", type=float, default=0.05,
                        help="Пауза после успешного запроса в секундах (0.05).")
    args = parser.parse_args(argv)
    if args.start < FIRST_DATE:
        parser.error("--start должен быть не раньше 2018-01-01.")
    if args.end > yesterday_moscow():
        parser.error("--end должен быть не позже вчерашнего дня по московскому времени.")
    if args.start > args.end:
        parser.error("--start не должен быть позже --end.")
    if not math.isfinite(args.timeout) or args.timeout <= 0 or args.retries < 1:
        parser.error("--timeout должен быть положительным числом, --retries — целым числом от 1.")
    if not math.isfinite(args.pause) or args.pause < 0:
        parser.error("--pause должен быть конечным неотрицательным числом.")
    return args


def close_logging() -> None:
    """Сбрасывает буферы и закрывает обработчики журнала текущего скрипта, освобождая файлы."""
    for handler in LOGGER.handlers[:]:
        LOGGER.removeHandler(handler)
        handler.flush()
        handler.close()


def configure_logging(log_dir: Path) -> tuple[Path, Path]:
    """Очищает два журнала и их старые копии в log_dir, настраивает ротацию по 5 МиБ с тремя копиями; возвращает пути."""
    close_logging()
    log_dir.mkdir(parents=True, exist_ok=True)
    full_path = log_dir / f"{ASSETCODE}_day2.log"
    error_path = log_dir / f"{ASSETCODE}_day2_errors.log"
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(console)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s%(details)s", defaults={"details": ""})
    for path, level in ((full_path, logging.INFO), (error_path, logging.WARNING)):
        for index in range(1, 4):
            path.with_name(f"{path.name}.{index}").unlink(missing_ok=True)
        path.write_text("", encoding="utf-8")
        handler = RotatingFileHandler(path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        handler.setLevel(level)
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)
    return full_path, error_path


def table_rows(payload: dict[str, Any], name: str) -> list[dict[str, Any]]:
    """Преобразует таблицу name из ответа payload в строки; при неверной структуре выдаёт ошибку."""
    table = payload.get(name)
    if not isinstance(table, dict):
        raise ISSRequestError(f"В ответе ISS отсутствует таблица {name}.")
    columns, data = table.get("columns"), table.get("data")
    if (not isinstance(columns, list) or not columns
            or not all(isinstance(column, str) for column in columns)
            or len(set(columns)) != len(columns) or not isinstance(data, list)):
        raise ISSRequestError(f"Некорректная структура таблицы {name}.")
    result = []
    for row in data:
        if not isinstance(row, list) or len(row) != len(columns):
            raise ISSRequestError(f"Некорректная строка таблицы {name}.")
        result.append(dict(zip(columns, row)))
    return result


def is_calendar_spread(secid: str, contract_assets: dict[str, str]) -> bool:
    """Проверяет, составлен ли secid из двух разных кодов одного актива из contract_assets; возвращает bool."""
    for first, asset in contract_assets.items():
        if secid.startswith(first):
            second = secid[len(first):]
            if second and second != first and contract_assets.get(second) == asset:
                return True
    return False


class ISSClient:
    """Получает JSON MOEX ISS с повторами, ограничением частоты и пагинацией."""

    def __init__(self, timeout: float = 20, retries: int = 4, pause: float = 0.05):
        """Сохраняет timeout в секундах, число попыток retries и pause между запросами."""
        self.timeout = timeout
        self.retries = retries
        self.pause = pause

    def get_json(self, path: str, parameters: dict[str, Any]) -> dict[str, Any]:
        """Запрашивает path относительно ISS с parameters; возвращает JSON либо ошибку после повторов."""
        url = ISS_BASE + path + "?" + urlencode({"iss.meta": "off", **parameters})
        request = Request(url, headers={"User-Agent": "MOEX-day2-downloader/1.0",
                                        "Accept": "application/json"})
        for attempt in range(self.retries):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("Корень JSON не является объектом.")
                if self.pause:
                    time.sleep(self.pause)
                return payload
            except (URLError, OSError, HTTPException, ValueError) as error:
                permanent = isinstance(error, HTTPError) and 400 <= error.code < 500 \
                    and error.code not in (408, 429)
                if permanent or attempt + 1 == self.retries:
                    raise ISSRequestError(f"Не удалось получить {url}: {error}") from error
                delay = min(2 ** attempt, 30)
                LOGGER.warning("Запрос не выполнен (%s/%s): %s. Повтор через %s с.",
                               attempt + 1, self.retries, error, delay)
                time.sleep(delay)
        raise ISSRequestError("Не выполнен ни один запрос ISS.")

    def history(self, tradedate: date) -> list[dict[str, Any]]:
        """Возвращает фьючерсы MIX за tradedate без спредов после всех страниц; при отказе прикладывает URL и ответ проблемной страницы."""
        parameters = {
            "date": tradedate.isoformat(), "assetcode": ASSETCODE,
            "iss.only": "history,history.cursor",
            "history.columns": "BOARDID,TRADEDATE,SECID,OPEN,LOW,HIGH,CLOSE,VOLUME,OPENPOSITION,ASSETCODE,NUMTRADES,SETTLEPRICE",
        }
        result, start, previous = [], 0, None
        origins = []
        payload = None
        page_parameters = {**parameters, "start": start}
        try:
            while True:
                page_parameters = {**parameters, "start": start}
                payload = None
                payload = self.get_json(HISTORY_PATH, page_parameters)
                rows = table_rows(payload, "history")
                required = set(parameters["history.columns"].split(","))
                if not required.issubset(payload["history"]["columns"]):
                    raise ISSRequestError("В таблице history отсутствуют запрошенные обязательные столбцы.")
                for row in rows:
                    for column in ("BOARDID", "TRADEDATE", "SECID"):
                        if not isinstance(row[column], str) or not row[column].strip():
                            raise ISSRequestError(f"ISS за {tradedate}: у SECID={row.get('SECID')!r} "
                                                  f"в history отсутствует корректное значение {column}")
                    if row["TRADEDATE"] != tradedate.isoformat():
                        raise ISSRequestError("ISS вернул историю за другую дату.")
                cursor = table_rows(payload, "history.cursor") if "history.cursor" in payload else []
                total = cursor[0].get("TOTAL") if cursor else None
                if total is not None and (not isinstance(total, int) or total < 0):
                    raise ISSRequestError("Некорректный размер истории в history.cursor.")
                if not rows:
                    if total is not None and start < total:
                        raise ISSRequestError("ISS вернул пустую страницу до конца истории.")
                    break
                if rows == previous:
                    raise ISSRequestError("ISS повторил страницу истории вместо следующей.")
                result.extend(rows)
                origins.extend((row, page_parameters, payload) for row in rows)
                start += len(rows)
                if total is not None and start >= total:
                    break
                previous = rows
            contract_assets = {
                row["SECID"]: row["ASSETCODE"] for row in result
                if row["BOARDID"] == "RFUD" and isinstance(row["ASSETCODE"], str)
                and row["ASSETCODE"].strip()
            }
            filtered, spreads = [], []
            for row, origin_parameters, origin_payload in origins:
                if row["BOARDID"] != "RFUD":
                    continue
                if is_calendar_spread(row["SECID"], contract_assets):
                    spreads.append(row["SECID"])
                    continue
                if not isinstance(row["ASSETCODE"], str) or not row["ASSETCODE"].strip():
                    page_parameters, payload = origin_parameters, origin_payload
                    raise ISSRequestError(f"ISS за {tradedate}: у SECID={row['SECID']!r} "
                                          "в history отсутствует корректное значение ASSETCODE")
                if row["ASSETCODE"] == ASSETCODE:
                    filtered.append(row)
            if spreads:
                LOGGER.info("%s: %s — исключены календарные спреды: %s.",
                            ASSETCODE, tradedate, ", ".join(spreads))
            return filtered
        except ISSRequestError as error:
            error.details = {
                "ASSETCODE": ASSETCODE, "TRADEDATE": tradedate.isoformat(), "reason": str(error),
                "request_url": ISS_BASE + HISTORY_PATH + "?" + urlencode({"iss.meta": "off", **page_parameters}),
                "response": payload,
            }
            raise

    def contract(self, secid: str) -> dict[str, str]:
        """Возвращает настоящие SHORTNAME и LSTTRADE для полного secid, включая архивный суффикс."""
        payload = self.get_json(f"/securities/{quote(secid, safe='')}.json", {"iss.only": "description"})
        description = {row.get("name"): row.get("value")
                       for row in table_rows(payload, "description")}
        if description.get("ASSETCODE") not in (None, ASSETCODE):
            raise DataError(f"Контракт {secid} относится к другому базовому активу.")
        shortname, expiry = description.get("SHORTNAME"), description.get("LSTTRADE")
        if not isinstance(shortname, str) or not shortname.strip():
            raise DataError(f"У контракта {secid} отсутствует SHORTNAME.")
        try:
            expiry = date.fromisoformat(expiry).isoformat()
        except (TypeError, ValueError) as error:
            raise DataError(f"У контракта {secid} отсутствует корректная LSTTRADE.") from error
        return {"SECID": secid, "SHORTNAME": shortname, "LSTTRADE": expiry}


def initialize_database(connection: sqlite3.Connection) -> None:
    """Создаёт таблицы, кеш выходных и отметки подстановок в connection; проверяет схему Day и базовый актив MIX."""
    with connection:
        connection.execute("""CREATE TABLE IF NOT EXISTS Day (
            TRADEDATE TEXT NOT NULL, SECID TEXT NOT NULL,
            OPEN REAL NOT NULL, LOW REAL NOT NULL, HIGH REAL NOT NULL, CLOSE REAL NOT NULL,
            VOLUME INTEGER NOT NULL, OPENPOSITION INTEGER NOT NULL,
            SHORTNAME TEXT NOT NULL, LSTTRADE TEXT NOT NULL,
            PRIMARY KEY (TRADEDATE, SECID), CHECK (LSTTRADE >= TRADEDATE),
            CHECK (VOLUME >= 0), CHECK (OPENPOSITION >= 0)
        )""")
        schema = connection.execute("PRAGMA table_info(Day)").fetchall()
        keys = [row[1] for row in sorted(schema, key=lambda row: row[5]) if row[5]]
        if tuple(row[1] for row in schema) != COLUMNS or keys != ["TRADEDATE", "SECID"]:
            raise DataError("Несовместимая таблица Day: нужны десять заданных полей "
                            "и составной первичный ключ (TRADEDATE, SECID). Укажите новую БД.")
        connection.execute("CREATE TABLE IF NOT EXISTS Metadata (KEY TEXT PRIMARY KEY, VALUE TEXT NOT NULL)")
        owner = connection.execute("SELECT VALUE FROM Metadata WHERE KEY = 'ASSETCODE'").fetchone()
        if owner and owner[0] != ASSETCODE:
            raise DataError(f"Эта БД предназначена для {owner[0]}, а скрипт загружает {ASSETCODE}.")
        if owner is None and connection.execute("SELECT 1 FROM Day LIMIT 1").fetchone():
            raise DataError("В непустой БД не указан базовый актив. Укажите новую БД.")
        connection.execute("INSERT OR IGNORE INTO Metadata VALUES ('ASSETCODE', ?)", (ASSETCODE,))
        connection.execute("""CREATE TABLE IF NOT EXISTS Contracts (
            SECID TEXT PRIMARY KEY, SHORTNAME TEXT NOT NULL, LSTTRADE TEXT NOT NULL
        )""")
        connection.execute("""CREATE TABLE IF NOT EXISTS EmptyWeekends (
            TRADEDATE TEXT PRIMARY KEY, CHECKED_AT TEXT NOT NULL,
            CHECK (strftime('%w', TRADEDATE) IN ('0', '6'))
        )""")
        connection.execute("""CREATE TABLE IF NOT EXISTS FilledQuotes (
            TRADEDATE TEXT NOT NULL, SECID TEXT NOT NULL,
            SOURCE_TRADEDATE TEXT NOT NULL, SOURCE_CLOSE REAL NOT NULL,
            ORIGINAL_ROW TEXT NOT NULL, FILLED_AT TEXT NOT NULL,
            PRIMARY KEY (TRADEDATE, SECID), CHECK (SOURCE_TRADEDATE < TRADEDATE)
        )""")
        connection.execute("CREATE INDEX IF NOT EXISTS Day_SECID_TRADEDATE ON Day (SECID, TRADEDATE)")


def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    """Проверяет row, сохраняет полный SECID и возвращает числовые поля; для NULL называет пустой столбец."""
    try:
        normalized = {column: row[column] for column in COLUMNS}
        tradedate = date.fromisoformat(normalized["TRADEDATE"])
        expiry = date.fromisoformat(normalized["LSTTRADE"])
        if (not isinstance(normalized["SECID"], str) or not normalized["SECID"].strip()
                or expiry < tradedate):
            raise ValueError("Неверный SECID либо контракт уже истёк.")
        if not isinstance(normalized["SHORTNAME"], str) or not normalized["SHORTNAME"].strip():
            raise ValueError("Отсутствует SHORTNAME.")
        normalized["TRADEDATE"], normalized["LSTTRADE"] = tradedate.isoformat(), expiry.isoformat()
        for column in ("OPEN", "LOW", "HIGH", "CLOSE", "VOLUME", "OPENPOSITION"):
            if normalized[column] is None:
                raise ValueError(f"{column}: NULL, отсутствует значение в ответе ISS.")
            if isinstance(normalized[column], bool):
                raise ValueError(f"Булево значение в {column}.")
            number = float(normalized[column])
            if not math.isfinite(number):
                raise ValueError(f"Неконечное значение {column}.")
            if column in ("VOLUME", "OPENPOSITION"):
                if number < 0 or not number.is_integer():
                    raise ValueError(f"{column} должен быть целым неотрицательным числом.")
                normalized[column] = int(number)
            else:
                normalized[column] = number
        return normalized
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise DataError(f"Неполная или неверная строка {row.get('SECID')} за {row.get('TRADEDATE')}: {error}",
                        records=[row]) from error


def fill_no_trade_record(connection: sqlite3.Connection, row: dict[str, Any]) -> dict[str, Any]:
    """Возвращает row с предыдущим CLOSE из connection и отметкой _fill только при отсутствии сделок и всех OHLC."""
    ohlc = ("OPEN", "LOW", "HIGH", "CLOSE")
    if (row.get("NUMTRADES") != 0 or isinstance(row.get("NUMTRADES"), bool)
            or any(column not in row or row[column] is not None for column in ohlc)
            or row.get("VOLUME") not in (None, 0) or isinstance(row.get("VOLUME"), bool)):
        return row
    if row.get("OPENPOSITION") is None:
        raise DataError(f"Подстановка {row['SECID']} за {row['TRADEDATE']} невозможна: "
                        "отсутствует текущий OPENPOSITION в ISS.", records=[row])
    previous = connection.execute("""SELECT TRADEDATE, CLOSE FROM Day
        WHERE SECID = ? AND TRADEDATE < ? ORDER BY TRADEDATE DESC LIMIT 1
    """, (row["SECID"], row["TRADEDATE"])).fetchone()
    if previous is None:
        raise DataError(f"Подстановка {row['SECID']} за {row['TRADEDATE']} невозможна: "
                        "в Day нет предыдущего закрытия этого контракта.", records=[row])
    try:
        close = float(previous[1])
        if not math.isfinite(close):
            raise ValueError("Цена не является конечным числом.")
    except (TypeError, ValueError, OverflowError) as error:
        raise DataError(f"Неверное предыдущее закрытие {row['SECID']} за {previous[0]}: {error}",
                        records=[row]) from error
    return {**row, **dict.fromkeys(ohlc, close), "VOLUME": 0, "_fill": {
        "SOURCE_TRADEDATE": previous[0], "SOURCE_CLOSE": close, "ORIGINAL_ROW": dict(row),
    }}


def choose_pair(tradedate: date, history: list[dict[str, Any]],
                contracts: dict[str, dict[str, str]],
                connection: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Возвращает ближайшую пару history на tradedate по contracts; при наличии connection допускает отмеченный перенос CLOSE."""
    candidates, seen = [], set()
    for row in history:
        secid = row.get("SECID")
        if row.get("TRADEDATE") != tradedate.isoformat() or secid in seen:
            raise DataError(f"Неверная дата или повтор SECID {secid} в истории за {tradedate}.")
        seen.add(secid)
        info = contracts[secid]
        try:
            expiry = date.fromisoformat(info["LSTTRADE"])
        except (KeyError, TypeError, ValueError) as error:
            raise DataError(f"Неверная LSTTRADE у {secid}.") from error
        if expiry >= tradedate:
            candidates.append({**row, "SHORTNAME": info["SHORTNAME"], "LSTTRADE": expiry.isoformat()})
    candidates.sort(key=lambda row: (row["LSTTRADE"], row["SECID"]))
    if len(candidates) < 2:
        raise DataError(f"За {tradedate} доступны только {len(candidates)} действующих контрактов; нужны два.",
                        records=candidates)
    # Сначала выбираем ближайшие серии, затем проверяем полноту их данных.
    # Отсутствующие цены не разрешают подменить ближний контракт дальним.
    selected = candidates[:2]
    try:
        result = []
        for row in selected:
            prepared = fill_no_trade_record(connection, row) if connection is not None else row
            checked = normalize_record(prepared)
            if "_fill" in prepared:
                checked["_fill"] = prepared["_fill"]
            result.append(checked)
        return result
    except DataError as error:
        raise DataError(str(error), records=selected) from error


def diagnostic_details(tradedate: date, history: list[dict[str, Any]],
                       contracts: dict[str, dict[str, str]], error: DataError) -> str:
    """Возвращает JSON диагностики error за tradedate: исходную history, описания contracts и выбранную пару."""
    payload = {
        "ASSETCODE": ASSETCODE,
        "TRADEDATE": tradedate.isoformat(),
        "reason": str(error),
        "history_url": ISS_BASE + HISTORY_PATH + "?" + urlencode({"date": tradedate.isoformat(), "assetcode": ASSETCODE}),
        "selected_contracts": error.records,
        "no_trades": [row["SECID"] for row in error.records if row.get("NUMTRADES") == 0],
        "history": history,
        "contracts": contracts,
    }
    return "\n" + json.dumps(payload, ensure_ascii=False, default=str)


def complete_dates(connection: sqlite3.Connection, start: date, end: date) -> set[date]:
    """Возвращает даты в диапазоне start..end, для которых connection уже содержит ровно две полные строки."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    query = f"SELECT {', '.join(COLUMNS)} FROM Day WHERE TRADEDATE BETWEEN ? AND ? ORDER BY TRADEDATE"
    for values in connection.execute(query, (start.isoformat(), end.isoformat())):
        row = dict(zip(COLUMNS, values))
        grouped.setdefault(row["TRADEDATE"], []).append(row)
    result = set()
    for tradedate, rows in grouped.items():
        if len(rows) != 2:
            continue
        try:
            for row in rows:
                normalize_record(row)
            result.add(date.fromisoformat(tradedate))
        except DataError:
            continue
    return result


def cached_empty_weekends(connection: sqlite3.Connection, start: date, end: date) -> set[date]:
    """Возвращает пустые выходные start..end из connection; даты с любыми строками Day исключает из кеша."""
    rows = connection.execute("""SELECT e.TRADEDATE FROM EmptyWeekends e
        WHERE e.TRADEDATE BETWEEN ? AND ?
        AND NOT EXISTS (SELECT 1 FROM Day d WHERE d.TRADEDATE = e.TRADEDATE)
    """, (start.isoformat(), end.isoformat()))
    return {date.fromisoformat(row[0]) for row in rows if date.fromisoformat(row[0]).weekday() >= 5}


def remember_empty_weekend(connection: sqlite3.Connection, tradedate: date) -> bool:
    """После пустого ответа сохраняет tradedate в connection; возвращает False для будней, текущих дат и строк Day."""
    if tradedate.weekday() < 5 or tradedate > yesterday_moscow():
        return False
    with connection:
        if connection.execute("SELECT 1 FROM Day WHERE TRADEDATE = ? LIMIT 1",
                              (tradedate.isoformat(),)).fetchone():
            return False
        connection.execute("INSERT OR REPLACE INTO EmptyWeekends VALUES (?, ?)",
                           (tradedate.isoformat(), datetime.now(MOSCOW).isoformat(timespec="seconds")))
    return True


def forget_empty_weekend(connection: sqlite3.Connection, tradedate: date) -> None:
    """Удаляет устаревшую отметку tradedate из connection после получения непустой истории."""
    with connection:
        connection.execute("DELETE FROM EmptyWeekends WHERE TRADEDATE = ?", (tradedate.isoformat(),))


def cached_contract(connection: sqlite3.Connection, client: ISSClient, secid: str) -> dict[str, str]:
    """Возвращает описание secid из connection; при отсутствии получает его через client и сохраняет."""
    stored = connection.execute("SELECT SECID, SHORTNAME, LSTTRADE FROM Contracts WHERE SECID = ?", (secid,)).fetchone()
    if stored:
        return dict(zip(("SECID", "SHORTNAME", "LSTTRADE"), stored))
    info = client.contract(secid)
    with connection:
        connection.execute("INSERT INTO Contracts VALUES (?, ?, ?)",
                           (secid, info["SHORTNAME"], info["LSTTRADE"]))
    return info


def save_day(connection: sqlite3.Connection, records: list[dict[str, Any]]) -> None:
    """Атомарно заменяет день в connection двумя records и их отметками _fill; при ошибке сохраняет котировки и отметки."""
    checked = [normalize_record(row) for row in records]
    if (len(checked) != 2 or checked[0]["TRADEDATE"] != checked[1]["TRADEDATE"]
            or checked[0]["SECID"] == checked[1]["SECID"]):
        raise DataError("Запись дня требует двух разных контрактов с одинаковой TRADEDATE.")
    with connection:
        connection.execute("DELETE FROM Day WHERE TRADEDATE = ?", (checked[0]["TRADEDATE"],))
        connection.execute("DELETE FROM FilledQuotes WHERE TRADEDATE = ?", (checked[0]["TRADEDATE"],))
        connection.executemany("INSERT INTO Day VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               [tuple(row[column] for column in COLUMNS) for row in checked])
        connection.execute("DELETE FROM EmptyWeekends WHERE TRADEDATE = ?", (checked[0]["TRADEDATE"],))
        for row in records:
            info = row.get("_fill")
            if info is not None:
                connection.execute("INSERT INTO FilledQuotes VALUES (?, ?, ?, ?, ?, ?)",
                                   (row["TRADEDATE"], row["SECID"], info["SOURCE_TRADEDATE"], info["SOURCE_CLOSE"],
                                    json.dumps(info["ORIGINAL_ROW"], ensure_ascii=False),
                                    datetime.now(MOSCOW).isoformat(timespec="seconds")))


def download_missing(connection: sqlite3.Connection, client: ISSClient,
                     start: date, end: date, recheck_empty_weekends: bool = False,
                     recheck_filled: bool = False) -> DownloadResult:
    """Заполняет start..end через client в connection; флаги перепроверяют пустые выходные и подстановки, возвращает счётчики."""
    completed = complete_dates(connection, start, end)
    filled_dates: set[date] = set()
    if recheck_filled:
        stored_fills = connection.execute("SELECT DISTINCT TRADEDATE FROM FilledQuotes WHERE TRADEDATE BETWEEN ? AND ?",
                                         (start.isoformat(), end.isoformat()))
        filled_dates = {date.fromisoformat(row[0]) for row in stored_fills}
        completed.difference_update(filled_dates)
    empty_weekends = cached_empty_weekends(connection, start, end)
    result = DownloadResult(existing_days=len(completed))
    LOGGER.info("%s: период %s — %s, полных дней в БД: %s; подтверждённых пустых выходных: %s; "
                "перепроверка пустых выходных: %s; перепроверка подставленных свечей: %s.",
                ASSETCODE, start, end, len(completed), len(empty_weekends),
                "да" if recheck_empty_weekends else "нет", "да" if recheck_filled else "нет")
    tradedate = start
    while tradedate <= end:
        if tradedate not in completed:
            if tradedate in empty_weekends and not recheck_empty_weekends:
                result.cached_empty_days += 1
                tradedate += timedelta(days=1)
                continue
            history = client.history(tradedate)
            if not history:
                result.empty_days += 1
                if tradedate in filled_dates:
                    result.failed_days += 1
                    error = DataError("ISS вернул пустую историю при перепроверке дня с подставленными OHLC.")
                    LOGGER.error("%s: %s — %s Прежняя пара и отметки сохранены; повторите --recheck-filled.",
                                 ASSETCODE, tradedate, error,
                                 extra={"details": diagnostic_details(tradedate, history, {}, error)})
                elif remember_empty_weekend(connection, tradedate):
                    LOGGER.info("%s: %s — история отсутствует; пустой выходной сохранён в EmptyWeekends.", ASSETCODE, tradedate)
                else:
                    LOGGER.info("%s: %s — история отсутствует; дата будет проверена при следующем запуске.", ASSETCODE, tradedate)
            else:
                forget_empty_weekend(connection, tradedate)
                contracts = {}
                try:
                    for row in history:
                        contracts[row["SECID"]] = cached_contract(connection, client, row["SECID"])
                    pair = choose_pair(tradedate, history, contracts, connection)
                    save_day(connection, pair)
                    result.written_days += 1
                    for row in pair:
                        if "_fill" in row:
                            info = row["_fill"]
                            result.filled_quotes += 1
                            LOGGER.info("%s: %s %s — подставлены OHLC=%s из CLOSE за %s; VOLUME=0; "
                                        "OPENPOSITION=%s из ISS. Отметка в FilledQuotes.",
                                        ASSETCODE, tradedate, row["SECID"], info["SOURCE_CLOSE"],
                                        info["SOURCE_TRADEDATE"], row["OPENPOSITION"])
                    LOGGER.info("%s: %s — записаны %s и %s.", ASSETCODE, tradedate, pair[0]["SECID"], pair[1]["SECID"])
                except DataError as error:
                    result.failed_days += 1
                    LOGGER.error("%s: %s — день не записан: %s", ASSETCODE, tradedate, error,
                                 extra={"details": diagnostic_details(tradedate, history, contracts, error)})
        tradedate += timedelta(days=1)
    return result


def main(argv: list[str] | None = None) -> int:
    """Выполняет загрузку и закрывает файловые журналы для argv; возвращает 0 при успехе, 1 при ошибке или 130 при прерывании."""
    args = parse_arguments(argv)
    try:
        full_log, error_log = configure_logging(args.log_dir)
        LOGGER.info("Общий журнал: %s. Предупреждения и ошибки: %s.", full_log, error_log)
        args.db.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(args.db, timeout=30)) as connection:
            initialize_database(connection)
            client = ISSClient(args.timeout, args.retries, args.pause)
            result = download_missing(connection, client, args.start, args.end,
                                      args.recheck_empty_weekends, args.recheck_filled)
        LOGGER.info("%s: БД %s. Пропущено полных дней: %s; записано дней: %s; "
                    "пустых ответов: %s; пропущено пустых выходных: %s; подставленных свечей: %s; "
                    "неполных дней: %s.", ASSETCODE, args.db, result.existing_days, result.written_days,
                    result.empty_days, result.cached_empty_days, result.filled_quotes, result.failed_days)
        if result.failed_days:
            LOGGER.warning("%s: загрузка завершена с %s неполными днями. Подробности: %s.",
                           ASSETCODE, result.failed_days, error_log)
        return 1 if result.failed_days else 0
    except (ISSRequestError, DataError, sqlite3.Error, OSError) as error:
        LOGGER.error("Загрузка остановлена: %s. Сохранённые полные дни остаются в БД; "
                     "следующий запуск проверит все пропуски.", error,
                     extra={"details": "\n" + json.dumps(error.details, ensure_ascii=False, default=str)
                            if isinstance(error, ISSRequestError) and error.details else ""})
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Загрузка прервана. Ранее записанные дни сохранены; повторите запуск для докачки.")
        return 130
    except Exception:
        LOGGER.exception("Неожиданная ошибка загрузки. Ранее записанные дни сохранены.")
        return 1
    finally:
        close_logging()


if __name__ == "__main__":
    raise SystemExit(main())
