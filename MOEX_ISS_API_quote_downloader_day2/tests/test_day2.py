"""Проверки двух загрузчиков дневных фьючерсов без обращения к MOEX.

Тесты используют временную SQLite БД, контролируемые описания контрактов
и сохранённые ответы истории ISS, предоставленные пользователем.
Проверяются экспирация, пропуски в истории, атомарная запись, пагинация,
сетевые ошибки, файловые журналы с исходными NULL, подстановка предыдущего
закрытия только при отсутствии сделок, её маркировка и кеш пустых выходных
и ограничение загрузки вчерашним московским днём. При повторном запуске
оба журнала и их резервные копии очищаются без изменения посторонних файлов.
Ответ MIX за 14.12.2020
проверяет исключение календарных спредов с NULL ASSETCODE на доске RFUD.

Запуск из корня quote_download:
    python -B -m unittest discover -s MOEX_ISS_API_quote_downloader_day2/tests -v
"""

import importlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse


DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DIRECTORY))
MODULES = ("update_futures_RTS_day2", "update_futures_MIX_day2")
PREFIXES = {"RTS": "RI", "MIX": "MX"}


def history_row(secid, tradedate="2018-03-15", close=110.0):
    """Возвращает историю для SECID, даты и CLOSE; определяет ASSETCODE по тестовому префиксу."""
    return {
        "BOARDID": "RFUD", "TRADEDATE": tradedate, "SECID": secid,
        "OPEN": 100.0, "LOW": 90.0, "HIGH": 120.0, "CLOSE": close,
        "VOLUME": 12, "OPENPOSITION": 0,
        "ASSETCODE": "RTS" if secid.startswith("RI") else "MIX",
        "NUMTRADES": 2, "SETTLEPRICE": 110.0,
    }


def contract_info(secid, expiry):
    """Возвращает SHORTNAME и LSTTRADE для заданных SECID и экспирации."""
    return {"SECID": secid, "SHORTNAME": secid, "LSTTRADE": expiry}


def no_trade_row(secid, tradedate, openposition=38):
    """Возвращает строку secid за tradedate без сделок и OHLC с текущим openposition из ISS."""
    row = history_row(secid, tradedate, close=None)
    row.update(OPEN=None, LOW=None, HIGH=None, VOLUME=None, NUMTRADES=0, OPENPOSITION=openposition)
    return row


def seed_close_pair(source, connection, tradedate="2018-01-11", close=227500):
    """Записывает через source пару в connection на tradedate с CLOSE=close у июньской серии; возвращает описания."""
    prefix = PREFIXES[source.ASSETCODE]
    contracts = {prefix + code: contract_info(prefix + code, expiry)
                 for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
    rows = [history_row(prefix + "H8", tradedate), history_row(prefix + "M8", tradedate, close=close)]
    source.save_day(connection, source.choose_pair(date.fromisoformat(tradedate), rows, contracts))
    return contracts


class FixtureClient:
    """Подменяет только внешние ответы ISS, сохраняя реальную запись в SQLite."""

    def __init__(self, rows, contracts):
        """Сохраняет историю по датам и описания по кодам контрактов."""
        self.rows = rows
        self.contracts = contracts
        self.requested_dates = []
        self.requested_contracts = []

    def history(self, tradedate):
        """Возвращает заданную историю за tradedate либо возбуждает заданную ошибку."""
        self.requested_dates.append(tradedate)
        result = self.rows.get(tradedate, [])
        if isinstance(result, Exception):
            raise result
        return result

    def contract(self, secid):
        """Возвращает описание secid и учитывает обращение к внешнему справочнику."""
        self.requested_contracts.append(secid)
        return self.contracts[secid]


class CommandLineTests(unittest.TestCase):
    """Проверяет доступность обоих скриптов как самостоятельных программ."""

    def test_help_runs_without_third_party_packages(self):
        """Оба скрипта запускают справку стандартным Python без установленных пакетов."""
        for name in MODULES:
            with self.subTest(script=name):
                result = subprocess.run(
                    [sys.executable, "-B", str(DIRECTORY / f"{name}.py"), "--help"],
                    capture_output=True, text=True, encoding="utf-8",
                    env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--db", result.stdout)


class DownloaderTests(unittest.TestCase):
    """Проверяет выбор пары, полноту истории и безопасную докачку обоих инструментов."""

    def setUp(self):
        """Загружает оба модуля и отключает только вывод сообщений тестового запуска."""
        self.modules = [importlib.import_module(name) for name in MODULES]
        for source in self.modules:
            quiet = patch.object(source.LOGGER, "disabled", True)
            quiet.start()
            self.addCleanup(quiet.stop)

    def test_expiry_day_and_zero_openposition_are_preserved(self):
        """Сравнение > вместо >= или отбрасывание нуля нарушит выбранную пару."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = [history_row(prefix + code) for code in ("Z8", "H8", "M8")]
            contracts = {
                prefix + "H8": contract_info(prefix + "H8", "2018-03-15"),
                prefix + "M8": contract_info(prefix + "M8", "2018-06-21"),
                prefix + "Z8": contract_info(prefix + "Z8", "2018-12-20"),
            }
            pair = source.choose_pair(date(2018, 3, 15), rows, contracts)
            self.assertEqual([row["SECID"] for row in pair], [prefix + "H8", prefix + "M8"])
            self.assertEqual(pair[0]["OPENPOSITION"], 0)

    def test_incomplete_nearest_contract_is_not_replaced_by_farther_one(self):
        """NULL у второго ближайшего контракта должен вызвать отказ от записи дня."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = [history_row(prefix + "H8"), history_row(prefix + "M8", close=None),
                    history_row(prefix + "Z8")]
            contracts = {
                prefix + "H8": contract_info(prefix + "H8", "2018-03-15"),
                prefix + "M8": contract_info(prefix + "M8", "2018-06-21"),
                prefix + "Z8": contract_info(prefix + "Z8", "2018-12-20"),
            }
            with self.assertRaises(source.DataError):
                source.choose_pair(date(2018, 3, 15), rows, contracts)

    def test_missing_interior_day_and_one_contract_day_are_downloaded(self):
        """Докачка от MAX даты пропустила бы дырку и день с единственным контрактом."""
        for source in self.modules:
            with self.subTest(instrument=source.ASSETCODE), \
                    tempfile.TemporaryDirectory(dir=DIRECTORY) as directory, \
                    closing(sqlite3.connect(Path(directory) / "quotes.db")) as connection:
                source.initialize_database(connection)
                prefix = PREFIXES[source.ASSETCODE]
                contracts = {
                    prefix + "H8": contract_info(prefix + "H8", "2018-03-15"),
                    prefix + "M8": contract_info(prefix + "M8", "2018-06-21"),
                }
                rows = {day: [history_row(prefix + code, day.isoformat()) for code in ("H8", "M8")]
                        for day in (date(2018, 1, 3), date(2018, 1, 4), date(2018, 1, 5))}
                pair = source.choose_pair(date(2018, 1, 5), rows[date(2018, 1, 5)], contracts)
                source.save_day(connection, pair)
                single = source.choose_pair(date(2018, 1, 4), rows[date(2018, 1, 4)], contracts)[0]
                connection.execute(
                    "INSERT INTO Day VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    tuple(single[column] for column in source.COLUMNS),
                )
                connection.commit()
                client = FixtureClient(rows, contracts)
                result = source.download_missing(connection, client, date(2018, 1, 3), date(2018, 1, 5))
                self.assertEqual(result.failed_days, 0)
                self.assertEqual(connection.execute(
                    "SELECT TRADEDATE, COUNT(*) FROM Day GROUP BY TRADEDATE ORDER BY TRADEDATE"
                ).fetchall(), [("2018-01-03", 2), ("2018-01-04", 2), ("2018-01-05", 2)])
                self.assertEqual(client.requested_dates, [date(2018, 1, 3), date(2018, 1, 4)])
                self.assertEqual(len(client.requested_contracts), 2)
                again = FixtureClient(rows, contracts)
                source.download_missing(connection, again, date(2018, 1, 3), date(2018, 1, 5))
                self.assertEqual(again.requested_dates, [])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 6)

    def test_empty_weekday_is_checked_again_on_next_run(self):
        """Пустой ответ за будний день не должен исключать позднее появившиеся котировки."""
        for source in self.modules:
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                day = date(2018, 1, 3)
                source.download_missing(connection, FixtureClient({}, {}), day, day)
                prefix = PREFIXES[source.ASSETCODE]
                rows = {day: [history_row(prefix + code, day.isoformat()) for code in ("H8", "M8")]}
                contracts = {prefix + code: contract_info(prefix + code, expiry)
                             for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
                source.download_missing(connection, FixtureClient(rows, contracts), day, day)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 2)

    def test_empty_weekends_are_skipped_after_database_reopening(self):
        """Кеш должен убрать повторные запросы пустых суббот и воскресений, сохранив проверку понедельника."""
        for source in self.modules:
            with self.subTest(instrument=source.ASSETCODE), \
                    tempfile.TemporaryDirectory(dir=DIRECTORY) as directory:
                database = Path(directory) / "quotes.db"
                with closing(sqlite3.connect(database)) as connection:
                    source.initialize_database(connection)
                    first = FixtureClient({}, {})
                    source.download_missing(connection, first, date(2018, 1, 6), date(2018, 1, 8))
                with closing(sqlite3.connect(database)) as connection:
                    source.initialize_database(connection)
                    again = FixtureClient({}, {})
                    result = source.download_missing(connection, again, date(2018, 1, 6), date(2018, 1, 8))
                    self.assertEqual(again.requested_dates, [date(2018, 1, 8)])
                    self.assertEqual(result.cached_empty_days, 2)
                    self.assertEqual(result.empty_days, 1)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 0)

    def test_weekend_with_quotes_is_downloaded(self):
        """Название дня недели не должно скрывать фактические котировки торговой субботы."""
        day = date(2018, 4, 28)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = {day: [history_row(prefix + code, day.isoformat()) for code in ("M8", "U8")]}
            contracts = {prefix + code: contract_info(prefix + code, expiry)
                         for code, expiry in (("M8", "2018-06-21"), ("U8", "2018-09-20"))}
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                result = source.download_missing(connection, FixtureClient(rows, contracts), day, day)
                self.assertEqual(result.written_days, 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)

    def test_forced_recheck_invalidates_empty_cache_when_incomplete_quotes_appear(self):
        """Принудительная проверка с неполными котировками должна снять отметку пустоты и разрешить обычную докачку."""
        day = date(2018, 1, 6)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            contracts = {prefix + code: contract_info(prefix + code, expiry)
                         for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                source.download_missing(connection, FixtureClient({}, {}), day, day)
                partial = {day: [history_row(prefix + "H8", day.isoformat()),
                                 history_row(prefix + "M8", day.isoformat(), close=None)]}
                result = source.download_missing(connection, FixtureClient(partial, contracts), day, day,
                                                 recheck_empty_weekends=True)
                self.assertEqual(result.failed_days, 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)
                full = {day: [history_row(prefix + code, day.isoformat()) for code in ("H8", "M8")]}
                result = source.download_missing(connection, FixtureClient(full, contracts), day, day)
                self.assertEqual(result.written_days, 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 2)

    def test_empty_cache_cannot_hide_partially_saved_weekend(self):
        """Наличие одной строки Day должно требовать повторного запроса даже при старой отметке пустоты."""
        day = date(2018, 1, 6)
        for source in self.modules:
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                source.download_missing(connection, FixtureClient({}, {}), day, day)
                connection.execute("INSERT INTO Day VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (day.isoformat(), PREFIXES[source.ASSETCODE] + "H8", 100, 90, 120,
                                    110, 12, 0, "Сохранённый", "2018-03-15"))
                connection.commit()
                for _ in range(2):
                    client = FixtureClient({}, {})
                    result = source.download_missing(connection, client, day, day)
                    self.assertEqual(client.requested_dates, [day])
                    self.assertEqual(result.cached_empty_days, 0)
                self.assertEqual(connection.execute("SELECT SHORTNAME FROM Day").fetchall(), [("Сохранённый",)])

    def test_network_or_protocol_failure_does_not_cache_weekend(self):
        """Сетевой отказ и недополученный ответ должны оставить выходной доступным для повторного запроса."""
        day = date(2018, 1, 6)
        for source in self.modules:
            malformed = {"history": {"columns": ["SECID"], "data": [["Неполный ответ"]]}}
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                with self.assertRaises(source.ISSRequestError):
                    source.download_missing(connection,
                                            FixtureClient({day: source.ISSRequestError("Отказ сети")}, {}), day, day)
                with patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(malformed).encode())):
                    with self.assertRaises(source.ISSRequestError):
                        source.download_missing(connection, source.ISSClient(retries=1, pause=0), day, day)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)
                client = FixtureClient({}, {})
                source.download_missing(connection, client, day, day)
                self.assertEqual(client.requested_dates, [day])

    def test_missing_assetcode_in_response_does_not_cache_weekend(self):
        """NULL или пустой ASSETCODE в полученной строке не должен превращаться в подтверждение пустого выходного."""
        day = date(2018, 1, 6)
        for source in self.modules:
            for value in (None, "", " ", 0):
                with self.subTest(instrument=source.ASSETCODE, assetcode=value), \
                        closing(sqlite3.connect(":memory:")) as connection:
                    source.initialize_database(connection)
                    row = history_row(PREFIXES[source.ASSETCODE] + "H8", day.isoformat())
                    row["ASSETCODE"] = value
                    body = {"history": {"columns": list(row), "data": [list(row.values())]},
                            "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 1, 100]]}}
                    with patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode())):
                        with self.assertRaises(source.ISSRequestError):
                            source.download_missing(connection, source.ISSClient(retries=1, pause=0), day, day)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)

    def test_command_line_rechecks_cached_empty_weekend(self):
        """Флаг командной строки должен перепроверить кеш и загрузить появившуюся полную пару."""
        day = date(2018, 1, 6)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = {day: [history_row(prefix + code, day.isoformat()) for code in ("H8", "M8")]}
            contracts = {prefix + code: contract_info(prefix + code, expiry)
                         for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as directory:
                database = Path(directory) / "quotes.db"
                with closing(sqlite3.connect(database)) as connection:
                    source.initialize_database(connection)
                    source.download_missing(connection, FixtureClient({}, {}), day, day)
                with patch.object(source, "ISSClient", return_value=FixtureClient(rows, contracts)):
                    result = source.main(["--db", str(database), "--log-dir", directory,
                                          "--start", "2018-01-06", "--end", "2018-01-06", "--recheck-empty-weekends"])
                self.assertEqual(result, 0)
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 2)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)

    def test_incomplete_response_preserves_existing_row(self):
        """Отказ загрузки полной пары не должен удалять ранее сохранённую строку."""
        for source in self.modules:
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                connection.execute("INSERT INTO Day VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   ("2018-03-15", PREFIXES[source.ASSETCODE] + "H8", 100, 90, 120,
                                    110, 12, 0, "Сохранённый", "2018-03-15"))
                connection.commit()
                day = date(2018, 3, 15)
                code = PREFIXES[source.ASSETCODE] + "H8"
                client = FixtureClient({day: [history_row(code)]},
                                       {code: contract_info(code, "2018-03-15")})
                result = source.download_missing(connection, client, day, day)
                self.assertEqual(result.failed_days, 1)
                self.assertEqual(connection.execute("SELECT SHORTNAME FROM Day").fetchall(),
                                 [("Сохранённый",)])

    def test_atomic_write_rolls_back_delete_when_insert_fails(self):
        """Ошибка SQLite во время вставки должна откатить предварительное удаление дня."""
        for source in self.modules:
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                prefix = PREFIXES[source.ASSETCODE]
                contracts = {prefix + code: contract_info(prefix + code, expiry)
                             for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
                pair = source.choose_pair(date(2018, 3, 15),
                                          [history_row(prefix + code) for code in ("H8", "M8")], contracts)
                source.save_day(connection, pair)
                connection.execute("CREATE TRIGGER reject_insert BEFORE INSERT ON Day "
                                   "BEGIN SELECT RAISE(ABORT, 'Тестовый отказ'); END")
                connection.commit()
                pair[0]["CLOSE"] = 111
                with self.assertRaises(sqlite3.IntegrityError):
                    source.save_day(connection, pair)
                self.assertEqual(connection.execute("SELECT CLOSE FROM Day ORDER BY SECID").fetchall(),
                                 [(110.0,), (110.0,)])

    def test_database_cannot_be_reused_for_another_instrument(self):
        """MIX не должен принять даты RTS за уже загруженные собственные данные."""
        with closing(sqlite3.connect(":memory:")) as connection:
            self.modules[0].initialize_database(connection)
            with self.assertRaises(self.modules[1].DataError):
                self.modules[1].initialize_database(connection)

    def test_moscow_yesterday_is_used_near_midnight(self):
        """В 21:30 UTC московская дата уже следующая; конец загрузки учитывает это."""
        instant = datetime(2026, 10, 5, 21, 30, tzinfo=timezone.utc)
        for source in self.modules:
            self.assertEqual(source.yesterday_moscow(instant), date(2026, 10, 5))

    def test_current_day_is_rejected_before_database_creation(self):
        """Явный --end не должен позволять загрузку незавершённого дня."""
        for source in self.modules:
            with patch.object(source, "yesterday_moscow", return_value=date(2026, 10, 5)), \
                    patch("sys.stderr", new_callable=io.StringIO):
                with self.assertRaises(SystemExit) as raised:
                    source.parse_arguments(["--end", "2026-10-06"])
                self.assertEqual(raised.exception.code, 2)

    def test_pagination_downloads_both_nearest_contracts(self):
        """Ближайший контракт на второй странице ISS не должен потеряться."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = [history_row(prefix + "M8"), history_row(prefix + "H8")]
            replies = []
            for index, row in enumerate(rows):
                body = {
                    "history": {"columns": list(row), "data": [list(row.values())]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"],
                                       "data": [[index, 2, 1]]},
                }
                replies.append(io.BytesIO(json.dumps(body).encode("utf-8")))
            with patch.object(source, "urlopen", side_effect=replies):
                client = source.ISSClient(timeout=1, retries=1, pause=0)
                result = client.history(date(2018, 3, 15))
            self.assertEqual([row["SECID"] for row in result], [prefix + "M8", prefix + "H8"])

    def test_network_error_is_not_an_empty_trading_day(self):
        """Исчерпанные сетевые попытки должны завершиться ошибкой, а не пустой историей."""
        for source in self.modules:
            with patch.object(source, "urlopen", side_effect=URLError("Тестовый отказ")), \
                    patch.object(source.time, "sleep"):
                client = source.ISSClient(timeout=1, retries=2, pause=0)
                with self.assertRaises(source.ISSRequestError):
                    client.history(date(2018, 3, 15))

    def test_missing_history_column_is_not_treated_as_no_trades(self):
        """Отсутствующий BOARDID в непустом ответе должен означать ошибку протокола, а не выходной."""
        for source in self.modules:
            row = history_row(PREFIXES[source.ASSETCODE] + "H8")
            del row["BOARDID"]
            body = {"history": {"columns": list(row), "data": [list(row.values())]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 1, 1]]}}
            with patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode())):
                client = source.ISSClient(timeout=1, retries=1, pause=0)
                with self.assertRaises(source.ISSRequestError):
                    client.history(date(2018, 3, 15))

    def test_truncated_pagination_is_rejected(self):
        """Пустая вторая страница при TOTAL=2 не разрешает использовать первую как полный ответ."""
        for source in self.modules:
            row = history_row(PREFIXES[source.ASSETCODE] + "H8")
            bodies = [
                {"history": {"columns": list(row), "data": [list(row.values())]},
                 "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 2, 1]]}},
                {"history": {"columns": list(row), "data": []},
                 "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[1, 2, 1]]}},
            ]
            replies = [io.BytesIO(json.dumps(body).encode()) for body in bodies]
            with patch.object(source, "urlopen", side_effect=replies):
                client = source.ISSClient(timeout=1, retries=1, pause=0)
                with self.assertRaises(source.ISSRequestError):
                    client.history(date(2018, 3, 15))

    def test_network_failure_preserves_previously_downloaded_days(self):
        """Сетевой отказ второго дня не должен откатывать успешно записанную пару первого дня."""
        for source in self.modules:
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                prefix = PREFIXES[source.ASSETCODE]
                contracts = {prefix + code: contract_info(prefix + code, expiry)
                             for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
                rows = {
                    date(2018, 1, 3): [history_row(prefix + code, "2018-01-03") for code in ("H8", "M8")],
                    date(2018, 1, 4): source.ISSRequestError("Тестовый сетевой отказ"),
                }
                with self.assertRaises(source.ISSRequestError):
                    source.download_missing(connection, FixtureClient(rows, contracts),
                                            date(2018, 1, 3), date(2018, 1, 4))
                self.assertEqual(connection.execute("SELECT TRADEDATE, COUNT(*) FROM Day GROUP BY TRADEDATE").fetchall(),
                                 [("2018-01-03", 2)])

    def test_archived_security_identifier_is_preserved(self):
        """Дополнительные символы в архивном SECID не разрешают отбрасывать ближайшую серию."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = [history_row(prefix + code, "2018-01-03")
                    for code in ("H8_2018", "M8_2018", "H9", "M9")]
            body = {"history": {"columns": list(rows[0]), "data": [list(row.values()) for row in rows]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 4, 100]]}}
            with patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode())):
                history = source.ISSClient(timeout=1, retries=1, pause=0).history(date(2018, 1, 3))
            contracts = {prefix + code: contract_info(prefix + code, expiry)
                         for code, expiry in (("H8_2018", "2018-03-15"), ("M8_2018", "2018-06-21"),
                                              ("H9", "2019-03-21"), ("M9", "2019-06-20"))}
            pair = source.choose_pair(date(2018, 1, 3), history, contracts)
            self.assertEqual([row["SECID"] for row in pair], [prefix + "H8_2018", prefix + "M8_2018"])

    def test_null_error_identifies_missing_column(self):
        """Ошибка должна называть пустое поле CLOSE, а не внутренний вызов float(None)."""
        for source in self.modules:
            row = {**history_row(PREFIXES[source.ASSETCODE] + "H8", close=None),
                   "SHORTNAME": "Ближний", "LSTTRADE": "2018-03-15"}
            with self.assertRaisesRegex(source.DataError, "CLOSE.*NULL"):
                source.normalize_record(row)

    def test_real_2018_response_downloads_archived_nearest_pair(self):
        """Реальный ответ пользователя должен записать март и июнь 2018 года с правильными ценами и объёмами."""
        source = self.modules[0]
        fixture = Path(__file__).parent / "fixtures" / "rts_history_2018-01-03.json"
        body = json.loads(fixture.read_text(encoding="utf-8"))
        rows = source.table_rows(body, "history")
        shortnames = {row["SECID"]: row["SHORTNAME"] for row in rows}
        expiries = {
            "RIH8_2018": "2018-03-15", "RIM8_2018": "2018-06-21",
            "RIU8_2018": "2018-09-20", "RIZ8_2018": "2018-12-20",
            "RIH9": "2019-03-21", "RIM9": "2019-06-20",
            "RIU9": "2019-09-19", "RIZ9": "2019-12-19",
        }

        def fixture_response(request, timeout):
            """Для request возвращает сохранённую историю или контрольное описание; timeout не используется."""
            path = urlparse(request.full_url).path
            if path == "/iss/history/engines/futures/markets/forts/securities.json":
                reply = body
            else:
                secid = path.removeprefix("/iss/securities/").removesuffix(".json")
                # Описания задаются тестом; реальным является именно ответ history.
                reply = {"description": {
                    "columns": ["name", "title", "value", "type", "sort_order", "is_hidden", "precision"],
                    "data": [
                        ["SHORTNAME", "Краткое имя", shortnames[secid], "string", 1, 0, None],
                        ["LSTTRADE", "Последний день торгов", expiries[secid], "date", 2, 0, None],
                        ["ASSETCODE", "Базовый актив", "RTS", "string", 3, 0, None],
                    ],
                }}
            return io.BytesIO(json.dumps(reply).encode("utf-8"))

        with closing(sqlite3.connect(":memory:")) as connection, \
                patch.object(source, "urlopen", side_effect=fixture_response):
            source.initialize_database(connection)
            result = source.download_missing(connection, source.ISSClient(timeout=1, retries=1, pause=0),
                                             date(2018, 1, 3), date(2018, 1, 3))
            self.assertEqual((result.written_days, result.failed_days), (1, 0))
            self.assertEqual(connection.execute("SELECT * FROM Day ORDER BY LSTTRADE").fetchall(), [
                ("2018-01-03", "RIH8_2018", 116390.0, 115410.0, 118830.0, 118700.0,
                 203474, 362180, "RTS-3.18", "2018-03-15"),
                ("2018-01-03", "RIM8_2018", 115430.0, 115430.0, 117500.0, 117500.0,
                 234, 3288, "RTS-6.18", "2018-06-21"),
            ])

    def test_real_mix_calendar_spreads_do_not_block_nearest_pair(self):
        """Ответ MIX за 14 декабря должен исключить спреды и записать MXZ0 и MXH1 без подстановок."""
        source = self.modules[1]
        fixture = Path(__file__).parent / "fixtures/mix_history_2020-12-14.json"
        body = json.loads(fixture.read_text(encoding="utf-8"))
        day = date(2020, 12, 14)
        client = source.ISSClient(timeout=1, retries=1, pause=0)
        with patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode("utf-8"))):
            try:
                rows = client.history(day)
            except source.ISSRequestError as error:
                self.fail(f"Календарные спреды мешают загрузке отдельных контрактов: {error}")
        self.assertEqual([row["SECID"] for row in rows], ["MXH1", "MXM1", "MXU1", "MXZ0", "MXZ1"])
        expiries = {"MXH1": "2021-03-18", "MXM1": "2021-06-17", "MXU1": "2021-09-16",
                    "MXZ0": "2020-12-17", "MXZ1": "2021-12-16"}
        contracts = {row["SECID"]: {"SECID": row["SECID"], "SHORTNAME": row["SHORTNAME"],
                                   "LSTTRADE": expiries[row["SECID"]]} for row in rows}
        with closing(sqlite3.connect(":memory:")) as connection:
            source.initialize_database(connection)
            result = source.download_missing(connection, FixtureClient({day: rows}, contracts), day, day)
            self.assertEqual((result.written_days, result.failed_days, result.filled_quotes), (1, 0, 0))
            self.assertEqual(connection.execute("SELECT * FROM Day ORDER BY LSTTRADE").fetchall(), [
                ("2020-12-14", "MXZ0", 326500.0, 324525.0, 330925.0, 325125.0,
                 31174, 24174, "MIX-12.20", "2020-12-17"),
                ("2020-12-14", "MXH1", 328400.0, 326425.0, 332925.0, 326650.0,
                 2336, 8634, "MIX-3.21", "2021-03-18"),
            ])

    def test_calendar_spread_and_its_legs_on_different_pages(self):
        """Спред на первой странице исключается по контрактам на следующих страницах с сохранением архивных SECID."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            first, second = prefix + "H8_2018", prefix + "M8_2018"
            rows = [history_row(first + second), history_row(first), history_row(second)]
            rows[0]["ASSETCODE"] = None
            replies = [
                io.BytesIO(json.dumps({
                    "history": {"columns": list(row), "data": [list(row.values())]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[index, 3, 1]]},
                }).encode("utf-8")) for index, row in enumerate(rows)
            ]
            with self.subTest(instrument=source.ASSETCODE), \
                    patch.object(source, "urlopen", side_effect=replies) as request:
                result = source.ISSClient(timeout=1, retries=1, pause=0).history(date(2018, 3, 15))
                self.assertEqual([row["SECID"] for row in result], [first, second])
                self.assertEqual([
                    parse_qs(urlparse(call.args[0].full_url).query)["start"][0]
                    for call in request.call_args_list
                ], ["0", "1", "2"])

    def test_recognized_spread_is_excluded_even_with_assetcode(self):
        """Заполненный ASSETCODE не превращает календарный спред в отдельный контракт."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            first, second = prefix + "H8", prefix + "M8"
            rows = [history_row(first), history_row(first + second), history_row(second)]
            body = {"history": {"columns": list(rows[0]), "data": [list(row.values()) for row in rows]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 3, 100]]}}
            with self.subTest(instrument=source.ASSETCODE), \
                    patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode("utf-8"))):
                result = source.ISSClient(timeout=1, retries=1, pause=0).history(date(2018, 3, 15))
                self.assertEqual([row["SECID"] for row in result], [first, second])

    def test_unverified_composite_code_with_null_asset_does_not_cache_weekend(self):
        """Неопознанная строка с NULL ASSETCODE должна вызвать отказ без ложной отметки пустого выходного."""
        day = date(2020, 12, 13)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            row = history_row(prefix + "Z0" + prefix + "H1", day.isoformat())
            row["ASSETCODE"] = None
            body = {"history": {"columns": list(row), "data": [list(row.values())]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 1, 100]]}}
            with self.subTest(instrument=source.ASSETCODE), closing(sqlite3.connect(":memory:")) as connection, \
                    patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode("utf-8"))):
                source.initialize_database(connection)
                with self.assertRaises(source.ISSRequestError):
                    source.download_missing(connection, source.ISSClient(timeout=1, retries=1, pause=0), day, day)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)

    def test_invalid_asset_on_earlier_page_keeps_its_diagnostic_response(self):
        """Проверка после пагинации должна приложить ответ и URL страницы с неверным ASSETCODE."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            rows = [history_row(prefix + "H8"), history_row(prefix + "M8")]
            rows[0]["ASSETCODE"] = None
            bodies = [{
                "history": {"columns": list(row), "data": [list(row.values())]},
                "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[index, 2, 1]]},
            } for index, row in enumerate(rows)]
            replies = [io.BytesIO(json.dumps(body).encode("utf-8")) for body in bodies]
            with self.subTest(instrument=source.ASSETCODE), \
                    patch.object(source, "urlopen", side_effect=replies):
                with self.assertRaises(source.ISSRequestError) as caught:
                    source.ISSClient(timeout=1, retries=1, pause=0).history(date(2018, 3, 15))
                self.assertEqual(caught.exception.details["response"], bodies[0])
                self.assertEqual(parse_qs(urlparse(caught.exception.details["request_url"]).query)["start"], ["0"])

    def test_real_mix_no_trades_day_is_filled_and_marked(self):
        """Реальный MIX за 12 января должен получить OHLC=227500, VOLUME=0 и исходный OPENPOSITION=38."""
        source = self.modules[1]
        fixture = Path(__file__).parent / "fixtures/mix_no_trades_2018-01-12.json"
        body = json.loads(fixture.read_text(encoding="utf-8"))
        with closing(sqlite3.connect(":memory:")) as connection:
            source.initialize_database(connection)
            source.save_day(connection, body["previous_day"])
            day = date(2018, 1, 12)
            client = FixtureClient({day: body["history"]}, body["contracts"])
            result = source.download_missing(connection, client, day, day)
            self.assertEqual((result.written_days, result.failed_days), (1, 0))
            self.assertEqual(connection.execute(
                "SELECT OPEN, LOW, HIGH, CLOSE, VOLUME, OPENPOSITION FROM Day "
                "WHERE TRADEDATE='2018-01-12' AND SECID='MXM8_2018'"
            ).fetchone(), (227500, 227500, 227500, 227500, 0, 38))
            marker = connection.execute(
                "SELECT SOURCE_TRADEDATE, SOURCE_CLOSE, ORIGINAL_ROW FROM FilledQuotes"
            ).fetchone()
            self.assertEqual(marker[:2], ("2018-01-11", 227500))
            original = json.loads(marker[2])
            self.assertEqual(original["NUMTRADES"], 0)
            self.assertIsNone(original["OPEN"])
            self.assertIsNone(original["VOLUME"])
            self.assertEqual(result.filled_quotes, 1)
            again = FixtureClient({}, {})
            source.download_missing(connection, again, day, day)
            self.assertEqual(again.requested_dates, [])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM FilledQuotes").fetchone()[0], 1)
            self.assertEqual(len(connection.execute("PRAGMA table_info(Day)").fetchall()), 10)

    def test_no_trade_chain_uses_same_contract_past_close_and_current_positions(self):
        """Перенос через выходные должен брать цену того же контракта из прошлого, включая предыдущую подстановку."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                seed_close_pair(source, connection, "2018-01-10", 225325)
                contracts = seed_close_pair(source, connection)
                seed_close_pair(source, connection, "2018-01-16", 999999)
                rows = {day: [history_row(prefix + "H8", day.isoformat()),
                              no_trade_row(prefix + "M8", day.isoformat(), position)]
                        for day, position in ((date(2018, 1, 12), 38), (date(2018, 1, 15), 40))}
                result = source.download_missing(connection, FixtureClient(rows, contracts),
                                                 date(2018, 1, 12), date(2018, 1, 15))
                self.assertEqual((result.written_days, result.filled_quotes, result.failed_days), (2, 2, 0))
                self.assertEqual(connection.execute(
                    "SELECT TRADEDATE, CLOSE, VOLUME, OPENPOSITION FROM Day WHERE SECID=? "
                    "AND TRADEDATE BETWEEN '2018-01-12' AND '2018-01-15' ORDER BY TRADEDATE", (prefix + "M8",)
                ).fetchall(), [("2018-01-12", 227500, 0, 38), ("2018-01-15", 227500, 0, 40)])
                self.assertEqual(connection.execute(
                    "SELECT TRADEDATE, SOURCE_TRADEDATE FROM FilledQuotes ORDER BY TRADEDATE"
                ).fetchall(), [("2018-01-12", "2018-01-11"), ("2018-01-15", "2018-01-12")])

    def test_fill_is_refused_when_trade_or_required_field_conditions_fail(self):
        """Сделки, частично заполненные OHLC, ненулевой объём и отсутствие текущего интереса должны запретить подстановку."""
        cases = ({"NUMTRADES": 1}, {"NUMTRADES": False}, {"NUMTRADES": None},
                 {"OPEN": 220000}, {"VOLUME": 1}, {"VOLUME": False}, {"OPENPOSITION": None})
        day = date(2018, 1, 12)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            for changes in cases:
                with self.subTest(instrument=source.ASSETCODE, changes=changes), \
                        closing(sqlite3.connect(":memory:")) as connection:
                    source.initialize_database(connection)
                    contracts = seed_close_pair(source, connection)
                    missing = no_trade_row(prefix + "M8", day.isoformat())
                    missing.update(changes)
                    rows = {day: [history_row(prefix + "H8", day.isoformat()), missing]}
                    result = source.download_missing(connection, FixtureClient(rows, contracts), day, day)
                    self.assertEqual(result.failed_days, 1)
                    self.assertEqual(connection.execute(
                        "SELECT COUNT(*) FROM Day WHERE TRADEDATE='2018-01-12'"
                    ).fetchone()[0], 0)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM FilledQuotes").fetchone()[0], 0)

    def test_fill_cannot_use_other_contract_or_future_close(self):
        """Цена другого SECID в прошлом или того же SECID в будущем не должна заполнять пустую свечу."""
        day = date(2018, 1, 12)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                contracts = seed_close_pair(source, connection, "2018-01-15", 999999)
                other = {prefix + code: contract_info(prefix + code, expiry)
                         for code, expiry in (("H8", "2018-03-15"), ("Z8", "2018-12-20"))}
                source.save_day(connection, source.choose_pair(date(2018, 1, 11),
                    [history_row(prefix + code, "2018-01-11") for code in ("H8", "Z8")], other))
                rows = {day: [history_row(prefix + "H8", day.isoformat()), no_trade_row(prefix + "M8", day.isoformat())]}
                result = source.download_missing(connection, FixtureClient(rows, contracts), day, day)
                self.assertEqual(result.failed_days, 1)
                self.assertEqual(result.filled_quotes, 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM FilledQuotes").fetchone()[0], 0)

    def test_fill_marker_failure_rolls_back_quotes_and_previous_marker(self):
        """Отказ записи служебной отметки должен откатить новую пару и сохранить прежнюю отметку и котировки."""
        day = date(2018, 1, 12)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                contracts = seed_close_pair(source, connection)
                rows = [history_row(prefix + "H8", day.isoformat()), no_trade_row(prefix + "M8", day.isoformat())]
                source.save_day(connection, source.choose_pair(day, rows, contracts, connection))
                before_quotes = connection.execute("SELECT * FROM Day ORDER BY TRADEDATE, SECID").fetchall()
                before_markers = connection.execute("SELECT * FROM FilledQuotes").fetchall()
                connection.execute("CREATE TRIGGER reject_marker BEFORE INSERT ON FilledQuotes "
                                   "BEGIN SELECT RAISE(ABORT, 'Отказ записи отметки'); END")
                connection.commit()
                rows[1]["OPENPOSITION"] = 40
                pair = source.choose_pair(day, rows, contracts, connection)
                with self.assertRaises(sqlite3.IntegrityError):
                    source.save_day(connection, pair)
                self.assertEqual(connection.execute("SELECT * FROM Day ORDER BY TRADEDATE, SECID").fetchall(), before_quotes)
                self.assertEqual(connection.execute("SELECT * FROM FilledQuotes").fetchall(), before_markers)

    def test_command_line_recheck_filled_replaces_quote_and_removes_marker(self):
        """Флаг --recheck-filled должен записать появившиеся реальные котировки и снять отметку подстановки."""
        day = date(2018, 1, 12)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as directory:
                database = Path(directory) / "quotes.db"
                with closing(sqlite3.connect(database)) as connection:
                    source.initialize_database(connection)
                    contracts = seed_close_pair(source, connection)
                    raw = {day: [history_row(prefix + "H8", day.isoformat()), no_trade_row(prefix + "M8", day.isoformat())]}
                    source.download_missing(connection, FixtureClient(raw, contracts), day, day)
                actual = {day: [history_row(prefix + "H8", day.isoformat()),
                                history_row(prefix + "M8", day.isoformat(), close=230350)]}
                with patch.object(source, "ISSClient", return_value=FixtureClient(actual, contracts)):
                    status = source.main(["--db", str(database), "--log-dir", directory,
                                          "--start", "2018-01-12", "--end", "2018-01-12", "--recheck-filled"])
                self.assertEqual(status, 0)
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(connection.execute("SELECT CLOSE, VOLUME FROM Day WHERE TRADEDATE=? AND SECID=?",
                                                       (day.isoformat(), prefix + "M8")).fetchone(), (230350, 12))
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM FilledQuotes").fetchone()[0], 0)

    def test_recheck_filled_updates_following_flat_candle_from_corrected_close(self):
        """Перепроверка должна обновить следующую подстановку по исправленному закрытию предыдущей даты."""
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            with closing(sqlite3.connect(":memory:")) as connection:
                source.initialize_database(connection)
                contracts = seed_close_pair(source, connection)
                rows = {day: [history_row(prefix + "H8", day.isoformat()), no_trade_row(prefix + "M8", day.isoformat())]
                        for day in (date(2018, 1, 12), date(2018, 1, 15))}
                source.download_missing(connection, FixtureClient(rows, contracts), date(2018, 1, 12), date(2018, 1, 15))
                rows[date(2018, 1, 12)][1] = history_row(prefix + "M8", "2018-01-12", close=230350)
                result = source.download_missing(connection, FixtureClient(rows, contracts),
                                                 date(2018, 1, 12), date(2018, 1, 15), recheck_filled=True)
                self.assertEqual((result.written_days, result.filled_quotes), (2, 1))
                self.assertEqual(connection.execute("SELECT TRADEDATE, SOURCE_CLOSE FROM FilledQuotes").fetchall(),
                                 [("2018-01-15", 230350)])

    def test_empty_response_during_filled_recheck_is_failure_and_preserves_pair(self):
        """Пустой ответ при --recheck-filled должен вернуть код 1 и сохранить прежние котировки и отметку."""
        day = date(2018, 1, 12)
        for source in self.modules:
            prefix = PREFIXES[source.ASSETCODE]
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as directory:
                database = Path(directory) / "quotes.db"
                with closing(sqlite3.connect(database)) as connection:
                    source.initialize_database(connection)
                    contracts = seed_close_pair(source, connection)
                    rows = {day: [history_row(prefix + "H8", day.isoformat()), no_trade_row(prefix + "M8", day.isoformat())]}
                    source.download_missing(connection, FixtureClient(rows, contracts), day, day)
                    before_quotes = connection.execute("SELECT * FROM Day ORDER BY TRADEDATE, SECID").fetchall()
                    before_markers = connection.execute("SELECT * FROM FilledQuotes").fetchall()
                with patch.object(source, "ISSClient", return_value=FixtureClient({}, {})):
                    status = source.main(["--db", str(database), "--log-dir", directory,
                                          "--start", "2018-01-12", "--end", "2018-01-12", "--recheck-filled"])
                self.assertEqual(status, 1)
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(connection.execute("SELECT * FROM Day ORDER BY TRADEDATE, SECID").fetchall(), before_quotes)
                    self.assertEqual(connection.execute("SELECT * FROM FilledQuotes").fetchall(), before_markers)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM EmptyWeekends").fetchone()[0], 0)


class FileLoggingTests(unittest.TestCase):
    """Проверяет реальные файлы журналов при неполных данных и повторных запусках."""

    def test_null_quotes_and_trade_count_are_saved_in_error_log(self):
        """Журнал должен сохранить NULL, NUMTRADES=0 и выбранную пару без изменения котировок."""
        for name in MODULES:
            source = importlib.import_module(name)
            prefix = PREFIXES[source.ASSETCODE]
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as temporary:
                log_dir = Path(temporary) / "logs"
                day = date(2018, 1, 12)
                nearby = history_row(prefix + "H8_2018", day.isoformat())
                next_contract = history_row(prefix + "M8_2018", day.isoformat(), close=None)
                next_contract.update(OPEN=None, LOW=None, HIGH=None, VOLUME=None, NUMTRADES=0)
                contracts = {
                    prefix + "H8_2018": contract_info(prefix + "H8_2018", "2018-03-15"),
                    prefix + "M8_2018": contract_info(prefix + "M8_2018", "2018-06-21"),
                }
                with closing(sqlite3.connect(":memory:")) as connection, \
                        patch("sys.stderr", new_callable=io.StringIO):
                    source.initialize_database(connection)
                    try:
                        source.configure_logging(log_dir)
                        source.LOGGER.info("Начало проверки")
                        result = source.download_missing(
                            connection, FixtureClient({day: [nearby, next_contract]}, contracts), day, day)
                        self.assertEqual(result.failed_days, 1)
                        self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 0)
                    finally:
                        source.close_logging()
                full_log = (log_dir / f"{source.ASSETCODE}_day2.log").read_text(encoding="utf-8")
                error_log = (log_dir / f"{source.ASSETCODE}_day2_errors.log").read_text(encoding="utf-8")
                self.assertIn("Начало проверки", full_log)
                self.assertNotIn("Начало проверки", error_log)
                self.assertIn("2018-01-12", error_log)
                diagnostics = json.loads(next(line for line in error_log.splitlines() if line.startswith("{")))
                self.assertEqual(diagnostics["ASSETCODE"], source.ASSETCODE)
                self.assertEqual([row["SECID"] for row in diagnostics["selected_contracts"]],
                                 [prefix + "H8_2018", prefix + "M8_2018"])
                self.assertEqual(diagnostics["selected_contracts"][1]["NUMTRADES"], 0)
                self.assertIsNone(diagnostics["selected_contracts"][1]["OPEN"])
                self.assertIn("date=2018-01-12", diagnostics["history_url"])

    def test_successful_fill_is_logged_without_error(self):
        """Успешный перенос должен отражаться в общем журнале с ценой и датой источника, без сообщения об ошибке."""
        for name in MODULES:
            source = importlib.import_module(name)
            prefix = PREFIXES[source.ASSETCODE]
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as directory, \
                    closing(sqlite3.connect(":memory:")) as connection, \
                    patch("sys.stderr", new_callable=io.StringIO):
                source.initialize_database(connection)
                contracts = seed_close_pair(source, connection)
                day = date(2018, 1, 12)
                rows = {day: [history_row(prefix + "H8", day.isoformat()), no_trade_row(prefix + "M8", day.isoformat())]}
                try:
                    source.configure_logging(Path(directory))
                    result = source.download_missing(connection, FixtureClient(rows, contracts), day, day)
                    self.assertEqual((result.written_days, result.filled_quotes, result.failed_days), (1, 1, 0))
                finally:
                    source.close_logging()
                full_log = (Path(directory) / f"{source.ASSETCODE}_day2.log").read_text(encoding="utf-8")
                self.assertIn("227500", full_log)
                self.assertIn("2018-01-11", full_log)
                self.assertIn(prefix + "M8", full_log)
                self.assertEqual((Path(directory) / f"{source.ASSETCODE}_day2_errors.log").read_text(encoding="utf-8"), "")

    def test_invalid_history_identifier_is_logged_with_raw_response(self):
        """Остановка на неверном ASSETCODE должна сохранить дату, SECID, URL и исходный ответ, а не только название поля."""
        for name in MODULES:
            source = importlib.import_module(name)
            prefix = PREFIXES[source.ASSETCODE]
            row = history_row(prefix + "H8", "2020-12-14")
            row["ASSETCODE"] = None
            body = {"history": {"columns": list(row), "data": [list(row.values())]},
                    "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 1, 100]]}}
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as directory, \
                    patch.object(source, "urlopen", return_value=io.BytesIO(json.dumps(body).encode())), \
                    patch("sys.stderr", new_callable=io.StringIO):
                status = source.main(["--db", str(Path(directory) / "quotes.db"), "--log-dir", directory,
                                      "--start", "2020-12-14", "--end", "2020-12-14", "--retries", "1"])
                self.assertEqual(status, 1)
                content = (Path(directory) / f"{source.ASSETCODE}_day2_errors.log").read_text(encoding="utf-8")
                self.assertIn(prefix + "H8", content)
                self.assertIn("2020-12-14", content)
                details = json.loads(next(line for line in content.splitlines() if line.startswith("{")))
                self.assertEqual(details["TRADEDATE"], "2020-12-14")
                self.assertIn("date=2020-12-14", details["request_url"])
                self.assertEqual(details["response"], body)

    def test_repeat_logging_keeps_only_last_run_without_duplicate_handlers(self):
        """Новый запуск очищает оба журнала и старые копии, сохраняет посторонние файлы и не дублирует сообщения."""
        for name in MODULES:
            source = importlib.import_module(name)
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as temporary, \
                    patch("sys.stderr", new_callable=io.StringIO):
                try:
                    full_path, error_path = source.configure_logging(Path(temporary))
                    source.LOGGER.warning("Первое предупреждение")
                    backups = [path.with_name(f"{path.name}.{index}")
                               for path in (full_path, error_path) for index in range(1, 4)]
                    for path in backups:
                        path.write_text("Старый запуск", encoding="utf-8")
                    unrelated = Path(temporary) / "other.log.1"
                    unrelated.write_text("Посторонний файл", encoding="utf-8")
                    source.configure_logging(Path(temporary))
                    self.assertEqual(error_path.read_text(encoding="utf-8"), "")
                    self.assertEqual(full_path.read_text(encoding="utf-8"), "")
                    source.LOGGER.error("Второе сообщение")
                finally:
                    source.close_logging()
                for path in (full_path, error_path):
                    content = path.read_text(encoding="utf-8")
                    self.assertNotIn("Первое предупреждение", content)
                    self.assertEqual(content.count("Второе сообщение"), 1)
                self.assertFalse(any(path.exists() for path in backups))
                self.assertEqual(unrelated.read_text(encoding="utf-8"), "Посторонний файл")

    def test_main_logs_network_failure_and_preserves_existing_database(self):
        """Отказ сети должен записаться в файл, вернуть код 1 и оставить ранее сохранённые дни."""
        for name in MODULES:
            source = importlib.import_module(name)
            prefix = PREFIXES[source.ASSETCODE]
            with tempfile.TemporaryDirectory(dir=DIRECTORY) as temporary:
                database = Path(temporary) / "quotes.db"
                log_dir = Path(temporary) / "logs"
                with closing(sqlite3.connect(database)) as connection:
                    source.initialize_database(connection)
                    contracts = {prefix + code: contract_info(prefix + code, expiry)
                                 for code, expiry in (("H8", "2018-03-15"), ("M8", "2018-06-21"))}
                    pair = source.choose_pair(date(2018, 1, 3),
                                              [history_row(prefix + code, "2018-01-03") for code in ("H8", "M8")], contracts)
                    source.save_day(connection, pair)
                with patch.object(source.ISSClient, "history", side_effect=source.ISSRequestError("Тестовый отказ сети")), \
                        patch("sys.stderr", new_callable=io.StringIO):
                    status = source.main(["--db", str(database), "--log-dir", str(log_dir),
                                          "--start", "2018-01-03", "--end", "2018-01-04"])
                self.assertEqual(status, 1)
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM Day").fetchone()[0], 2)
                error_log = (log_dir / f"{source.ASSETCODE}_day2_errors.log").read_text(encoding="utf-8")
                self.assertIn("Тестовый отказ сети", error_log)



if __name__ == "__main__":
    unittest.main()
