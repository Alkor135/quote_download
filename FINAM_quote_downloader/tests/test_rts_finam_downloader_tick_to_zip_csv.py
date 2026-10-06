r"""Проверки загрузчика Финама без обращения к внешнему серверу.

Пример запуска из корня проекта:
    .venv\Scripts\python.exe -B -m unittest discover -s FINAM_quote_downloader -p "test_rts_*.py" -v
"""
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pandas as pd

import rts_finam_downloader_tick_to_zip_csv as source
import finam_guest_token as auth
from FINAM_quote_downloader.tests.test_finam_guest_token import CONFIG, Response, auth_response, fixture_token


CSV = b"<DATE>,<TIME>,<LAST>,<VOL>\n20220211,070001,100,2\n20220211,070001,100,3\n20220211,070002,101,4\n"


class RequestTests(unittest.TestCase):
    """Проверяет запросы к актуальному сервису экспорта."""

    def test_request_uses_instance_ticker(self):
        """Тикер экземпляра не должен подменяться глобальной переменной."""
        loader = source.DownloadFinam("SPFB.RTS", ".", 14, 9, 1)
        with patch.object(source, "ticker", "SPFB.BR", create=True):
            loader.create_request_finam("20220211")
        self.assertEqual(parse_qs(urlsplit(loader.url).query)["em"], ["17455"])

    def test_current_endpoint_and_date_format(self):
        """Адрес и даты соответствуют действующей форме Финама."""
        with patch.object(source, "ticker", "SPFB.RTS", create=True):
            loader = source.DownloadFinam("SPFB.RTS", ".", 14, 9, 1)
            loader.create_request_finam("20220211")
        url = urlsplit(loader.url)
        self.assertEqual((url.scheme, url.netloc, url.path),
                         ("https", "export.finam.ru", "/export9.out"))
        params = parse_qs(url.query)
        self.assertEqual(params["from"], ["11.02.2022"])
        self.assertEqual(params["code"], ["RTS"])

    def test_token_from_environment(self):
        """Токен сайта передаётся без хранения в исходном коде."""
        with patch.dict("os.environ", {"FINAM_TOKEN": "test-token"}), \
                patch.object(source, "ticker", "SPFB.RTS", create=True):
            loader = source.DownloadFinam("SPFB.RTS", ".", 14, 9, 1)
            loader.create_request_finam("20220211")
        self.assertEqual(parse_qs(urlsplit(loader.url).query).get("finam_token"), ["test-token"])


class TimestampTests(unittest.TestCase):
    """Проверяет сохранность сделок при присвоении условного времени."""

    def test_busy_second_keeps_all_trades_inside_original_second(self):
        """Более тысячи сделок не теряются и не переходят в соседнюю секунду."""
        base = pd.Timestamp("2022-02-11 12:05:33")
        frame = pd.DataFrame({"datetime": [base] * 1001 + [base + pd.Timedelta(seconds=1)],
                              "last": range(1002), "volume": [1] * 1002})
        result = source.make_timestamps_unique(frame)
        self.assertEqual(len(result), 1002)
        self.assertEqual(result["last"].tolist(), frame["last"].tolist())
        self.assertTrue(result["datetime"].is_unique)
        self.assertTrue((result["datetime"].iloc[:1001].dt.floor("s") == base).all())
        self.assertEqual(result["volume"].sum(), frame["volume"].sum())

    def test_normal_milliseconds_unchanged(self):
        """Для обычной секунды сохраняется привычный шаг в миллисекунду."""
        base = pd.Timestamp("2022-02-11 12:05:33")
        result = source.make_timestamps_unique(pd.DataFrame({"datetime": [base] * 3}))
        self.assertEqual(result["datetime"].tolist(), [base + pd.Timedelta(milliseconds=i) for i in range(3)])

    def test_invalid_time_is_rejected(self):
        """Некорректная дата не должна попадать в архив."""
        with self.assertRaises(ValueError):
            source.make_timestamps_unique(pd.DataFrame({"datetime": [pd.NaT, pd.NaT]}))


class DownloadTests(unittest.TestCase):
    """Проверяет полный путь от ответа сервера до дневного ZIP."""

    def setUp(self):
        """Создаёт временную папку и отключает задержку между тестовыми запросами."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        environment = patch.dict("os.environ", {"FINAM_TOKEN": "test-manual-token"})
        environment.start()
        self.addCleanup(environment.stop)
        self.loader = source.DownloadFinam("SPFB.RTS", self.tmp.name, 14, 9, 1)
        self.loader.timeout = 0.25
        self.loader.attempts = 2
        self.loader.retry_delay = 0
        legacy = patch.object(source, "ticker", "SPFB.RTS", create=True)
        legacy.start()
        self.addCleanup(legacy.stop)
        delay = patch.object(source.time, "sleep")
        delay.start()
        self.addCleanup(delay.stop)

    def test_csv_is_saved_without_losing_identical_trades(self):
        """Одинаковые цена и время не являются основанием удалить сделку."""
        with patch.object(source, "urlopen", return_value=io.BytesIO(CSV)):
            self.loader.run("20220211")
        with zipfile.ZipFile(self.folder / "20220211.zip") as archive:
            self.assertIsNone(archive.testzip())
            result = pd.read_csv(archive.open("20220211.csv"))
        self.assertEqual(result["volume"].tolist(), [2, 3, 4])
        self.assertEqual(result.columns.tolist(), ["datetime", "last", "volume"])

    def test_timeout_is_retried_then_valid_csv_is_saved(self):
        """Кратковременный сетевой сбой не прерывает успешную повторную загрузку."""
        with patch.object(source, "urlopen", side_effect=[TimeoutError("timed out"), io.BytesIO(CSV)]) as request:
            try:
                self.loader.run("20220211")
            except TimeoutError:
                self.fail("Тайм-аут не был обработан повторной попыткой")
        self.assertTrue((self.folder / "20220211.zip").exists())
        self.assertEqual(request.call_args.kwargs.get("timeout"), 0.25)

    def test_html_error_is_not_saved_as_market_data(self):
        """HTML вместо CSV даёт понятную ошибку и не создаёт дневной архив."""
        with patch.object(source, "urlopen", return_value=io.BytesIO(b"<html>Access denied</html>")):
            caught = None
            try:
                self.loader.run("20220211")
            except Exception as error:
                caught = error
        self.assertIsInstance(caught, RuntimeError)
        self.assertIn("CSV", str(caught))
        self.assertFalse((self.folder / "20220211.zip").exists())

    def test_wrong_date_is_not_saved(self):
        """Ответ с другим торговым днём не сохраняется под запрошенной датой."""
        body = CSV.replace(b"20220211", b"20220210")
        with patch.object(source, "urlopen", return_value=io.BytesIO(body)):
            with self.assertRaises(RuntimeError):
                self.loader.run("20220211")
        self.assertFalse((self.folder / "20220211.zip").exists())

    def test_http_auth_error_does_not_leak_token(self):
        """Сообщение об отказе доступа не раскрывает токен из URL."""
        self.loader.token = "private-test-token"
        error = HTTPError("https://export.finam.ru/?finam_token=private-test-token", 403, "Forbidden", {}, None)
        with patch.object(source, "urlopen", side_effect=error):
            caught = None
            try:
                self.loader.run("20220211")
            except Exception as failure:
                caught = failure
        self.assertIsInstance(caught, RuntimeError)
        self.assertIn("403", str(caught))
        self.assertIn("FINAM_TOKEN", str(caught))
        self.assertNotIn("private-test-token", str(caught))

    def test_existing_archive_is_preserved(self):
        """Повторный запуск не перезаписывает уже загруженный архив."""
        path = self.folder / "20220211.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("20220211.csv", "datetime,last,volume\n")
        before = path.read_bytes()
        with patch.object(source, "urlopen", side_effect=AssertionError("Лишний сетевой запрос")):
            self.loader.run("20220211")
        self.assertEqual(path.read_bytes(), before)

    def test_http_400_includes_server_explanation(self):
        """Пояснение к неверному запросу видно пользователю вместо одного кода 400."""
        body = '<html><body>Неверный параметр em</body></html>'.encode('utf-8')
        error = HTTPError('https://export.finam.ru/', 400, 'Bad Request', {}, io.BytesIO(body))
        with patch.object(source, 'urlopen', side_effect=error):
            with self.assertRaises(RuntimeError) as result:
                self.loader.run('20220211')
        self.assertIn('Неверный параметр em', str(result.exception))
        self.assertNotIn('<html>', str(result.exception))

    def test_http_400_redacts_echoed_token(self):
        """Токен, возвращённый в тексте ошибки, не попадает в консоль."""
        self.loader.token = 'secret-token-for-test-only'
        body = b'<p>Invalid token: secret-token-for-test-only</p><a href="https://example.com/?finam_token=secret-token-for-test-only">Export failed</a>'
        error = HTTPError('https://export.finam.ru/', 400, 'Bad Request', {}, io.BytesIO(body))
        with patch.object(source, 'urlopen', side_effect=error):
            with self.assertRaises(RuntimeError) as result:
                self.loader.run('20220211')
        self.assertIn('Export failed', str(result.exception))
        self.assertNotIn(self.loader.token, str(result.exception))

    def test_http_400_decodes_legacy_russian_text(self):
        """Русское пояснение в старой кодировке остаётся читаемым."""
        body = '<p>Ошибка запроса</p>'.encode('cp1251')
        error = HTTPError('https://export.finam.ru/', 400, 'Bad Request', {}, io.BytesIO(body))
        with patch.object(source, 'urlopen', side_effect=error):
            with self.assertRaises(RuntimeError) as result:
                self.loader.run('20220211')
        self.assertIn('Ошибка запроса', str(result.exception))

    def test_http_400_preserved_when_body_cannot_be_read(self):
        """Сбой чтения пояснения не скрывает исходный HTTP-статус."""
        error = HTTPError('https://export.finam.ru/', 400, 'Bad Request', {}, io.BytesIO())
        with patch.object(error, 'read', side_effect=OSError('read failed')), \
                patch.object(source, 'urlopen', side_effect=error):
            with self.assertRaises(RuntimeError) as result:
                self.loader.run('20220211')
        self.assertIn('400', str(result.exception))

    def test_interrupted_write_does_not_leave_final_archive(self):
        """Ошибка записи не оставляет ZIP, который следующий запуск примет за готовый."""
        with patch.object(source, "urlopen", return_value=io.BytesIO(CSV)), \
                patch.object(pd.DataFrame, "to_csv", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                self.loader.run("20220211")
        self.assertFalse((self.folder / "20220211.zip").exists())
        self.assertEqual(list(self.folder.glob("*.part")), [])


class GuestDownloadTests(unittest.TestCase):
    """Проверяет передачу гостевого токена из сервиса авторизации в экспорт."""

    def test_guest_token_reaches_export_and_zip(self):
        """Обычный запуск без FINAM_TOKEN проходит от гостевой сессии до готового ZIP."""
        with tempfile.TemporaryDirectory() as folder, \
                patch.dict("os.environ", {"FINAM_TOKEN": ""}), \
                patch.object(auth.time, "time", return_value=1000), \
                patch.object(source.time, "sleep"):
            loader = source.DownloadFinam("SPFB.RTS", folder, 14, 9)
            replies = [Response(json.dumps(CONFIG).encode()), Response(auth_response(fixture_token()))]
            with patch.object(auth, "urlopen", side_effect=replies) as guest_request, \
                    patch.object(source, "urlopen", return_value=io.BytesIO(CSV)) as export_request:
                loader.run("20220211")
            params = parse_qs(urlsplit(export_request.call_args.args[0].full_url).query)
            self.assertEqual(params["finam_token"], [fixture_token()])
            self.assertEqual(guest_request.call_count, 2)
            with zipfile.ZipFile(Path(folder) / "20220211.zip") as archive:
                self.assertIsNone(archive.testzip())
                self.assertEqual(len(pd.read_csv(archive.open("20220211.csv"))), 3)

    def test_existing_zip_needs_no_guest_token(self):
        """Существующий архив пропускается даже при недоступной авторизации."""
        with tempfile.TemporaryDirectory() as folder, patch.dict("os.environ", {"FINAM_TOKEN": ""}):
            path = Path(folder) / "20220211.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("20220211.csv", "datetime,last,volume\n")
            loader = source.DownloadFinam("SPFB.RTS", folder, 14, 9)
            with patch.object(auth, "urlopen", side_effect=AssertionError("Лишняя авторизация")):
                self.assertEqual(loader.run("20220211"), path)

    def test_auth_failure_stops_before_export(self):
        """Сбой получения гостевого токена не создаёт CSV или ZIP и не вызывает экспорт."""
        with tempfile.TemporaryDirectory() as folder, patch.dict("os.environ", {"FINAM_TOKEN": ""}):
            loader = source.DownloadFinam("SPFB.RTS", folder, 14, 9, attempts=1)
            with patch.object(auth, "urlopen", side_effect=TimeoutError()), \
                    patch.object(source, "urlopen") as export_request:
                with self.assertRaisesRegex(source.DownloadError, "гостевой токен"):
                    loader.run("20220211")
            export_request.assert_not_called()
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_manual_token_bypasses_guest_authorization(self):
        """Токен из FINAM_TOKEN имеет приоритет и не требует гостевой авторизации."""
        with tempfile.TemporaryDirectory() as folder, \
                patch.dict("os.environ", {"FINAM_TOKEN": "manual-test-token"}), \
                patch.object(source.time, "sleep"):
            loader = source.DownloadFinam("SPFB.RTS", folder, 14, 9)
            with patch.object(auth, "urlopen", side_effect=AssertionError("Лишняя авторизация")), \
                    patch.object(source, "urlopen", return_value=io.BytesIO(CSV)) as request:
                loader.run("20220211")
            self.assertIn("finam_token=manual-test-token", request.call_args.args[0].full_url)


if __name__ == "__main__":
    unittest.main()
