r"""Проверяет загрузку склеенных серий RTS/MIX в компактные дневные CSV внутри ZIP.

Ответы сервера заменяются заданными CSV без TICKER; основной запуск, разбор
данных и запись ZIP выполняются реально. Проверяются формат datf=9, три поля
CSV, порядок всех сделок и догрузка пропусков без изменения существующих ZIP.

Запуск из корня quote_download:
    .venv\Scripts\python.exe -B -m unittest discover -s FINAM_quote_downloader -p test_finam_continuous_download.py -v
"""
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pandas as pd

import mix_finam_downloader_tick_to_zip_csv as mix
import rts_finam_downloader_tick_to_zip_csv as rts


class ContinuousDownloadTests(unittest.TestCase):
    """Проверяет компактную выгрузку склеенных серий и продолжение загрузки."""

    def setUp(self):
        """Создаёт временную папку и задаёт тестовый токен на время проверки."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        token = patch.dict("os.environ", {"FINAM_TOKEN": "test-token"})
        token.start()
        self.addCleanup(token.stop)

    def test_main_downloads_continuous_series_without_symbol_columns(self):
        """Основной запуск обоих скриптов сохраняет все сделки и только три поля."""
        for module, asset, instrument_id in [(rts, "RTS", "17455"), (mix, "MIX", "81408")]:
            with self.subTest(asset=asset):
                folder = self.folder / asset
                body = b"<DATE>,<TIME>,<LAST>,<VOL>\n20261005,100000,100,2\n20261005,100000,101,3\n"
                with patch.object(module, "urlopen", return_value=io.BytesIO(body)) as request, \
                        patch.object(module.time, "sleep"):
                    result = module.main(["--start", "2026-10-05", "--end", "2026-10-05",
                                          "--output", str(folder)])
                self.assertEqual(result, 0)
                self.assertEqual(request.call_count, 1)
                with zipfile.ZipFile(folder / "20261005.zip") as archive:
                    self.assertEqual(archive.namelist(), ["20261005.csv"])
                    frame = pd.read_csv(archive.open("20261005.csv"))
                self.assertEqual(frame.columns.tolist(), ["datetime", "last", "volume"])
                self.assertEqual(frame["last"].tolist(), [100, 101])
                self.assertEqual(frame["volume"].tolist(), [2, 3])
                self.assertEqual(pd.to_datetime(frame["datetime"]).tolist(),
                                 [pd.Timestamp("2026-10-05 10:00:00"), pd.Timestamp("2026-10-05 10:00:00.001")])
                params = parse_qs(urlsplit(request.call_args.args[0].full_url).query)
                self.assertEqual(params["em"], [instrument_id])
                self.assertEqual(params["code"], [asset])
                self.assertEqual(params["datf"], ["9"])

    def test_existing_days_are_untouched_and_only_gap_is_downloaded(self):
        """Повторный запуск пропускает ZIP с тремя и пятью полями и догружает пробел."""
        for module, asset in [(rts, "RTS"), (mix, "MIX")]:
            with self.subTest(asset=asset):
                folder = self.folder / asset
                folder.mkdir()
                for day, content in [("20261004", "datetime,last,volume\n2026-10-04 10:00:00,100,2\n"),
                                     ("20261006", f"contract,ticker,datetime,last,volume\n{asset},SPFB.{asset},2026-10-06 10:00:00,100,2\n")]:
                    with zipfile.ZipFile(folder / f"{day}.zip", "w") as archive:
                        archive.writestr(f"{day}.csv", content)
                before = {day: (folder / f"{day}.zip").read_bytes() for day in ["20261004", "20261006"]}
                body = b"<DATE>,<TIME>,<LAST>,<VOL>\n20261005,100000,100,2\n"
                with patch.object(module, "urlopen", return_value=io.BytesIO(body)) as request, \
                        patch.object(module.time, "sleep"):
                    result = module.main(["--start", "2026-10-04", "--end", "2026-10-06",
                                          "--output", str(folder)])
                self.assertEqual(result, 0)
                self.assertEqual(request.call_count, 1)
                for day, original in before.items():
                    self.assertEqual((folder / f"{day}.zip").read_bytes(), original)
                with zipfile.ZipFile(folder / "20261005.zip") as archive:
                    frame = pd.read_csv(archive.open("20261005.csv"))
                self.assertEqual(frame.columns.tolist(), ["datetime", "last", "volume"])


if __name__ == "__main__":
    unittest.main()
