r"""Проверяет однодневную выгрузку конкретного контракта RTS без обращения к сети.

Проверяются сохранение названия и тикера в CSV, выбор отдельного инструмента
Финама, отклонение неподходящих данных, проверка существующего архива
и отсутствие контракта в имени ZIP.
Ответы сервера заменяются заданными CSV; обработка и запись ZIP выполняются.

Запуск из корня quote_download:
    .venv\Scripts\python.exe -B -m unittest discover -s FINAM_quote_downloader -p test_rts_contract_tick_probe.py -v
"""
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pandas as pd

import rts_finam_downloader_tick_to_zip_csv as base
import FINAM_quote_downloader.old.rts_finam_contract_tick_probe as source


CSV = (
    b"<TICKER>,<DATE>,<TIME>,<LAST>,<VOL>\n"
    b"RIZ6,20261005,100000,85320,2\n"
    b"RIZ6,20261005,100000,85330,1\n"
    b"RIZ6,20261005,100001,85340,3\n"
)


class RtsContractProbeTests(unittest.TestCase):
    """Проверяет результат пробы на заданных ответах Финама."""

    def setUp(self):
        """Создаёт временную папку и загрузчик; результат хранится в self.loader."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        with patch.dict("os.environ", {"FINAM_TOKEN": "test-token"}):
            self.loader = source.RtsContractDownload(str(self.folder), attempts=1)

    def test_contract_survives_download_into_date_named_zip(self):
        """Исчезновение тикера или названия при записи должно нарушить проверку CSV."""
        with patch.object(base, "urlopen", return_value=io.BytesIO(CSV)), \
                patch.object(base.time, "sleep"):
            path = self.loader.run("20261005")
        self.assertEqual(path, self.folder / "20261005.zip")
        with zipfile.ZipFile(path) as archive:
            self.assertEqual(archive.namelist(), ["20261005.csv"])
            self.assertIsNone(archive.testzip())
            frame = pd.read_csv(archive.open("20261005.csv"))
        self.assertEqual(list(frame.columns), ["contract", "ticker", "datetime", "last", "volume"])
        self.assertEqual(frame["contract"].tolist(), ["RTS-12.26"] * 3)
        self.assertEqual(frame["ticker"].tolist(), ["RIZ6"] * 3)
        self.assertEqual(frame["last"].tolist(), [85320, 85330, 85340])
        self.assertEqual(frame["volume"].tolist(), [2, 1, 3])
        self.assertEqual(frame["datetime"].tolist(), [
            "2026-10-05 10:00:00.000", "2026-10-05 10:00:00.001", "2026-10-05 10:00:01.000",
        ])
        params = parse_qs(urlsplit(self.loader.req.full_url).query)
        self.assertEqual(params["em"], ["5436517"])
        self.assertEqual(params["code"], ["RIZ6"])
        self.assertEqual(params["cn"], ["RIZ6"])
        self.assertEqual(params["datf"], ["7"])
        self.assertEqual(params["p"], ["1"])
        self.assertEqual(params["from"], ["05.10.2026"])
        self.assertEqual(params["to"], ["05.10.2026"])
        self.assertNotIn("market", params)

    def test_unidentified_or_wrong_contract_is_not_saved(self):
        """Чужой тикер и ответ без тикера не должны превращаться в подписанные данные."""
        responses = [
            CSV.replace(b"RIZ6", b"RIU6"),
            CSV.replace(b"RIZ6", b"RTS"),
            CSV.replace(b"RIZ6", b""),
            CSV.replace(b"<TICKER>,", b"").replace(b"RIZ6,", b""),
        ]
        for body in responses:
            with self.subTest(body=body[:60]), \
                    patch.object(base, "urlopen", return_value=io.BytesIO(body)):
                with self.assertRaises(base.DownloadError):
                    self.loader.run("20261005")
            self.assertEqual(list(self.folder.iterdir()), [])

    def test_wrong_day_is_not_saved(self):
        """Данные с другой датой отвергаются до создания дневного архива."""
        with patch.object(base, "urlopen", return_value=io.BytesIO(CSV.replace(b"20261005", b"20261004"))):
            with self.assertRaises(base.DownloadError):
                self.loader.run("20261005")
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_empty_day_returns_failure_without_archive(self):
        """Пустой день сообщает неуспех и не оставляет файл, похожий на готовую загрузку."""
        with patch.dict("os.environ", {"FINAM_TOKEN": "test-token"}), \
                patch.object(base, "urlopen", return_value=io.BytesIO(CSV.splitlines(keepends=True)[0])), \
                patch.object(base.time, "sleep"):
            result = source.main(["--date", "2026-10-05", "--output", str(self.folder)])
        self.assertEqual(result, 1)
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_repeated_run_preserves_existing_archive(self):
        """Повторная проба не перезаписывает существующий дневной ZIP и не требует сети."""
        path = self.folder / "20261005.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("20261005.csv", (
                "contract,ticker,datetime,last,volume\n"
                "RTS-12.26,RIZ6,2026-10-05 10:00:00,85320,2\n"
            ))
        before = path.read_bytes()
        with patch.object(base, "urlopen", side_effect=AssertionError("Лишний запрос")):
            self.assertEqual(self.loader.run("20261005"), path)
        self.assertEqual(path.read_bytes(), before)

    def test_existing_unidentified_archive_is_not_reported_as_success(self):
        """Архив без контракта либо с чужим тикером/днём нельзя принять за результат пробы."""
        path = self.folder / "20261005.zip"
        responses = [
            "datetime,last,volume\n2026-10-05 10:00:00,85320,2\n",
            "contract,ticker,datetime,last,volume\nRTS-9.26,RIU6,2026-10-05 10:00:00,85320,2\n",
            "contract,ticker,datetime,last,volume\nRTS-12.26,RIZ6,2026-10-04 10:00:00,85320,2\n",
        ]
        for text in responses:
            with self.subTest(text=text):
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("20261005.csv", text)
                before = path.read_bytes()
                with patch.object(base, "urlopen", side_effect=AssertionError("Лишний запрос")):
                    with self.assertRaises(base.DownloadError):
                        self.loader.run("20261005")
                self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
