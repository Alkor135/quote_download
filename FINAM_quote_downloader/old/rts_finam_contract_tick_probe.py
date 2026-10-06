r"""Загружает один день тиков RTS-12.26 (RIZ6) с названием контракта в данных.

Выбирает отдельный инструмент Финама (em=5436517), запрашивает datf=7
с колонкой TICKER и проверяет, что сервер вернул RIZ6 за выбранный день.
Сохраняет contract,ticker,datetime,last,volume в CSV внутри ZIP. Имена файлов
содержат только дату: по умолчанию C:\data_quote\20261005.zip и 20261005.csv
внутри него. Название контракта в имя файла не добавляется.
Гостевой токен получается автоматически; FINAM_TOKEN имеет приоритет.
Существующий ZIP проверяется на наличие выбранного контракта и не перезаписывается.
Условные доли секунды обозначают порядок сделок, а не биржевую точность.

Примеры запуска из корня quote_download в PowerShell:
    .venv\Scripts\python.exe FINAM_quote_downloader\rts_finam_contract_tick_probe.py
    .venv\Scripts\python.exe FINAM_quote_downloader\rts_finam_contract_tick_probe.py --date 2026-10-05 --output C:\data_quote --timeout 15 --attempts 1
    .venv\Scripts\python.exe FINAM_quote_downloader\rts_finam_contract_tick_probe.py --help

ID и тикер проверены по официальной форме 06.10.2026:
https://www.finam.ru/quote/moex/riz6202412/export/
Другие контракты и непрерывная серия SPFB.RTS этим скриптом не загружаются.
"""
import argparse
import datetime
import sys
import zipfile
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request

import pandas as pd

from rts_finam_downloader_tick_to_zip_csv import (
    FINAM_EXPORT_URL,
    DownloadError,
    DownloadFinam,
)


# Настройки конкретного инструмента; это ID контракта, а не общей серии RTS.
CONTRACT_NAME = "RTS-12.26"
CONTRACT_TICKER = "RIZ6"
CONTRACT_FINAM_ID = 5436517
DEFAULT_DATE = datetime.date(2026, 10, 5)
DEFAULT_OUTPUT = r"C:\data_quote"


class RtsContractDownload(DownloadFinam):
    """Использует авторизацию и запись ZIP загрузчика RTS для отдельного контракта."""

    def __init__(self, dir_data: str, *, timeout: float = 30, attempts: int = 3):
        """Создаёт загрузчик RIZ6 в dir_data с тайм-аутом timeout и attempts попытками.

        Авторизацию, проверки времени и атомарную запись предоставляет базовый
        класс. Формат запроса переопределяется на datf=7 с тикером в каждой строке.
        """
        super().__init__("SPFB.RTS", dir_data, 14, 9, timeout=timeout, attempts=attempts)
        self.ticker = CONTRACT_TICKER
        self.datf = 7

    def create_request_finam(self, download_date: str) -> None:
        """Заполняет self.req запросом тиков RIZ6 за download_date в формате ГГГГММДД.

        Использует ID отдельного контракта из формы Финама. У новой формы нет
        числового market; инструмент определяется параметром em. Токен в консоль
        не выводится. Результат хранится в self.req и self.url.
        """
        day = datetime.datetime.strptime(download_date, "%Y%m%d").date()
        params = {
            "em": CONTRACT_FINAM_ID, "code": CONTRACT_TICKER, "apply": 0,
            "df": day.day, "mf": day.month - 1, "yf": day.year,
            "from": day.strftime("%d.%m.%Y"), "dt": day.day,
            "mt": day.month - 1, "yt": day.year, "to": day.strftime("%d.%m.%Y"),
            "p": 1, "f": download_date, "e": ".csv", "cn": CONTRACT_TICKER,
            "dtf": 1, "tmf": 1, "MSOR": 0, "mstime": "on", "mstimever": 1,
            "sep": 1, "sep2": 1, "datf": 7, "at": 1,
        }
        if self.token:
            params["finam_token"] = self.token
        self.url = f"{FINAM_EXPORT_URL}?{urlencode(params)}"
        self.req = Request(self.url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.finam.ru/quote/moex/riz6202412/export/",
        })

    def _parse(self, body: bytes, download_date: str) -> pd.DataFrame:
        """Возвращает проверенные тики из body с названием контракта и тикером.

        download_date задаётся как ГГГГММДД. Отвергает ответ без TICKER или
        с чужим/пустым тикером; базовый класс проверяет дату, время, цену и объём.
        Порядок строк сохраняется, к результату добавляются contract и ticker.
        """
        try:
            frame = pd.read_csv(StringIO(body.decode("utf-8-sig")), dtype={"<TICKER>": str})
        except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError):
            raise DownloadError("Финам не вернул корректный CSV с тикером контракта") from None
        if "<TICKER>" not in frame.columns:
            raise DownloadError("В ответе нет колонки <TICKER>; ожидался формат datf=7")
        if not frame["<TICKER>"].eq(CONTRACT_TICKER).all():
            raise DownloadError(f"В ответе есть пустой или чужой тикер; ожидался {CONTRACT_TICKER}")
        result = super()._parse(body, download_date)
        result.insert(0, "ticker", frame["<TICKER>"].to_numpy())
        result.insert(0, "contract", CONTRACT_NAME)
        return result

    def run(self, download_date: str) -> Path | None:
        """Возвращает путь ZIP за download_date, проверяя контракт в существующем файле.

        download_date — дата ГГГГММДД. Готовый CSV проверяется целиком на название,
        тикер и дату. Неподходящий архив вызывает DownloadError и не изменяется.
        Если файла нет, базовый класс выполняет загрузку; пустой день даёт None.
        """
        path = self.path_file(download_date)
        if path.exists():
            try:
                with zipfile.ZipFile(path) as archive:
                    with archive.open(f"{download_date}.csv") as buffer:
                        frame = pd.read_csv(buffer, dtype={"contract": str, "ticker": str})
                columns = {"contract", "ticker", "datetime", "last", "volume"}
                if frame.empty or not columns.issubset(frame.columns):
                    raise ValueError("Нет тиков с названием контракта")
                if (not frame["contract"].eq(CONTRACT_NAME).all()
                        or not frame["ticker"].eq(CONTRACT_TICKER).all()):
                    raise ValueError("Другой или пустой контракт")
                stamps = pd.to_datetime(frame["datetime"], format="mixed", errors="raise")
                if stamps.isna().any() or not stamps.dt.strftime("%Y%m%d").eq(download_date).all():
                    raise ValueError("Другая или пустая дата")
            except (OSError, KeyError, ValueError, UnicodeError, zipfile.BadZipFile,
                    pd.errors.ParserError, pd.errors.EmptyDataError):
                raise DownloadError(
                    f"Существующий архив не содержит проверенных данных {CONTRACT_NAME} "
                    f"за {download_date}: {path}. Файл не изменён; укажите отдельную папку --output."
                ) from None
            print(f"Проверенный архив уже существует: {path}", flush=True)
            return path
        return super().run(download_date)


def main(argv=None) -> int:
    """Загружает один день по аргументам argv и возвращает код 0 при наличии ZIP.

    argv — список аргументов или None для командной строки. Код 1 означает
    ошибку либо отсутствие сделок, 130 — прерывание пользователем.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=datetime.date.fromisoformat, default=DEFAULT_DATE,
                        help="Один торговый день, ГГГГ-ММ-ДД; по умолчанию 2026-10-05")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help=r"Папка ZIP; по умолчанию C:\data_quote")
    parser.add_argument("--timeout", type=float, default=30, help="Тайм-аут сетевой операции, секунды")
    parser.add_argument("--attempts", type=int, default=3, help="Число попыток запроса")
    args = parser.parse_args(argv)
    try:
        loader = RtsContractDownload(args.output, timeout=args.timeout, attempts=args.attempts)
        print(f"Контракт: {CONTRACT_NAME} ({CONTRACT_TICKER}); день: {args.date:%Y-%m-%d}", flush=True)
        path = loader.run(args.date.strftime("%Y%m%d"))
        if path is None:
            print("Проба не завершена: Финам не вернул сделок за выбранный день.", file=sys.stderr)
            return 1
    except (DownloadError, OSError, ValueError) as error:
        print(f"Проба остановлена: {error}", file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print("Проба прервана пользователем", file=sys.stderr)
        return 130
    print(f"Дневной архив: {path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
