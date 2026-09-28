r"""Скачивает тиковые сделки MIX с Финама в отдельный CSV внутри ZIP за каждый день.

Примеры запуска из корня проекта quote_download:
    .venv\Scripts\python.exe FINAM_quote_downloader\mix_finam_downloader_tick_to_zip_csv.py
    .venv\Scripts\python.exe FINAM_quote_downloader\mix_finam_downloader_tick_to_zip_csv.py --start 2022-02-11 --end 2022-02-11 --output data\finam_mix_probe --timeout 15 --attempts 1

Без аргументов используются настройки внизу файла. Гостевой токен сайта
получается автоматически без логина и пароля. Переменная FINAM_TOKEN
позволяет использовать токен сайта, скопированный вручную из браузера.
Токен сайта finam_token и токен брокерского Trade API — разные сущности.
Миллисекунды условные: обозначают порядок строк, а не биржевое время.
При большом числе сделок шаг уменьшается, чтобы сохранить всю секунду.
"""
import argparse
import datetime
import math
import os
import re
import sys
import time
import zipfile
from http.client import IncompleteRead
from html import unescape
from io import StringIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

from finam_guest_token import GuestTokenClient, GuestTokenError
from settings import TICKERS


FINAM_EXPORT_URL = "https://export.finam.ru/export9.out"


class DownloadError(RuntimeError):
    """Обозначает сбой сервера или недостоверный ответ вместо тиковых данных."""


def make_timestamps_unique(df, time_column="datetime"):
    """Сохраняет все строки и их порядок, распределяя условные метки внутри секунды.

    До 1000 сделок включительно шаг составляет 1 мс. Для более плотной секунды
    шаг уменьшается до целого числа наносекунд. Совпавшие сделки не удаляются.
    На входе ожидается исходное время с точностью до секунды.
    """
    result = df.copy()
    stamps = result[time_column]
    if stamps.isna().any():
        raise ValueError("В данных есть некорректные временные метки")
    if result.empty:
        return result
    if not stamps.eq(stamps.dt.floor("s")).all():
        raise ValueError("Ожидаются исходные метки с точностью до секунды")
    groups = stamps.groupby(stamps, sort=False)
    counts = groups.transform("size")
    if (counts > 1_000_000_000).any():
        raise ValueError("Слишком много сделок для уникальных меток внутри секунды")
    steps = (1_000_000_000 // counts).clip(upper=1_000_000)
    result[time_column] = stamps + pd.to_timedelta(groups.cumcount() * steps, unit="ns")
    return result


class DownloadFinam:
    """Загружает и проверяет один торговый день перед сохранением архива."""

    def __init__(self, ticker: str, dir_data: str, market: int, daft: int,
                 period: int = 1, *, timeout: float = 30, attempts: int = 3):
        """Задаёт инструмент, папку, формат и ограничение сетевых попыток."""
        if ticker not in TICKERS:
            raise ValueError(f"Неизвестный тикер {ticker}: добавьте его в settings.py")
        if period != 1 or daft != 9:
            raise ValueError("Этот загрузчик поддерживает тики: period=1, daft=9")
        if not math.isfinite(timeout) or timeout <= 0 or attempts < 1:
            raise ValueError("Тайм-аут должен быть положительным, число попыток — не менее 1")
        self.dir_data = dir_data
        self.ticker = ticker
        self.market = market
        self.datf = daft
        self.period = period
        self.timeout = timeout
        self.attempts = attempts
        self.retry_delay = 2
        self.token = os.environ.get("FINAM_TOKEN", "").strip()
        self._guest_tokens = None if self.token else GuestTokenClient(timeout=timeout, attempts=attempts)
        self.url = ""
        self.req = None

    def create_request_finam(self, download_date: str) -> None:
        """Составляет запрос по параметрам действующей формы экспорта Финама."""
        day = datetime.datetime.strptime(download_date, "%Y%m%d").date()
        code = self.ticker.removeprefix("SPFB.")
        params = {
            "market": self.market, "em": TICKERS[self.ticker], "code": code,
            "apply": 0, "df": day.day, "mf": day.month - 1, "yf": day.year,
            "from": day.strftime("%d.%m.%Y"), "dt": day.day,
            "mt": day.month - 1, "yt": day.year, "to": day.strftime("%d.%m.%Y"),
            "p": self.period, "f": f"{self.ticker}_{download_date}",
            "e": ".csv", "cn": code, "dtf": 1, "tmf": 1, "MSOR": 0,
            "mstime": "on", "mstimever": 1, "sep": 1, "sep2": 1,
            "datf": self.datf, "at": 1,
        }
        if self.token:
            params["finam_token"] = self.token
        self.url = f"{FINAM_EXPORT_URL}?{urlencode(params)}"
        self.req = Request(self.url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.finam.ru/",
        })

    def path_file(self, file_name_date: str) -> Path:
        """Возвращает путь дневного архива и при необходимости создаёт папку."""
        datetime.datetime.strptime(file_name_date, "%Y%m%d")
        folder = Path(self.dir_data)
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{file_name_date}.zip"

    def _fetch(self) -> bytes:
        """Получает ответ с тайм-аутом и повторами, не выводя URL с токеном."""
        for attempt in range(1, self.attempts + 1):
            try:
                with urlopen(self.req, timeout=self.timeout) as response:
                    return response.read()
            except HTTPError as error:
                detail = self._http_error_detail(error)
                if error.code in (401, 403):
                    raise DownloadError(
                        f"Финам отклонил запрос: HTTP {error.code}. Проверьте доступ "
                        f"к экспорту на сайте и актуальность FINAM_TOKEN. {detail}"
                    ) from None
                if error.code not in (429, 500, 502, 503, 504):
                    token_state = ("гостевой, получен автоматически" if self._guest_tokens
                                   else "задан через FINAM_TOKEN")
                    raise DownloadError(
                        f"Финам вернул HTTP {error.code}. {detail} "
                        f"Токен: {token_state}."
                    ) from None
                reason = f"HTTP {error.code}"
            except (URLError, OSError, IncompleteRead) as error:
                is_timeout = isinstance(error, TimeoutError) or isinstance(getattr(error, "reason", None), TimeoutError)
                reason = "тайм-аут" if is_timeout else "сетевая ошибка"
            if attempt == self.attempts:
                raise DownloadError(
                    f"Сервер export.finam.ru недоступен: {reason}; "
                    f"исчерпаны попытки ({self.attempts}), тайм-аут {self.timeout:g} с. "
                    "Файл не сохранён. Проверьте доступность экспорта в браузере."
                ) from None
            print(f"Финам: {reason}; повтор {attempt + 1}/{self.attempts}", flush=True)
            time.sleep(self.retry_delay * attempt)
        raise DownloadError("Не удалось получить ответ Финама")

    def _http_error_detail(self, error: HTTPError) -> str:
        """Читает короткое пояснение сервера, убирая HTML, URL и значения токенов."""
        try:
            raw = error.read(8192)
        except (OSError, IncompleteRead):
            return "Не удалось прочитать пояснение сервера."
        finally:
            error.close()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1251", errors="replace")
        text = unescape(text)
        for secret in (self.token, quote(self.token, safe="")):
            if secret:
                # Маскируется также токен, обрезанный ограничением длины ответа.
                text = re.sub(re.escape(secret[:16]) + r"""[^\s<>"'&]*""", "[скрыт]", text)
        text = re.sub(r"""https?://[^\s<>"']+""", "[URL скрыт]", text)
        text = re.sub(r"""(?i)(finam_token|access_token)\s*["']?\s*[:=]\s*["']?[^\s&<>"']+""",
                      r"\1=[скрыт]", text)
        text = re.sub(r"\b[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]+",
                      "[токен скрыт]", text)
        text = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1\s*>", " ", text)
        text = re.sub(r"<[^>]*>", " ", text)
        text = " ".join("".join(char for char in text if char.isprintable() or char.isspace()).split())
        return f"Ответ сервера: {text[:800]}" if text else "Сервер не прислал пояснения."

    def _parse(self, body: bytes, download_date: str) -> pd.DataFrame:
        """Проверяет CSV, торговый день, порядок времени, цены и объёмы."""
        try:
            text = body.decode("utf-8-sig")
            frame = pd.read_csv(StringIO(text), dtype={"<DATE>": str, "<TIME>": str})
        except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError):
            raise DownloadError("Финам не вернул корректный CSV; дневной архив не создан") from None
        required = {"<DATE>", "<TIME>", "<LAST>", "<VOL>"}
        if not required.issubset(frame.columns):
            raise DownloadError(
                "Вместо ожидаемого CSV получен другой ответ (возможно, HTML). "
                "Проверьте доступ к экспорту и FINAM_TOKEN."
            )
        if frame.empty:
            return pd.DataFrame(columns=["datetime", "last", "volume"])
        try:
            stamps = pd.to_datetime(frame["<DATE>"] + frame["<TIME>"].str.zfill(6),
                                    format="%Y%m%d%H%M%S", errors="raise")
            prices = pd.to_numeric(frame["<LAST>"], errors="raise")
            volumes = pd.to_numeric(frame["<VOL>"], errors="raise")
        except (ValueError, TypeError):
            raise DownloadError("CSV содержит некорректное время, цену или объём") from None
        if stamps.isna().any() or not stamps.dt.strftime("%Y%m%d").eq(download_date).all():
            raise DownloadError("Дата сделок в CSV не соответствует запрошенному дню")
        if not stamps.is_monotonic_increasing:
            raise DownloadError("Время сделок в CSV идёт назад; исходный порядок требует проверки")
        if (prices.isna().any() or volumes.isna().any() or
                prices.isin([float("inf"), float("-inf")]).any() or
                volumes.isin([float("inf"), float("-inf")]).any() or volumes.le(0).any()):
            raise DownloadError("CSV содержит пустую/бесконечную цену или некорректный объём")
        return make_timestamps_unique(pd.DataFrame({"datetime": stamps, "last": prices, "volume": volumes}))

    def run(self, download_date: str) -> Path | None:
        """Сохраняет проверенный CSV атомарно; существующие архивы не перезаписывает."""
        file_path = self.path_file(download_date)
        if file_path.exists():
            if not zipfile.is_zipfile(file_path):
                raise DownloadError(f"Существующий файл не является ZIP: {file_path}")
            print(f"Уже существует: {file_path}", flush=True)
            return file_path
        if self._guest_tokens is not None:
            try:
                previous_token = self.token
                self.token = self._guest_tokens.get_token()
            except GuestTokenError as error:
                raise DownloadError(str(error)) from None
            if self.token != previous_token:
                print("Финам: гостевой токен получен автоматически", flush=True)
        self.create_request_finam(download_date)
        frame = self._parse(self._fetch(), download_date)
        if frame.empty:
            print(f"{download_date}: CSV содержит только заголовок, сделок в ответе нет", flush=True)
            time.sleep(2)
            return None
        temporary = file_path.with_suffix(".zip.part")
        try:
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                with archive.open(f"{download_date}.csv", "w") as buffer:
                    frame.to_csv(buffer, index=False)
            temporary.replace(file_path)
        finally:
            temporary.unlink(missing_ok=True)
        print(f"Сохранено {len(frame):,} сделок: {file_path}", flush=True)
        time.sleep(2)
        return file_path


# Основные настройки для запуска кнопкой Run в VS Code.
dir_data = r"C:\data_quote\data_finam_MIX_tick_zip"
ticker = "SPFB.MIX"
market = 14
period = 1
daft = 9
start_date_range = datetime.date(2022, 1, 1)
end_date_range = datetime.date.today() - datetime.timedelta(days=1)


def main(argv=None) -> int:
    """Загружает выбранный диапазон и останавливается при неустранённой ошибке."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=datetime.date.fromisoformat, default=start_date_range,
                        help="Начало диапазона, ГГГГ-ММ-ДД")
    parser.add_argument("--end", type=datetime.date.fromisoformat, default=end_date_range,
                        help="Конец диапазона включительно, ГГГГ-ММ-ДД")
    parser.add_argument("--output", default=dir_data, help="Папка дневных ZIP")
    parser.add_argument("--timeout", type=float, default=30, help="Тайм-аут сетевой операции, секунды")
    parser.add_argument("--attempts", type=int, default=3, help="Число попыток запроса")
    args = parser.parse_args(argv)
    if args.end < args.start:
        parser.error("Конец диапазона раньше начала")
    try:
        loader = DownloadFinam(ticker, args.output, market, daft, period,
                               timeout=args.timeout, attempts=args.attempts)
        for day in pd.date_range(args.start, args.end):
            date_string = day.strftime("%Y%m%d")
            print(f"Дата: {date_string}", flush=True)
            loader.run(date_string)
    except (DownloadError, OSError, ValueError) as error:
        print(f"Загрузка остановлена: {error}", file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print("Загрузка прервана пользователем", file=sys.stderr)
        return 130
    print("Обработка диапазона завершена", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
