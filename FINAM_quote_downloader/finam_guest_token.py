"""Получает гостевой токен сайта Финама без логина, пароля и браузера.

Используется ZIP-загрузчиком автоматически. Пример запуска из quote_download:
    .venv\\Scripts\\python.exe FINAM_quote_downloader\\rts_finam_downloader_tick_to_zip_csv.py --start 2022-02-11 --end 2022-02-11

Протокол взят из публичного https://libs-cdn.finam.ru/auth-widget/@8.js,
настройки — https://ga-cdn.finam.ru/config/v1/finamRu.json (28.09.2026).
Это внутренний протокол сайта, который Финам может изменить.
Токен и случайный идентификатор устройства хранятся только в памяти процесса.
"""
import base64
import binascii
import json
import math
import re
import time
import uuid
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


CONFIG_URL = "https://ga-cdn.finam.ru/config/v1/finamRu.json"
GUEST_PROVIDER = "FINAMRU_DEVICE_ID"
MAX_RESPONSE = 1024 * 1024


class GuestTokenError(RuntimeError):
    """Сообщает об ошибке гостевой авторизации без раскрытия токена."""


def _varint(value):
    """Кодирует неотрицательное целое в формате protobuf varint."""
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def _field(number, value):
    """Кодирует строку или вложенное сообщение protobuf с указанным номером поля."""
    if isinstance(value, str):
        value = value.encode("utf-8")
    return _varint(number << 3 | 2) + _varint(len(value)) + value


def _read_varint(data, position):
    """Читает ограниченное 64 битами целое и возвращает следующую позицию."""
    value = 0
    for shift in range(0, 70, 7):
        if position >= len(data):
            raise ValueError("Обрезанное поле protobuf")
        byte = data[position]
        position += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, position
    raise ValueError("Слишком длинное поле protobuf")


def _fields(data):
    """Читает поля protobuf, отвергая неполные и неподдерживаемые сообщения."""
    position = 0
    while position < len(data):
        tag, position = _read_varint(data, position)
        number, wire_type = tag >> 3, tag & 7
        if number == 0:
            raise ValueError("Нулевой номер поля protobuf")
        if wire_type == 0:
            value, position = _read_varint(data, position)
        else:
            if wire_type == 2:
                size, position = _read_varint(data, position)
            elif wire_type in (1, 5):
                size = 8 if wire_type == 1 else 4
            else:
                raise ValueError("Неподдерживаемое поле protobuf")
            if position + size > len(data):
                raise ValueError("Обрезанное сообщение protobuf")
            value = data[position:position + size]
            position += size
        yield number, wire_type, value


def _decode_text_frames(body):
    """Декодирует gRPC-Web-Text, включая последовательность отдельных блоков base64."""
    body = b"".join(body.split())
    parts = re.findall(rb"[A-Za-z0-9+/]+={1,2}|[A-Za-z0-9+/]+$", body)
    if not body or b"".join(parts) != body:
        raise ValueError("Некорректный base64")
    return b"".join(base64.b64decode(part, validate=True) for part in parts)


def _response_token(body, headers):
    """Извлекает JWT только из полного успешного ответа gRPC-Web TokenResponse."""
    if "grpc-web-text" in headers.get("Content-Type", "").lower():
        body = _decode_text_frames(body)
    position = 0
    messages = []
    statuses = []
    if headers.get("grpc-status") is not None:
        statuses.append(headers.get("grpc-status"))
    while position < len(body):
        if position + 5 > len(body):
            raise ValueError("Обрезанный заголовок gRPC")
        flag = body[position]
        size = int.from_bytes(body[position + 1:position + 5], "big")
        position += 5
        if position + size > len(body):
            raise ValueError("Обрезанное тело gRPC")
        payload = body[position:position + size]
        position += size
        if flag == 0:
            messages.append(payload)
        elif flag == 128:
            for line in payload.decode("ascii").splitlines():
                name, separator, value = line.partition(":")
                if separator and name.strip().lower() == "grpc-status":
                    statuses.append(value.strip())
        else:
            raise ValueError("Неподдерживаемый блок gRPC")
    if not statuses or any(status != "0" for status in statuses) or len(messages) != 1:
        raise ValueError("Неуспешный ответ gRPC")
    token = None
    for number, wire_type, value in _fields(messages[0]):
        if number == 1 and wire_type == 2:
            for status_field, status_wire, status_value in _fields(value):
                if status_field == 1 and (status_wire != 0 or status_value != 0):
                    raise ValueError("Ошибка авторизации в TokenResponse")
        elif number == 2 and wire_type == 2:
            token = value.decode("ascii")
    if not token:
        raise ValueError("Нет токена в TokenResponse")
    return token


class GuestTokenClient:
    """Получает и повторно использует гостевой токен до приближения срока истечения."""

    def __init__(self, *, timeout=30, attempts=3):
        """Задаёт сетевые ограничения и временный идентификатор гостевой сессии."""
        if not math.isfinite(timeout) or timeout <= 0 or attempts < 1:
            raise ValueError("Некорректный тайм-аут или число попыток авторизации")
        self.timeout = timeout
        self.attempts = attempts
        self.device_id = str(uuid.uuid4())
        self.auth_host = ""
        self.token = ""
        self.refresh_at = 0

    def _request(self, request):
        """Выполняет ограниченный сетевой запрос и скрывает содержимое ошибок сервера."""
        host = urlsplit(request.full_url).hostname
        for attempt in range(1, self.attempts + 1):
            retryable = True
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    body = response.read(MAX_RESPONSE + 1)
                    if len(body) > MAX_RESPONSE:
                        raise GuestTokenError("Слишком большой ответ сервиса гостевой авторизации")
                    return body, response.headers
            except HTTPError as error:
                reason = f"HTTP {error.code}"
                retryable = error.code in (429, 500, 502, 503, 504)
                error.close()
            except (URLError, OSError, IncompleteRead) as error:
                timed_out = isinstance(error, TimeoutError) or isinstance(getattr(error, "reason", None), TimeoutError)
                reason = "тайм-аут" if timed_out else "сетевая ошибка"
            if not retryable or attempt == self.attempts:
                raise GuestTokenError(
                    f"Не удалось получить гостевой токен: {host}, {reason}; "
                    f"попыток: {attempt}, тайм-аут {self.timeout:g} с. "
                    "Проверьте доступ к Финаму без VPN."
                ) from None
            print(f"Гостевой токен: {reason}; повтор {attempt + 1}/{self.attempts}", flush=True)
            time.sleep(2 * attempt)
        raise GuestTokenError("Не удалось получить гостевой токен")

    def _load_config(self):
        """Читает адрес гостевой авторизации из той же конфигурации, что использует сайт."""
        body, _ = self._request(Request(CONFIG_URL, headers={"User-Agent": "Mozilla/5.0"}))
        try:
            variant = json.loads(body)["txAuth"]["finam"]
            if GUEST_PROVIDER not in variant["provider"]:
                raise ValueError("Гостевой провайдер отсутствует")
            host = variant["config"]["services"]["txauth"][0]
            parsed = urlsplit(host)
            if (parsed.scheme != "https" or not (parsed.hostname or "").endswith(".finam.ru")
                    or parsed.username or parsed.password or parsed.port not in (None, 443)
                    or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
                raise ValueError("Неподдерживаемый адрес авторизации")
        except (ValueError, KeyError, IndexError, TypeError, AttributeError):
            raise GuestTokenError("Изменился формат конфигурации гостевой авторизации Финама") from None
        self.auth_host = host.rstrip("/")

    def get_token(self):
        """Возвращает действующий токен либо получает новый без ввода учётных данных."""
        if self.token and time.time() < self.refresh_at:
            return self.token
        if not self.auth_host:
            self._load_config()
        # AuthRequest: provider=1, device=4. Логин и пароль остаются пустыми.
        # Device: local_id=1, name=2, info=3; ключ APP_NAME в info равен 26.
        app_info = b"\x08\x1a" + _field(2, "finamRu")
        device = (_field(1, self.device_id) + _field(2, "Python quote downloader")
                  + _field(3, app_info))
        message = _field(1, GUEST_PROVIDER) + _field(4, device)
        frame = b"\x00" + len(message).to_bytes(4, "big") + message
        request = Request(self.auth_host + "/grpc.txauth.TxAuthApi/Auth",
                          data=base64.b64encode(frame), headers={
                              "Content-Type": "application/grpc-web-text",
                              "Accept": "application/grpc-web-text",
                              "X-Grpc-Web": "1", "User-Agent": "Mozilla/5.0",
                              "Origin": "https://www.finam.ru",
                              "Referer": "https://www.finam.ru/",
                          })
        body, headers = self._request(request)
        try:
            token = _response_token(body, headers)
            sections = token.split(".")
            if len(sections) != 3:
                raise ValueError("Некорректный JWT")
            payload = sections[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            expires = float(claims["exp"])
            now = time.time()
            if not math.isfinite(expires) or expires <= now or claims["provider"] != GUEST_PROVIDER:
                raise ValueError("Неподходящий гостевой токен")
        except (ValueError, KeyError, TypeError, UnicodeError, binascii.Error):
            raise GuestTokenError(
                "Финам не выдал корректный гостевой токен. "
                "Возможно, изменился протокол сайта; можно задать FINAM_TOKEN вручную."
            ) from None
        # JWT разбирается только для срока обновления, подпись проверяет сервер экспорта.
        self.token = token
        self.refresh_at = expires - min(60, (expires - now) / 10)
        return token
