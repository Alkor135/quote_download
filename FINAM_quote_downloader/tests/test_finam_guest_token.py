"""Проверяет гостевой токен Финама без сети и настоящих учётных данных.

Пример запуска из корня quote_download:
    .venv\\Scripts\\python.exe -B -m unittest discover -s FINAM_quote_downloader -p "test_*.py" -v
"""
import base64
import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import finam_guest_token as source


CONFIG = {"txAuth": {"finam": {"provider": ["FINAMRU_DEVICE_ID"],
          "config": {"services": {"txauth": ["https://ftrr04.finam.ru"]}}}}}


def fixture_token(exp=4600):
    """Создаёт явно тестовый JWT для проверки срока жизни, без подписи сервера."""
    payload = base64.urlsafe_b64encode(json.dumps({
        "provider": "FINAMRU_DEVICE_ID", "exp": exp,
    }).encode()).rstrip(b"=").decode()
    return "test." + payload + ".test"


def auth_response(token, status=0):
    """Составляет независимый тестовый ответ TokenResponse с трейлером gRPC."""
    encoded = token.encode()
    assert len(encoded) < 128
    message = b"\x12" + bytes([len(encoded)]) + encoded
    trailers = f"grpc-status: {status}\r\n".encode()
    return (b"\x00" + len(message).to_bytes(4, "big") + message
            + b"\x80" + len(trailers).to_bytes(4, "big") + trailers)


class Response(io.BytesIO):
    """Предоставляет тело и HTTP-заголовки тестового ответа."""

    def __init__(self, data, content_type="application/grpc-web+proto"):
        """Сохраняет тело ответа и его тип содержимого."""
        super().__init__(data)
        self.headers = {"Content-Type": content_type}


class GuestTokenTests(unittest.TestCase):
    """Проверяет запрос, повторное использование, обновление и отказы сервера."""

    def setUp(self):
        """Фиксирует часы и создаёт отдельный клиент гостевой сессии."""
        clock = patch.object(source.time, "time", return_value=1000)
        self.clock = clock.start()
        self.addCleanup(clock.stop)
        delay = patch.object(source.time, "sleep")
        delay.start()
        self.addCleanup(delay.stop)
        self.client = source.GuestTokenClient(timeout=2, attempts=1)

    def test_guest_request_and_cached_token(self):
        """Без логина получается токен, повторное обращение обходится без сети."""
        token = fixture_token()
        replies = [Response(json.dumps(CONFIG).encode(), "application/json"),
                   Response(auth_response(token))]
        with patch.object(source, "urlopen", side_effect=replies) as request:
            self.assertEqual(self.client.get_token(), token)
            self.assertEqual(self.client.get_token(), token)
        self.assertEqual(request.call_count, 2)
        req = request.call_args.args[0]
        self.assertEqual(req.full_url, "https://ftrr04.finam.ru/grpc.txauth.TxAuthApi/Auth")
        self.assertEqual(req.get_method(), "POST")
        wire = base64.b64decode(req.data, validate=True)
        self.assertEqual(int.from_bytes(wire[1:5], "big"), len(wire) - 5)
        self.assertTrue(wire[5:].startswith(b"\x0a\x11FINAMRU_DEVICE_ID"))
        self.assertIn(b"finamRu", wire)
        self.assertIn(self.client.device_id.encode(), wire)
        self.assertNotIn(b"password", wire)
        self.assertEqual(request.call_args.kwargs["timeout"], 2)

    def test_expiring_token_is_replaced_with_same_device(self):
        """Перед истечением токена повторно авторизуется та же гостевая сессия."""
        replies = [Response(json.dumps(CONFIG).encode()), Response(auth_response(fixture_token())),
                   Response(auth_response(fixture_token(8200)))]
        with patch.object(source, "urlopen", side_effect=replies) as request:
            self.client.get_token()
            self.clock.return_value = 4590
            self.assertEqual(self.client.get_token(), fixture_token(8200))
        self.assertEqual(request.call_count, 3)
        self.assertEqual(request.call_args_list[1].args[0].data, request.call_args_list[2].args[0].data)

    def test_text_response_can_have_separately_encoded_chunks(self):
        """Обрабатываются раздельно закодированные блоки ответа gRPC-Web-Text."""
        wire = auth_response(fixture_token())
        split = 5 + int.from_bytes(wire[1:5], "big")
        body = base64.b64encode(wire[:split]) + base64.b64encode(wire[split:])
        replies = [Response(json.dumps(CONFIG).encode()),
                   Response(body, "application/grpc-web-text")]
        with patch.object(source, "urlopen", side_effect=replies):
            self.assertEqual(self.client.get_token(), fixture_token())

    def test_rejected_or_malformed_reply_does_not_become_token(self):
        """Ошибка протокола, отказ, истёкший или отсутствующий JWT останавливают загрузку."""
        for body in [auth_response(fixture_token(), 7), b"<html>Denied</html>",
                     auth_response(fixture_token())[:-1], auth_response(""),
                     auth_response(fixture_token(999)), auth_response("invalid")]:
            with self.subTest(body_length=len(body)):
                client = source.GuestTokenClient(timeout=2, attempts=1)
                replies = [Response(json.dumps(CONFIG).encode()), Response(body)]
                with patch.object(source, "urlopen", side_effect=replies):
                    with self.assertRaises(source.GuestTokenError) as caught:
                        client.get_token()
                self.assertNotIn(fixture_token(), str(caught.exception))

    def test_auth_timeout_has_host_and_no_secret(self):
        """Тайм-аут авторизации отличается от недоступности сервера котировок."""
        replies = [Response(json.dumps(CONFIG).encode()), TimeoutError("secret")]
        with patch.object(source, "urlopen", side_effect=replies):
            with self.assertRaises(source.GuestTokenError) as caught:
                self.client.get_token()
        self.assertIn("ftrr04.finam.ru", str(caught.exception))
        self.assertIn("тайм-аут", str(caught.exception))
        self.assertNotIn("secret", str(caught.exception))

    def test_http_error_does_not_leak_response(self):
        """Диагностика гостевой авторизации не выводит содержимое ответа сервера."""
        error = HTTPError("https://ga-cdn.finam.ru/", 403, "Forbidden", {}, io.BytesIO(b"secret"))
        with patch.object(source, "urlopen", side_effect=error):
            with self.assertRaisesRegex(source.GuestTokenError, "HTTP 403") as caught:
                self.client.get_token()
        self.assertNotIn("secret", str(caught.exception))

    def test_temporary_error_is_retried_with_limit(self):
        """Временный сбой допускает повтор в пределах заданного ограничения."""
        self.client.attempts = 2
        replies = [TimeoutError(), Response(json.dumps(CONFIG).encode()), Response(auth_response(fixture_token()))]
        with patch.object(source, "urlopen", side_effect=replies) as request:
            self.assertEqual(self.client.get_token(), fixture_token())
        self.assertEqual(request.call_count, 3)

    def test_config_cannot_redirect_auth_to_other_site(self):
        """Адрес из конфигурации ограничен HTTPS-серверами домена Финама."""
        bad = {"txAuth": {"finam": {"provider": ["FINAMRU_DEVICE_ID"],
               "config": {"services": {"txauth": ["https://finam.ru.example.org"]}}}}}
        with patch.object(source, "urlopen", return_value=Response(json.dumps(bad).encode())) as request:
            with self.assertRaises(source.GuestTokenError):
                self.client.get_token()
        self.assertEqual(request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
