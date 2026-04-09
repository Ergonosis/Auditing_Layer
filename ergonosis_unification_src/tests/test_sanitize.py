"""Tests for exception sanitization utility."""

from src.utils.sanitize import sanitize_exception


class TestSanitizeException:
    def test_basic_extraction(self):
        exc = ValueError("something went wrong")
        result = sanitize_exception(exc)
        assert result == "ValueError: something went wrong"

    def test_multiline_keeps_first_line(self):
        exc = RuntimeError("first line\nsecond line\nthird line")
        result = sanitize_exception(exc)
        assert result == "RuntimeError: first line"

    def test_strips_bearer_token(self):
        exc = RuntimeError("Auth failed: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig")
        result = sanitize_exception(exc)
        assert "eyJ" not in result
        assert "[REDACTED]" in result

    def test_strips_key_value_secrets(self):
        exc = RuntimeError("Connection error token=abc123secret password=hunter2")
        result = sanitize_exception(exc)
        assert "abc123secret" not in result
        assert "hunter2" not in result
        assert result.count("[REDACTED]") == 2

    def test_strips_long_base64(self):
        long_b64 = "A" * 50
        exc = RuntimeError(f"API returned {long_b64} in response")
        result = sanitize_exception(exc)
        assert long_b64 not in result
        assert "[REDACTED]" in result

    def test_truncation(self):
        exc = RuntimeError("error: " + "word " * 120)
        result = sanitize_exception(exc, max_length=100)
        assert len(result) == 100
        assert result.endswith("...")

    def test_short_message_not_truncated(self):
        exc = ValueError("ok")
        result = sanitize_exception(exc, max_length=100)
        assert result == "ValueError: ok"
        assert not result.endswith("...")

    def test_empty_message(self):
        exc = RuntimeError("")
        result = sanitize_exception(exc)
        assert result == "RuntimeError: "

    def test_strips_jwt_token(self):
        jwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        exc = RuntimeError(f"Graph API returned token: {jwt}")
        result = sanitize_exception(exc)
        assert "eyJ" not in result
        assert "[REDACTED]" in result
