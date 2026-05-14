"""Testes de JsonFormatter."""
import json
import logging
import unittest

from streaming.logging.json_formatter import JsonFormatter, setup_logging


class TestJsonFormatter(unittest.TestCase):

    def setUp(self):
        self.formatter = JsonFormatter()

    def _make_record(self, msg, level=logging.INFO, **extra):
        record = logging.LogRecord(
            name="test", level=level, pathname=__file__, lineno=1,
            msg=msg, args=(), exc_info=None,
        )
        for k, v in extra.items():
            setattr(record, k, v)
        return record

    def test_formats_basic_fields(self):
        record = self._make_record("hello")
        out = json.loads(self.formatter.format(record))
        self.assertEqual(out["level"], "INFO")
        self.assertEqual(out["logger"], "test")
        self.assertEqual(out["message"], "hello")
        self.assertIn("timestamp", out)

    def test_includes_extra_fields(self):
        record = self._make_record("event", topic="foo", count=42)
        out = json.loads(self.formatter.format(record))
        self.assertEqual(out["topic"], "foo")
        self.assertEqual(out["count"], 42)

    def test_excludes_reserved_fields(self):
        record = self._make_record("event")
        out = json.loads(self.formatter.format(record))
        for reserved in ("name", "msg", "args", "pathname", "lineno"):
            self.assertNotIn(reserved, out)

    def test_handles_exception(self):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname=__file__, lineno=1,
                msg="oops", args=(), exc_info=sys.exc_info(),
            )
        out = json.loads(self.formatter.format(record))
        self.assertIn("exception", out)
        self.assertIn("ValueError: boom", out["exception"])

    def test_setup_logging_returns_configured_logger(self):
        logger = setup_logging("test_setup", "DEBUG")
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertTrue(any(isinstance(h, logging.StreamHandler) for h in logger.handlers))


if __name__ == "__main__":
    unittest.main()