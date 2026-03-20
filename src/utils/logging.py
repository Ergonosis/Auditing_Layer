"""Structured logging configuration"""

import logging
import json
from datetime import datetime
from typing import Any, Dict


class StructuredLogger:
    """Structured JSON logger for audit system"""

    def __init__(self, name: str, level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Only add the handler once per named logger — calling get_logger()
        # multiple times for the same module would otherwise stack handlers
        # and produce duplicate log lines.
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(JSONFormatter())
            self.logger.addHandler(handler)

    def log(self, level: str, message: str, **kwargs):
        """Log structured message"""
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "level": level.upper(),
            "message": message,
            **kwargs
        }

        getattr(self.logger, level.lower())(json.dumps(log_data))

    def info(self, message: str, **kwargs):
        self.log("info", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.log("warning", message, **kwargs)

    def error(self, message: str, **kwargs):
        self.log("error", message, **kwargs)

    def debug(self, message: str, **kwargs):
        self.log("debug", message, **kwargs)


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter"""

    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


# Global logger instance
def get_logger(name: str) -> StructuredLogger:
    """Get or create structured logger"""
    import os
    log_level = os.getenv("LOG_LEVEL", "INFO")
    return StructuredLogger(name, log_level)
