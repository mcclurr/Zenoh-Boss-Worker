import logging
import json
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = (
            datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

        payload = record.payload if hasattr(record, "payload") else {}

        log_entry = {
            "timestamp": timestamp,
            "level": record.levelname,
            "fields": {
                "message": record.getMessage(),
                "payload": payload,
            },
        }

        if record.exc_info:
            log_entry["fields"]["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)

def init_logging(prefix: str) -> logging.Logger:
    """
    Initialize structured logging.

    Creates:
        out/YYYYMMDD_HHMMSS-<prefix>.log

    Returns:
        configured logger
    """
    log_dir = Path("out")
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = log_dir / f"{ts}-{prefix}.log"

    logger = logging.getLogger(prefix)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent duplicate root logging

    # Prevent duplicate handlers if reinitialized
    if logger.handlers:
        return logger

    formatter = JsonFormatter()

    file_handler = logging.FileHandler(filename, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized -> {filename}")

    return logger