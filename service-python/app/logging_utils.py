import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

try:
    from fastapi import Request
except Exception:  # pragma: no cover
    Request = Any


def get_request_id(request: "Request") -> str:
    try:
        rid = None
        if request is not None:
            rid = request.headers.get("x-request-id") or request.headers.get("x-correlation-id")
        rid = str(rid or "").strip()
        return rid if rid else str(uuid.uuid4())
    except Exception:
        return str(uuid.uuid4())


def setup_logging(logger_name: str = "python_ocr") -> logging.Logger:
    level = str(os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(level=level)
    return logging.getLogger(logger_name)


def log_json(logger: logging.Logger, level: int, event: str, payload: Optional[Dict[str, Any]] = None) -> None:
    try:
        data: Dict[str, Any] = {"event": event}
        if isinstance(payload, dict):
            data.update(payload)
        msg = json.dumps(data, ensure_ascii=False)
        logger.log(level, msg)
    except Exception:
        try:
            logger.log(level, event)
        except Exception:
            pass
