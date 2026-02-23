from typing import Any, Dict

from .celery_app import celery_app


@celery_app.task(name="ocr.extract")
def ocr_extract_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    from .main import ocr_extract_sync

    return ocr_extract_sync(payload)
