import os

from celery import Celery


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v else default


celery_app = Celery(
    "service_python",
    broker=_env("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=_env("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
