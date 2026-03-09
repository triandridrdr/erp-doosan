from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class PreprocessMeta(BaseModel):
    enabled: bool
    mode: Optional[str] = None
    target: Optional[str] = None


class DocumentTimings(BaseModel):
    total_sec: Optional[float] = None


class DocumentMeta(BaseModel):
    request_id: str
    filename: str
    engine: str
    preprocess: PreprocessMeta
    page_count: int
    timings: DocumentTimings


class OcrResponse(BaseModel):
    schema_version: str
    document_meta: DocumentMeta
    warnings: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    filename: Optional[str] = None
    engine: Optional[str] = None
    pages: Optional[List[Dict[str, Any]]] = None
    text: Optional[str] = None
    tables: Optional[List[Dict[str, Any]]] = None
    fields: Optional[List[Dict[str, Any]]] = None
    field_pairs: Optional[List[Dict[str, Any]]] = None

    sales_order_payload: Optional[Dict[str, Any]] = None
    bom_payload: Optional[Dict[str, Any]] = None
