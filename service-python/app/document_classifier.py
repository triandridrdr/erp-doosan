import re
from typing import Any, Dict


def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().upper()


def classify_document(*, text: str, filename: str | None = None) -> Dict[str, Any]:
    t = _norm_text(text)
    fn = (filename or "").strip().lower()

    if "ZARA" in t or "INDITEX" in t:
        return {"template": "zara", "confidence": 0.7, "signals": ["ZARA/INDITEX"]}

    if "H&M" in t or "HENNES" in t:
        return {"template": "hm", "confidence": 0.65, "signals": ["H&M"]}

    if "MANGO" in t:
        return {"template": "mango", "confidence": 0.6, "signals": ["MANGO"]}

    if fn.endswith(".pdf") and "PURCHASE ORDER" in t:
        return {"template": "generic_po_pdf", "confidence": 0.55, "signals": ["PDF", "PURCHASE ORDER"]}

    return {"template": "generic", "confidence": 0.2, "signals": []}
