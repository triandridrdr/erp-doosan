import re
from typing import Any, Dict, List, Optional, Tuple


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _cell_str(x: Any) -> str:
    return " ".join(str(x or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _to_number(s: str) -> Optional[float]:
    t = (s or "").strip()
    if not t:
        return None
    t = t.replace(" ", "")
    t = t.replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d+)?", t):
        return None
    try:
        return float(t)
    except Exception:
        return None


def _infer_bom_columns(headers: List[str]) -> Dict[str, int]:
    cols: Dict[str, int] = {}
    norm = [_norm_key(h) for h in headers]

    def pick(keys: List[str]) -> Optional[int]:
        for k in keys:
            nk = _norm_key(k)
            for i, h in enumerate(norm):
                if h == nk:
                    return i
        for k in keys:
            nk = _norm_key(k)
            for i, h in enumerate(norm):
                if nk and h and (nk in h or h in nk):
                    return i
        return None

    material_i = pick(["material", "materialcode", "item", "itemcode", "code", "part", "component", "trim", "accessory", "accessories"])
    desc_i = pick(["description", "desc", "materialdescription", "itemdescription", "name"])
    color_i = pick(["color", "colour", "col"])
    size_i = pick(["size", "sizespec", "dimension"])
    qty_i = pick(["qty", "quantity", "consumption", "usage", "req", "required"])
    uom_i = pick(["uom", "unit", "unitofmeasure", "measure"])

    if material_i is not None:
        cols["material"] = int(material_i)
    if desc_i is not None:
        cols["description"] = int(desc_i)
    if color_i is not None:
        cols["color"] = int(color_i)
    if size_i is not None:
        cols["size"] = int(size_i)
    if qty_i is not None:
        cols["qty"] = int(qty_i)
    if uom_i is not None:
        cols["uom"] = int(uom_i)

    return cols


def _score_bom_table(headers: List[str], rows_matrix: List[List[Any]]) -> int:
    blob = " ".join([str(h or "") for h in (headers or [])]).upper()
    head = " ".join([str(x or "") for r in (rows_matrix or [])[:6] if isinstance(r, list) for x in r]).upper()
    b = (blob + " " + head).strip()
    if not b:
        return 0
    score = 0
    if re.search(r"\b(BOM|BILL\s+OF\s+MATERIALS)\b", b, flags=re.IGNORECASE):
        score += 6
    if re.search(r"\b(MATERIAL|FABRIC|TRIM|ACCESSOR(Y|IES)|COMPONENT)\b", b, flags=re.IGNORECASE):
        score += 4
    if re.search(r"\b(QTY|QUANTITY|CONSUMPTION|USAGE)\b", b, flags=re.IGNORECASE):
        score += 2
    if re.search(r"\b(UOM|UNIT)\b", b, flags=re.IGNORECASE):
        score += 1
    if re.search(r"\b(COLOU?R|COLOR)\b", b, flags=re.IGNORECASE):
        score += 1
    return score


def build_bom_payload(*, tables: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(tables, list) or not tables:
        return None

    best: Optional[Tuple[int, Dict[str, Any]]] = None
    for t in tables:
        if not isinstance(t, dict):
            continue
        headers = t.get("headers") or []
        rm = t.get("rows_matrix") or []
        if not isinstance(headers, list) or not isinstance(rm, list):
            continue
        if len(rm) < 2:
            continue
        score = _score_bom_table([str(h or "") for h in headers], rm)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, t)

    if best is None or best[0] < 6:
        return None

    tbl = best[1]
    headers = [str(h or "") for h in (tbl.get("headers") or [])]
    rm = tbl.get("rows_matrix") or []
    cols = _infer_bom_columns(headers)

    if "material" not in cols and "description" not in cols:
        return None

    lines: List[Dict[str, Any]] = []
    for r in rm[1:] if rm and isinstance(rm[0], list) and any(_norm_key(x) in {_norm_key(h) for h in headers} for x in rm[0]) else rm:
        if not isinstance(r, list):
            continue
        cells = [_cell_str(c) for c in r]
        if not any(cells):
            continue

        def get(col: str) -> str:
            idx = cols.get(col)
            if idx is None:
                return ""
            if idx < 0 or idx >= len(cells):
                return ""
            return cells[idx]

        material = get("material")
        desc = get("description")
        qty_raw = get("qty")
        uom = get("uom")
        color = get("color")
        size = get("size")

        if not (material or desc):
            continue

        qty_num = _to_number(qty_raw)
        line: Dict[str, Any] = {
            "material": material,
            "description": desc,
            "qty": qty_num if qty_num is not None else (qty_raw or ""),
            "uom": uom,
            "color": color,
            "size": size,
        }
        line = {k: v for k, v in line.items() if v not in (None, "")}
        if line:
            lines.append(line)

    if not lines:
        return None

    return {"lines": lines, "source": {"table_kind": tbl.get("table_kind"), "score": best[0]}}
