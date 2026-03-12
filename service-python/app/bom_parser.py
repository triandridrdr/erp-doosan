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


def _norm_uom(u: str) -> str:
    s = _cell_str(u).upper()
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9]", "", s)
    m = {
        "PCS": "PCS",
        "PC": "PCS",
        "EA": "PCS",
        "EACH": "PCS",
        "UNIT": "PCS",
        "M": "M",
        "MTR": "M",
        "METER": "M",
        "METRE": "M",
        "CM": "CM",
        "MM": "MM",
        "YD": "YD",
        "YARD": "YD",
        "FT": "FT",
        "KG": "KG",
        "G": "G",
        "SET": "SET",
        "PACK": "PACK",
        "ROLL": "ROLL",
    }
    return m.get(s, s)


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
    position_i = pick(["position"])
    placement_i = pick(["placement"])
    type_i = pick(["type"])
    composition_i = pick(["composition"])
    consumption_i = pick(["consumption", "consumptionperunit", "consumptionperunit", "consumptionperunit", "usage", "req", "required"])
    color_i = pick(["color", "colour", "col"])
    size_i = pick(["size", "sizespec", "dimension"])
    qty_i = pick(["qty", "quantity", "consumption", "usage", "req", "required"])
    uom_i = pick(["uom", "unit", "unitofmeasure", "measure"])

    if material_i is not None:
        cols["material"] = int(material_i)
    if desc_i is not None:
        cols["description"] = int(desc_i)
    if position_i is not None:
        cols["position"] = int(position_i)
    if placement_i is not None:
        cols["placement"] = int(placement_i)
    if type_i is not None:
        cols["type"] = int(type_i)
    if composition_i is not None:
        cols["composition"] = int(composition_i)
    if consumption_i is not None:
        cols["consumption"] = int(consumption_i)
    if color_i is not None:
        cols["color"] = int(color_i)
    if size_i is not None:
        cols["size"] = int(size_i)
    if qty_i is not None:
        cols["qty"] = int(qty_i)
    if uom_i is not None:
        cols["uom"] = int(uom_i)

    return cols


def _looks_like_header_row(row: List[Any]) -> bool:
    try:
        cells = [_cell_str(c) for c in (row or [])]
        cells = [c for c in cells if c]
        if len(cells) < 2:
            return False
        blob = " ".join(cells).upper()
        hits = 0
        for kw in ["MATERIAL", "ITEM", "DESCRIPTION", "DESC", "QTY", "QUANTITY", "UOM", "UNIT", "COLOR", "COLOUR", "SIZE", "CONSUMPTION"]:
            if re.search(r"\b" + re.escape(kw) + r"\b", blob):
                hits += 1
        return hits >= 2
    except Exception:
        return False


def _is_section_or_total_row(cells: List[str]) -> bool:
    try:
        blob = " ".join([c for c in (cells or []) if c]).strip().upper()
        if not blob:
            return True
        if re.fullmatch(r"(BOM|BILL\s+OF\s+MATERIALS)", blob, flags=re.IGNORECASE):
            return True
        if re.search(r"\b(TOTAL|SUBTOTAL|GRAND\s+TOTAL)\b", blob, flags=re.IGNORECASE):
            return True
        if re.search(r"\b(MAIN\s+FABRIC|SECONDARY\s+FABRIC|EMBELLISHMENT|LINING|INTERLINING|TRIM|TRIMMINGS|ACCESSOR(Y|IES)|LABEL|PACKING|PACKAGING)\b", blob, flags=re.IGNORECASE):
            # Often a section header, not a line item
            if len(blob) <= 60 and not re.search(r"\d", blob):
                return True
        # Skip rows that are purely column titles repeated
        if _looks_like_header_row([blob]):
            return True
        return False
    except Exception:
        return False


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
    if not isinstance(rm, list):
        return None

    # If headers are missing/garbled, try to infer headers from the first rows_matrix row
    if (not headers or all(not str(h or "").strip() for h in headers)) and rm and isinstance(rm[0], list) and _looks_like_header_row(rm[0]):
        headers = [str(x or "") for x in rm[0]]

    cols = _infer_bom_columns(headers)

    if "material" not in cols and "description" not in cols:
        return None

    start_idx = 0
    if rm and isinstance(rm[0], list) and _looks_like_header_row(rm[0]):
        start_idx = 1

    lines: List[Dict[str, Any]] = []
    seen_keys = set()
    for r in rm[start_idx:]:
        if not isinstance(r, list):
            continue
        cells = [_cell_str(c) for c in r]
        if not any(cells):
            continue

        if _is_section_or_total_row(cells):
            continue

        def get(col: str) -> str:
            idx = cols.get(col)
            if idx is None:
                return ""
            if idx < 0 or idx >= len(cells):
                return ""
            return cells[idx]

        position = get("position")
        placement = get("placement")
        typ = get("type")
        material = get("material")
        desc = get("description")
        composition = get("composition")
        consumption_raw = get("consumption")
        qty_raw = get("qty")
        uom = _norm_uom(get("uom"))
        color = get("color")
        size = get("size")

        if not (material or desc):
            continue

        component = " ".join([x for x in [position, placement, typ] if x]).strip()
        if not component:
            component = material or desc

        consumption_qty = None
        consumption_uom = ""
        if consumption_raw:
            mcons = re.search(r"(-?\d+(?:[\.,]\d+)?)", consumption_raw.replace(" ", ""))
            if mcons:
                consumption_qty = _to_number(mcons.group(1))
            mu = re.search(r"\b([A-Za-z]{1,6})\b", consumption_raw)
            if mu:
                consumption_uom = _norm_uom(mu.group(1))
        if not uom and consumption_uom:
            uom = consumption_uom

        qty_num = _to_number(qty_raw)
        line: Dict[str, Any] = {
            "component": component,
            "material": material,
            "description": desc,
            "composition": composition,
            "consumption": consumption_qty if consumption_qty is not None else (consumption_raw or ""),
            "qty": qty_num if qty_num is not None else (qty_raw or ""),
            "uom": uom,
            "color": color,
            "size": size,
        }
        line = {k: v for k, v in line.items() if v not in (None, "")}
        if line:
            k = (
                _norm_key(str(line.get("material") or ""))
                + "|"
                + _norm_key(str(line.get("description") or ""))
                + "|"
                + _norm_key(str(line.get("color") or ""))
                + "|"
                + _norm_key(str(line.get("size") or ""))
            )
            if k.strip("|") and k not in seen_keys:
                seen_keys.add(k)
                lines.append(line)

    if not lines:
        return None

    return {"lines": lines, "source": {"table_kind": tbl.get("table_kind"), "score": best[0]}}
