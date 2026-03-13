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

    # NOTE: avoid overly broad tokens like 'component' since HM tables have a column
    # 'Component Treatments' which is not the material/part number.
    material_i = pick([
        "material",
        "materialappearance",
        "material appearance",
        "materialcode",
        "item",
        "itemcode",
        "code",
        "part",
        "trim",
        "accessory",
        "accessories",
        "supplierarticle",
        "supplier article",
        "article",
    ])
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
        for kw in [
            "MATERIAL",
            "MATERIAL APPEARANCE",
            "ITEM",
            "DESCRIPTION",
            "DESC",
            "POSITION",
            "PLACEMENT",
            "TYPE",
            "COMPOSITION",
            "QTY",
            "QUANTITY",
            "UOM",
            "UNIT",
            "COLOR",
            "COLOUR",
            "SIZE",
            "CONSUMPTION",
            "WEIGHT",
            "SUPPLIER",
        ]:
            if re.search(r"\b" + re.escape(kw) + r"\b", blob):
                hits += 1
        return hits >= 3
    except Exception:
        return False


def _is_section_or_total_row(cells: List[str]) -> bool:
    try:
        blob = " ".join([c for c in (cells or []) if c]).strip().upper()
        if not blob:
            return True
        if re.fullmatch(r"(BOM|BILL\s+OF\s+MATERIALS)", blob, flags=re.IGNORECASE):
            return True
        if re.search(r"\bBILL\s+OF\s+MATERIAL\b", blob, flags=re.IGNORECASE):
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
    # HM Supplementary PPStructure signature (often doesn't include explicit 'BOM' keyword)
    hm_hits = 0
    for kw in ["POSITION", "PLACEMENT", "COMPOSITION", "CONSUMPTION", "SUPPLIER", "MATERIAL APPEARANCE"]:
        if re.search(r"\b" + re.escape(kw) + r"\b", b, flags=re.IGNORECASE):
            hm_hits += 1
    if re.search(r"\bTYPE\b", b, flags=re.IGNORECASE):
        hm_hits += 1
    if hm_hits >= 4:
        score += 8

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

    # Determine a robust start row (skip title/header repeats) early so HM heuristics can sample item rows.
    start_idx = 0
    try:
        while start_idx < len(rm) and isinstance(rm[start_idx], list):
            cells0 = [_cell_str(c) for c in rm[start_idx]]
            if _looks_like_header_row(rm[start_idx]) or _is_section_or_total_row(cells0):
                start_idx += 1
                continue
            break
    except Exception:
        start_idx = 0

    try:
        # HM Supplementary PPStructure often emits placeholder headers (col_6/col_8)
        # and/or merges the 3 columns (Composition/Construction/Consumption/Weight).
        # Use item-row content to infer positions for composition/consumption/weight.
        is_hm = isinstance(headers, list) and any(
            re.search(r"\bPOSITION\b", str(h or ""), flags=re.IGNORECASE) for h in headers
        )
        if is_hm and isinstance(rm, list):
            # Collect sample strings per column from the first item rows.
            col_w = 0
            for r in rm[start_idx : start_idx + 12]:
                if isinstance(r, list):
                    col_w = max(col_w, len(r))

            def _sample_col(i: int) -> List[str]:
                out: List[str] = []
                for r in rm[start_idx : start_idx + 18]:
                    if not isinstance(r, list):
                        continue
                    if i < 0 or i >= len(r):
                        continue
                    s = _cell_str(r[i])
                    if s:
                        out.append(s)
                return out

            comp_score: Dict[int, int] = {}
            cons_score: Dict[int, int] = {}
            weight_score: Dict[int, int] = {}

            for i in range(col_w):
                vals = _sample_col(i)
                if not vals:
                    continue
                blob = " ".join(vals).upper()

                # Composition: usually contains percent + fiber names.
                sc = 0
                if re.search(r"\b\d{1,3}\s*%\b", blob) is not None:
                    sc += 3
                if re.search(
                    r"\b(COTTON|POLYESTER|VISCOSE|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK|POLYAMIDE)\b",
                    blob,
                ) is not None:
                    sc += 1
                if sc:
                    comp_score[i] = sc

                # Weight: g/m, g/m2, gram/km, etc.
                sw = 0
                if re.search(r"\b\d+(?:[\.,]\d+)?\s*(?:G/M2|G/M|G/PC|G/PIECE|GSM)\b", blob) is not None:
                    sw += 3
                if re.search(r"\bGRAM\b", blob) is not None and re.search(r"\b/\s*(?:M|M2|KM|PC|PIECE)\b", blob) is not None:
                    sw += 2
                if sw:
                    weight_score[i] = sw

                # Consumption: numeric + length/per-unit units, but not a weight pattern.
                ss = 0
                if re.search(r"\b\d+(?:[\.,]\d+)?\b", blob) is not None:
                    if re.search(r"\b(M|CM|MM|YD|YARD|KM|PER\s*UNIT|/\s*UNIT)\b", blob) is not None:
                        ss += 2
                if re.search(r"\bG/", blob) is None and re.search(r"\bGSM\b", blob) is None:
                    if ss:
                        cons_score[i] = ss

            def _best(d: Dict[int, int]) -> Optional[int]:
                if not d:
                    return None
                return sorted(d.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

            i_comp = _best(comp_score)
            i_cons = _best(cons_score)
            i_w = _best(weight_score)

            if i_comp is not None:
                cols["composition"] = int(i_comp)
            if i_cons is not None:
                cols["consumption"] = int(i_cons)
            if i_w is not None:
                cols["weight"] = int(i_w)
    except Exception:
        pass

    try:
        # HM Supplementary PPStructure often emits placeholder headers (col_7/col_9)
        # and/or merges the 3 columns (Construction/Consumption/Weight) into one header.
        # Use early row content to infer positions for consumption/weight.
        if isinstance(headers, list) and any(re.search(r"\bPOSITION\b", str(h or ""), flags=re.IGNORECASE) for h in headers):
            if isinstance(rm, list) and rm:
                scan_rows = [r for r in rm[:6] if isinstance(r, list)]
                if scan_rows:
                    best_hits: Dict[str, int] = {}
                    for r in scan_rows:
                        for i, c in enumerate([_cell_str(x) for x in r]):
                            cu = c.upper()
                            if not cu:
                                continue
                            if re.fullmatch(r"CONSUMPTION", cu, flags=re.IGNORECASE):
                                best_hits["consumption"] = i
                            if re.fullmatch(r"WEIGHT", cu, flags=re.IGNORECASE):
                                best_hits["weight"] = i
                            if re.fullmatch(r"CONSTRUCTION", cu, flags=re.IGNORECASE):
                                best_hits["construction"] = i
                    if "consumption" in best_hits and "consumption" not in cols:
                        cols["consumption"] = int(best_hits["consumption"])
                    if "weight" in best_hits and "weight" not in cols:
                        cols["weight"] = int(best_hits["weight"])
    except Exception:
        pass

    if "material" not in cols and "description" not in cols:
        return None

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
