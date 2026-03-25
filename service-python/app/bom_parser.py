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
        "PERUNIT": "PER",
        "PER": "PER",
        "M": "M",
        "MTR": "M",
        "METER": "M",
        "METRE": "M",
        "CM": "CM",
        "MM": "MM",
        "YD": "YD",
        "YARD": "YD",
        "KM": "KM",
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
    weight_i = pick(["weight", "grammage", "g/m", "g/m2", "gsm"])
    supplier_i = pick([
        "materialsupply",
        "materialsupplier",
        "material supplier",
        "supplier",
        "supplierarticle",
        "supplier article",
    ])
    production_unit_i = pick([
        "productionunit",
        "production unit",
        "productionunits",
        "production units",
        "factory",
        "mill",
    ])
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
    if weight_i is not None:
        cols["weight"] = int(weight_i)
    if supplier_i is not None:
        cols["supplier"] = int(supplier_i)
    if production_unit_i is not None:
        cols["production_unit"] = int(production_unit_i)
    if color_i is not None:
        cols["color"] = int(color_i)
    if size_i is not None:
        cols["size"] = int(size_i)
    if qty_i is not None:
        cols["qty"] = int(qty_i)
    if uom_i is not None:
        cols["uom"] = int(uom_i)

    return cols


def _extract_weight_value(s: str) -> str:
    t = _cell_str(s)
    if not t:
        return ""
    tu = t.upper()
    if re.search(r"\b\d+(?:[\.,]\d+)?\s*(?:G/M2|G/M|G/PC|G/PIECE|GSM)\b", tu) is not None:
        return t
    if re.search(r"\b\d+(?:[\.,]\d+)?\s*(?:GRAM)\b", tu) is not None and re.search(r"\b/\s*(?:M|M2|KM|PC|PIECE)\b", tu) is not None:
        return t
    return ""


def _looks_like_supplier(s: str) -> bool:
    tu = _cell_str(s).upper()
    if not tu:
        return False
    return (
        re.search(
            r"\b(PT\.?|LTD\.?|LIMITED|TRADING|CO\.?|COMPANY|CORP\.?|CORPORATION|GMBH|BV|S\.A\.|S\.P\.A\.|INC\.?|LLC|CO\.,\s*LTD)\b",
            tu,
        )
        is not None
    )


def _looks_like_consumption_value(s: str) -> bool:
    tu = _cell_str(s).upper()
    if not tu:
        return False
    # Avoid confusing composition percentages (e.g. '100% POLYESTER') as consumption.
    if "%" in tu:
        return False
    if re.search(r"\b\d+(?:[\.,]\d+)?\b", tu) is None:
        return False
    if re.search(r"\b(PER\s*UNIT|/\s*UNIT)\b", tu) is not None:
        return True
    if re.search(r"\b(M|CM|MM|YD|YARD|KM|PCS|PC|EA|UNIT)\b", tu) is not None:
        return True
    return False


def _quality_score_line(line: Dict[str, Any], text_fields: Optional[List[str]] = None) -> int:
    try:
        tf = text_fields or []
        filled = sum(1 for v in (line or {}).values() if v not in (None, ""))
        penalty = 0
        for k in tf:
            v = str((line or {}).get(k) or "")
            if not v:
                continue
            penalty += max(0, len(v) - 24)
            if sum(1 for ch in v if ch.isdigit()) >= 6:
                penalty += 20
        return filled * 10 - penalty
    except Exception:
        return 0


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
        if _looks_like_header_row(cells):
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

    candidates: List[Tuple[int, int, Dict[str, Any]]] = []
    for t in tables:
        if not isinstance(t, dict):
            continue
        headers0 = t.get("headers") or []
        rm0 = t.get("rows_matrix") or []
        if not isinstance(headers0, list) or not isinstance(rm0, list):
            continue
        if len(rm0) < 2:
            continue
        score0 = _score_bom_table([str(h or "") for h in headers0], rm0)
        if score0 < 6:
            continue
        page0 = t.get("page")
        try:
            page_i = int(page0) if page0 is not None else 10**9
        except Exception:
            page_i = 10**9
        candidates.append((page_i, -int(score0), t))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1]))

    def _extract_lines_from_table(
        tbl: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        headers = [str(h or "") for h in (tbl.get("headers") or [])]
        rm = tbl.get("rows_matrix") or []
        if not isinstance(rm, list):
            return ([], [], 0)

        if (not headers or all(not str(h or "").strip() for h in headers)) and rm and isinstance(rm[0], list) and _looks_like_header_row(rm[0]):
            headers = [str(x or "") for x in rm[0]]

        cols = _infer_bom_columns(headers)

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
            is_hm = isinstance(headers, list) and any(
                re.search(r"\bPOSITION\b", str(h or ""), flags=re.IGNORECASE) for h in headers
            )
            if is_hm and isinstance(rm, list):
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

                    sw = 0
                    if re.search(r"\b\d+(?:[\.,]\d+)?\s*(?:G/M2|G/M|G/PC|G/PIECE|GSM)\b", blob) is not None:
                        sw += 3
                    if re.search(r"\bGRAM\b", blob) is not None and re.search(r"\b/\s*(?:M|M2|KM|PC|PIECE)\b", blob) is not None:
                        sw += 2
                    if sw:
                        weight_score[i] = sw

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
            return ([], [], 0)

        cons_map: Dict[str, Dict[str, Any]] = {}
        cons_q: Dict[str, int] = {}
        prod_map: Dict[str, Dict[str, Any]] = {}
        prod_q: Dict[str, int] = {}
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
            weight_raw = get("weight")
            supplier = get("supplier")
            production_unit = get("production_unit")
            qty_raw = get("qty")
            uom = _norm_uom(get("uom"))
            color = get("color")
            size = get("size")

            # Fallback: if consumption column is missing/misaligned, scan row cells for a
            # consumption-like value (e.g. '0.56 km').
            #
            # Important: in HM Supplementary, inferred 'consumption' column can sometimes
            # point to 'construction', so we also scan when consumption_raw exists but does
            # not look like consumption.
            if (not consumption_raw) or (not _looks_like_consumption_value(consumption_raw)):
                try:
                    best_cc = ""

                    def _is_unit_token(tok: str) -> bool:
                        u = _norm_uom(tok)
                        return u in {"M", "CM", "MM", "YD", "KM", "PCS", "PER", "UNIT"}

                    for i_cc, cc in enumerate(cells):
                        if not cc:
                            continue
                        if _extract_weight_value(cc):
                            continue
                        if _looks_like_consumption_value(cc):
                            best_cc = cc
                            break

                        # Even if a cell contains composition percentages, it can also contain
                        # consumption at the end (e.g. '80% ... 1.203 yd'). Try to detect that.
                        if re.search(
                            r"(-?\d+(?:[\.,]\d+)?)\s*(KM|YD|YARD|M|CM|MM|PCS|PC|EA|UNIT|PER)\b",
                            cc.upper(),
                        ) is not None:
                            best_cc = cc
                            break

                        # Split case: numeric cell followed by unit cell.
                        if re.fullmatch(r"-?\d+(?:[\.,]\d+)?", cc.replace(" ", "")) is not None:
                            if i_cc + 1 < len(cells):
                                uu = cells[i_cc + 1]
                                if uu and _is_unit_token(uu):
                                    best_cc = f"{cc} {uu}"
                                    break
                            if i_cc > 0:
                                uu = cells[i_cc - 1]
                                if uu and _is_unit_token(uu):
                                    best_cc = f"{cc} {uu}"
                                    break

                    if best_cc:
                        consumption_raw = best_cc
                except Exception:
                    pass

            # HM Supplementary rows sometimes have Type filled (e.g. 'Thread Trim')
            # while Material Appearance/Description cells are empty. Keep these rows
            # if they carry useful values like consumption/composition.
            if not (material or desc):
                if (typ or placement or position) and (composition or consumption_raw or qty_raw):
                    desc = typ or placement or position
                else:
                    continue

            component = " ".join([x for x in [position, placement, typ] if x]).strip()
            if not component:
                component = material or desc

            consumption_qty = None
            consumption_uom = ""
            if consumption_raw:
                # Prefer patterns where a unit token is adjacent to a numeric value.
                # This handles cells that contain both construction and consumption.
                m_qty_uom = None
                try:
                    m_qty_uom = list(
                        re.finditer(
                            r"(-?\d+(?:[\.,]\d+)?)\s*(KM|YD|YARD|M|CM|MM|PCS|PC|EA|UNIT|PER)\b",
                            consumption_raw.upper(),
                        )
                    )
                except Exception:
                    m_qty_uom = None
                if m_qty_uom:
                    m_last = m_qty_uom[-1]
                    consumption_qty = _to_number(m_last.group(1))
                    consumption_uom = _norm_uom(m_last.group(2))
                else:
                    mcons = re.search(r"(-?\d+(?:[\.,]\d+)?)", consumption_raw.replace(" ", ""))
                    if mcons:
                        consumption_qty = _to_number(mcons.group(1))
                    mu = re.search(r"\b(KM|YD|YARD|M|CM|MM|PCS|PC|EA|UNIT|PER)\b", consumption_raw.upper())
                    if mu:
                        consumption_uom = _norm_uom(mu.group(1))
                # If the value looks like a percentage (composition), don't treat it as consumption
                # unless we also have a real unit from the uom column.
                if "%" in consumption_raw and not (uom or consumption_uom):
                    consumption_qty = None
                    consumption_uom = ""
            if not uom and consumption_uom:
                uom = consumption_uom

            weight = _extract_weight_value(weight_raw)
            if (not weight) and composition:
                weight = _extract_weight_value(composition)
            if (not weight) and consumption_raw:
                weight = _extract_weight_value(consumption_raw)

            qty_num = _to_number(qty_raw)

            # Row-level classification
            supplier_blob = " ".join([x for x in [supplier, production_unit, qty_raw] if x]).strip()
            is_supplier_row = _looks_like_supplier(supplier_blob) or _looks_like_supplier(material) or _looks_like_supplier(desc)
            # Only consider numeric-only cells as consumption when we can attach a valid unit.
            has_consumption_unit = bool(uom or consumption_uom)
            is_consumption_row = _looks_like_consumption_value(consumption_raw) or (consumption_qty is not None and has_consumption_unit)

            # If supplier is present, it should go to Production Units.
            if is_supplier_row:
                line_p: Dict[str, Any] = {
                    "component": component,
                    "supplier": supplier or (qty_raw or ""),
                    "composition": composition,
                    "production_unit": production_unit,
                    "weight": weight,
                }
                line_p = {k: v for k, v in line_p.items() if v not in (None, "")}
                if line_p:
                    kp = (
                        _norm_key(str(line_p.get("component") or ""))
                        + "|"
                        + _norm_key(str(line_p.get("supplier") or ""))
                        + "|"
                        + _norm_key(str(line_p.get("production_unit") or ""))
                        + "|"
                        + _norm_key(str(line_p.get("composition") or ""))
                    )
                    if kp.strip("|"):
                        q = _quality_score_line(line_p, ["supplier", "production_unit", "composition"])
                        if (kp not in prod_map) or (q > int(prod_q.get(kp, -10**9))):
                            prod_map[kp] = line_p
                            prod_q[kp] = q

            # If consumption exists, it must still go to Consumption BoM even if supplier exists.
            if is_consumption_row:
                cons_val: Any = consumption_qty if consumption_qty is not None else (consumption_raw or "")
                line_c: Dict[str, Any] = {
                    "component": component,
                    "description": desc,
                    "composition": composition,
                    "consumption": cons_val,
                    "uom": uom,
                    "weight": weight,
                }
                line_c = {k: v for k, v in line_c.items() if v not in (None, "")}
                if line_c:
                    # Dedup by component + consumption + uom. Do not include description/weight,
                    # then pick the best-quality row (so duplicated 0.87m collapses to one).
                    kc = (
                        _norm_key(str(line_c.get("component") or ""))
                        + "|"
                        + _norm_key(str(line_c.get("consumption") or ""))
                        + "|"
                        + _norm_key(str(line_c.get("uom") or ""))
                    )
                    if kc.strip("|"):
                        q = _quality_score_line(line_c, ["description"]) + (8 if line_c.get("weight") else 0)
                        if (kc not in cons_map) or (q > int(cons_q.get(kc, -10**9))):
                            cons_map[kc] = line_c
                            cons_q[kc] = q

        score_out = _score_bom_table(headers, rm)
        return (list(cons_map.values()), list(prod_map.values()), int(score_out))

    merged_map: Dict[str, Dict[str, Any]] = {}
    merged_q: Dict[str, int] = {}
    merged_prod_map: Dict[str, Dict[str, Any]] = {}
    merged_prod_q: Dict[str, int] = {}
    sources: List[Dict[str, Any]] = []
    best_score = 0
    best_kind = None

    for page_i, neg_score, t in candidates:
        lines_i, prod_i, score_i = _extract_lines_from_table(t)
        if not lines_i and not prod_i:
            continue
        if score_i > best_score:
            best_score = score_i
            best_kind = t.get("table_kind")
        sources.append(
            {
                "page": None if page_i >= 10**9 else page_i,
                "table_kind": t.get("table_kind"),
                "score": score_i,
            }
        )
        for line in lines_i:
            k = (
                _norm_key(str(line.get("component") or ""))
                + "|"
                + _norm_key(str(line.get("consumption") or ""))
                + "|"
                + _norm_key(str(line.get("uom") or ""))
            )
            if not k.strip("|"):
                continue
            q = _quality_score_line(line, ["description"]) + (8 if line.get("weight") else 0)
            if (k not in merged_map) or (q > int(merged_q.get(k, -10**9))):
                merged_map[k] = line
                merged_q[k] = q

        for line in prod_i:
            kp = (
                _norm_key(str(line.get("component") or ""))
                + "|"
                + _norm_key(str(line.get("supplier") or ""))
                + "|"
                + _norm_key(str(line.get("production_unit") or ""))
                + "|"
                + _norm_key(str(line.get("composition") or ""))
            )
            if not kp.strip("|"):
                continue
            q = _quality_score_line(line, ["supplier", "production_unit", "composition"])
            if (kp not in merged_prod_map) or (q > int(merged_prod_q.get(kp, -10**9))):
                merged_prod_map[kp] = line
                merged_prod_q[kp] = q

    merged = list(merged_map.values())
    merged_prod = list(merged_prod_map.values())

    if not merged and not merged_prod:
        return None

    out: Dict[str, Any] = {"lines": merged, "source": {"table_kind": best_kind, "score": best_score}}
    if merged_prod:
        out["production_units_lines"] = merged_prod
    if len(sources) > 1:
        out["sources"] = sources
    return out
