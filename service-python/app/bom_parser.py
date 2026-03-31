import os
import re
from typing import Any, Dict, List, Optional, Tuple


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _cell_str(x: Any) -> str:
    return " ".join(str(x or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _fix_split_fiber_words(s: str) -> str:
    """Fix common OCR splits inside fiber words in HM supplementary BoM.

    Examples seen:
    - 'RECYCLED P OLYESTER' -> 'RECYCLED POLYESTER'
    - 'POL YESTER' -> 'POLYESTER'

    Keep it conservative and only join known targets.
    """
    t = _cell_str(s)
    if not t:
        return ""
    tu = t.upper()

    # Join single-letter prefix splits (e.g. 'P OLYESTER').
    tu = re.sub(r"\bP\s+OLYESTER\b", "POLYESTER", tu)
    tu = re.sub(r"\bP\s+OLYAMIDE\b", "POLYAMIDE", tu)

    # Handle missing leading letter cases: 'OLYESTER' -> 'POLYESTER' when preceded by a short fragment.
    # (Actual join is handled outside; here we only normalize if OCR already merged it.)
    tu = re.sub(r"\bOLYESTER\b", "OLYESTER", tu)

    # Join common mid-word splits.
    tu = re.sub(r"\bPOL\s+YESTER\b", "POLYESTER", tu)
    tu = re.sub(r"\bPOLY\s+ESTER\b", "POLYESTER", tu)
    tu = re.sub(r"\bPOLY\s+AMIDE\b", "POLYAMIDE", tu)
    tu = re.sub(r"\bVIS\s+COSE\b", "VISCOSE", tu)

    # Re-normalize whitespace after substitutions.
    return " ".join(tu.split()).strip()


def _is_composition_fragment_only(s: str) -> bool:
    """Return True for standalone continuation fragments that shouldn't be emitted as composition."""
    tu = _cell_str(s).upper()
    if not tu:
        return True
    if re.fullmatch(r"(YESTER|OLYESTER|ESTER|COSE|AMIDE|OLYAMIDE)", tu) is not None:
        return True
    # Too short to be meaningful composition.
    if len(tu) <= 4 and "%" not in tu:
        return True
    return False


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
        dbg_comp = os.getenv("BOM_DEBUG_COMPOSITION", "").strip().lower() in {"1", "true", "yes", "y", "on"}

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
                # If the table has an explicit 'Composition' header, do not override it with
                # heuristic column scoring (OCR can leak '%' and fiber tokens into Description).
                explicit_comp_idx: Optional[int] = None
                try:
                    for i_h, h in enumerate(headers):
                        if re.search(r"\bCOMPOSITION\b", str(h or ""), flags=re.IGNORECASE) is not None:
                            explicit_comp_idx = i_h
                            break
                    if explicit_comp_idx is not None:
                        cols["composition"] = int(explicit_comp_idx)
                except Exception:
                    explicit_comp_idx = None

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

                if i_comp is not None and explicit_comp_idx is None:
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
        skip_row_idx: set[int] = set()

        for ridx, r in enumerate(rm[start_idx:], start=start_idx):
            if ridx in skip_row_idx:
                continue
            if not isinstance(r, list):
                continue
            cells = [_cell_str(c) for c in r]
            if not any(cells):
                continue
            if _is_section_or_total_row(cells):
                continue

            dbg_blob = ""
            try:
                if dbg_comp:
                    dbg_blob = " ".join([c for c in cells if c]).upper()
            except Exception:
                dbg_blob = ""

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
            composition = _fix_split_fiber_words(get("composition"))
            consumption_raw = get("consumption")
            weight_raw = get("weight")
            supplier = get("supplier")
            production_unit = get("production_unit")
            qty_raw = get("qty")
            uom = _norm_uom(get("uom"))
            color = get("color")
            size = get("size")

            # HM Supplementary: composition can be split across consecutive rows.
            # Example observed:
            #   Row N:  'RECYCLED P'
            #   Row N+1:'OLYESTER'
            # In these cases, the inferred composition column can be empty; recover by
            # looking at row cells and the immediate next row.
            try:
                if not composition and ridx + 1 < len(rm):
                    blob_u = " ".join([c for c in cells if c]).upper()
                    if re.search(r"\bRECYCLED\b", blob_u) and re.search(r"\bP\b", blob_u):
                        # Find a fragment cell like 'RECYCLED P' or ending with ' P'.
                        frag = ""
                        for cc in cells:
                            cu = _cell_str(cc).upper()
                            if not cu:
                                continue
                            if re.search(r"\bRECYCLED\s+P\b", cu) is not None:
                                frag = cc
                                break
                            if re.search(r"\bRECYCLED\b", cu) is not None and cu.split() and cu.split()[-1] == "P":
                                frag = cc
                                break

                        if frag:
                            r_next = rm[ridx + 1]
                            if isinstance(r_next, list):
                                next_cells = [_cell_str(x) for x in r_next]
                                tok = ""
                                for nc in next_cells:
                                    nu = _cell_str(nc).upper()
                                    if nu in {"OLYESTER", "YESTER", "ESTER", "OLYAMIDE", "AMIDE", "COSE"}:
                                        tok = nc
                                        break
                                if tok:
                                    composition = _fix_split_fiber_words(f"{frag} {tok}")
                                    # Skip the next row if it looks like a continuation fragment row.
                                    # PPStructure sometimes keeps extra fragments in description, so be lenient:
                                    # if key columns are empty and the row mainly contains the continuation token,
                                    # we drop it.
                                    try:
                                        # Identify whether the next row has any meaningful fields.
                                        def _get_next(col: str) -> str:
                                            idx = cols.get(col)
                                            if idx is None:
                                                return ""
                                            if idx < 0 or idx >= len(next_cells):
                                                return ""
                                            return next_cells[idx]

                                        has_key = any(
                                            _get_next(k)
                                            for k in [
                                                "position",
                                                "placement",
                                                "type",
                                                "material",
                                                "supplier",
                                                "consumption",
                                                "weight",
                                                "qty",
                                            ]
                                        )

                                        nxt_blob = " ".join([c for c in next_cells if c]).upper().strip()
                                        has_cont = re.search(r"\b(OLYESTER|YESTER|ESTER|OLYAMIDE|AMIDE|COSE)\b", nxt_blob) is not None
                                        has_other_fiber = re.search(
                                            r"\b(POLYESTER|POLYAMIDE|COTTON|VISCOSE|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b",
                                            nxt_blob,
                                        ) is not None

                                        # Skip when it doesn't carry key columns and is essentially a continuation.
                                        if (not has_key) and has_cont and (not has_other_fiber or re.fullmatch(r"(OLYESTER|YESTER|ESTER|OLYAMIDE|AMIDE|COSE)(?:\s+\d.*)?", nxt_blob) is not None):
                                            skip_row_idx.add(ridx + 1)
                                    except Exception:
                                        nxt_blob = " ".join([c for c in next_cells if c]).upper()
                                        if re.fullmatch(r"(OLYESTER|YESTER|ESTER|OLYAMIDE|AMIDE|COSE)", nxt_blob.strip() or "") is not None:
                                            skip_row_idx.add(ridx + 1)
            except Exception:
                pass

            # HM Supplementary: composition text can be split across adjacent cells, e.g.
            # 'RECYCLED P' | 'OLYESTER' or 'POL' | 'YESTER'. Try to join with the
            # immediate next cell when it looks like a fiber continuation.
            try:
                ci = cols.get("composition")
                if ci is not None and ci >= 0 and ci < len(cells):
                    nxt = cells[ci + 1] if (ci + 1) < len(cells) else ""
                    if composition and nxt:
                        cu = composition.upper()
                        nu = _cell_str(nxt).upper()
                        # Continuation tokens that often appear in the next cell.
                        if re.fullmatch(r"(OLYESTER|YESTER|ESTER|OLYAMIDE|AMIDE|COSE)", nu or "") is not None:
                            # If the last token of composition is a short fragment, join.
                            last_tok = (cu.split()[-1] if cu.split() else "")
                            if 1 <= len(last_tok) <= 5:
                                composition = _fix_split_fiber_words(f"{composition} {nxt}")

                    # Some HM tables have shifted columns; the continuation token (e.g. 'YESTER')
                    # may not be in the immediate next cell. If composition ends with a short
                    # fragment like 'POL', try to find a continuation token anywhere in the row.
                    if composition:
                        cu2 = composition.upper().strip()
                        last_tok2 = (cu2.split()[-1] if cu2.split() else "")
                        if last_tok2 in {"POL", "POLY", "P"}:
                            cont = ""
                            for cc in cells:
                                ccu = _cell_str(cc).upper().strip()
                                if ccu in {"OLYESTER", "YESTER", "ESTER", "OLYAMIDE", "AMIDE", "COSE"}:
                                    cont = cc
                                    break
                            if cont:
                                composition = _fix_split_fiber_words(f"{composition} {cont}")
            except Exception:
                pass

            def _maybe_join_composition_continuation(comp: str) -> str:
                """Join composition fragments with continuation tokens found elsewhere in the row or headers.

                Examples:
                - 'RECYCLED P' + 'OLYESTER' (in another cell or merged header) -> 'RECYCLED POLYESTER'
                """
                try:
                    base = _fix_split_fiber_words(comp)
                    if not base:
                        return ""
                    bu = base.upper()
                    toks = bu.split()
                    last_tok = toks[-1] if toks else ""
                    if not (1 <= len(last_tok) <= 5):
                        return base

                    # Look for a continuation token in cells.
                    cont = ""
                    for cc in cells:
                        cu = _cell_str(cc).upper()
                        if cu in {"OLYESTER", "YESTER", "ESTER", "OLYAMIDE", "AMIDE", "COSE"}:
                            cont = cu
                            break

                    # Look for a continuation token in merged headers too (PPStructure sometimes leaks text there).
                    if not cont:
                        hblob = " ".join([str(h or "") for h in headers]).upper()
                        for tok in ["OLYESTER", "YESTER", "ESTER", "OLYAMIDE", "AMIDE", "COSE"]:
                            if re.search(r"\b" + re.escape(tok) + r"\b", hblob):
                                cont = tok
                                break

                    if not cont:
                        return base
                    return _fix_split_fiber_words(f"{base} {cont}")
                except Exception:
                    return _fix_split_fiber_words(comp)

            if dbg_comp:
                try:
                    if re.search(r"\b(RECYCLED|POLYESTER|OLYESTER|POLYAMIDE|COTTON|VISCOSE|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b", dbg_blob or "") is not None:
                        print(
                            "debug_bom_composition_row",
                            {
                                "page": tbl.get("page"),
                                "headers": headers,
                                "cols": cols,
                                "cells": cells,
                                "picked": {
                                    "material": material,
                                    "description": desc,
                                    "composition": composition,
                                    "consumption_raw": consumption_raw,
                                    "weight_raw": weight_raw,
                                },
                            },
                        )
                except Exception:
                    pass

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
                if not ((typ or placement or position) and (composition or consumption_raw or qty_raw)):
                    continue

            component = " ".join([x for x in [position, placement, typ] if x]).strip()
            if not component:
                component = material or desc

            def _looks_like_composition_text(s: str) -> bool:
                try:
                    tu = _fix_split_fiber_words(s).upper()
                    if not tu:
                        return False
                    if _is_composition_fragment_only(tu):
                        return False
                    # Guard: construction-like tokens shouldn't be treated as composition.
                    # Examples: '150x94', '20dx45s'
                    if re.search(r"\b\d{2,4}X\d{2,4}\b", tu) is not None or re.search(r"\b\d+DX\d+S\b", tu) is not None:
                        return False
                    if _extract_weight_value(tu):
                        return False
                    if _looks_like_consumption_value(tu):
                        return False
                    if "%" in tu:
                        return True
                    if re.search(r"\b(RECYCLED|POLYESTER|COTTON|VISCOSE|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b", tu) is not None:
                        return True
                    return False
                except Exception:
                    return False

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

            # If composition column is misaligned and contains a weight value, clear it.
            try:
                if composition and _extract_weight_value(composition):
                    if not weight:
                        weight = _extract_weight_value(composition)
                    composition = ""
            except Exception:
                pass

            # If composition is missing/cleared, search row cells for a composition-like text.
            if not composition:
                try:
                    best_comp = ""
                    for cc in cells:
                        if not cc:
                            continue
                        if not _looks_like_composition_text(cc):
                            continue
                        # Prefer percentage-based compositions.
                        cc_fixed = _fix_split_fiber_words(cc)
                        if _is_composition_fragment_only(cc_fixed):
                            continue
                        if "%" in _cell_str(cc_fixed).upper():
                            best_comp = cc_fixed
                            break
                        if not best_comp:
                            best_comp = cc_fixed
                    if best_comp:
                        composition = best_comp
                except Exception:
                    pass

            # HM Supplementary: composition can be split across Description/Composition columns and
            # sometimes continues in the next physical rows. Stitch those fragments conservatively.
            try:
                if is_hm:
                    # Merge percentage-leading fragment from description with the composition cell.
                    desc_u = _cell_str(desc).upper()
                    comp_u = _cell_str(composition).upper()
                    if ("%" in desc_u) and (desc_u not in comp_u):
                        # Avoid using description text that looks like a component name (e.g., SMOCKING THR).
                        if _looks_like_composition_text(desc) and (not _looks_like_supplier(desc)):
                            merged0 = _fix_split_fiber_words((desc + " " + (composition or "")).strip())
                            if len(merged0) > len(_cell_str(composition)) + 3:
                                composition = merged0

                    # Append composition fragments from immediate continuation rows where Position is blank.
                    if ridx + 1 < len(rm):
                        for j in range(ridx + 1, min(len(rm), ridx + 6)):
                            rr = rm[j]
                            if not isinstance(rr, list):
                                continue
                            row_cells = [_cell_str(x) for x in rr]
                            if not any(row_cells):
                                continue
                            # Stop when a new position starts.
                            pos_idx = cols.get("position")
                            row_pos = ""
                            if pos_idx is not None and 0 <= pos_idx < len(row_cells):
                                row_pos = _cell_str(row_cells[pos_idx])
                            if row_pos:
                                break

                            add_frags: List[str] = []
                            for cc in row_cells:
                                if not cc:
                                    continue
                                if _extract_weight_value(cc):
                                    continue
                                if _looks_like_consumption_value(cc):
                                    continue
                                if _looks_like_supplier(cc):
                                    continue
                                if not _looks_like_composition_text(cc):
                                    continue
                                cc_fixed = _fix_split_fiber_words(cc)
                                if _is_composition_fragment_only(cc_fixed):
                                    continue
                                add_frags.append(cc_fixed)

                            if add_frags:
                                composition = _fix_split_fiber_words((composition + " " + " ".join(add_frags)).strip())
                                skip_row_idx.add(j)
            except Exception:
                pass

            # Final normalization for HM Supplementary: composition can be scattered across
            # cells and/or split over multiple OCR lines. We (1) reconstruct percent clauses
            # from the whole row, then (2) clean/dedup, then (3) join continuation fragments.
            dbg_comp_pick: Dict[str, Any] = {}
            if is_hm:
                try:
                    def _clean_hm_composition(comp: str) -> str:
                        try:
                            c0 = _fix_split_fiber_words(comp)
                            if not c0:
                                return ""
                            m_pct = re.search(r"\b\d{1,3}\s*%", c0)
                            if m_pct is not None:
                                c0 = c0[m_pct.start() :]
                            c0 = re.sub(r"\s*\+?\d+\s*/\s*\d+\b", "", c0)
                            c0 = re.sub(r"\b\d{2,4}X\d{2,4}\b", "", c0, flags=re.IGNORECASE)
                            c0 = re.sub(r"\b\d+DX\d+S\b", "", c0, flags=re.IGNORECASE)
                            c0 = re.sub(r"\s*,\s*", ", ", c0)
                            c0 = _cell_str(c0)
                            try:
                                c0u = c0.upper()
                                m_lead = re.match(r"\s*(\d{1,3}\s*%\s*)", c0u)
                                if m_lead is not None:
                                    lead = m_lead.group(1).strip()
                                    if lead and c0u.count(lead) >= 2:
                                        last = c0u.rfind(lead)
                                        if last > 0:
                                            first_seg = c0[: last].strip()
                                            tail = c0[last:].strip()
                                            c0 = first_seg if len(first_seg) >= len(tail) else tail
                                c0 = re.sub(r"\b(\d{1,3}\s*%\b[^%]{0,80})\s+\1\b", r"\1", c0, flags=re.IGNORECASE)
                                c0 = _cell_str(c0)
                            except Exception:
                                c0 = _cell_str(c0)
                            return c0
                        except Exception:
                            return _cell_str(_fix_split_fiber_words(comp))

                    blob = " ".join([_cell_str(x) for x in cells if _cell_str(x)])
                    blob = _fix_split_fiber_words(blob)

                    def _clean_clause(cl: str) -> str:
                        c = _cell_str(cl)
                        if not c:
                            return ""
                        c = re.sub(r"\b\d+\*\d+\b", "", c)
                        c = re.sub(r"\b\d{2,4}X\d{2,4}\b", "", c, flags=re.IGNORECASE)
                        c = re.sub(r"\b\d+DX\d+S\b", "", c, flags=re.IGNORECASE)
                        c = re.sub(r"\s*\+?\d+\s*/\s*\d+\b", "", c)
                        c = re.sub(r"\bX\b", " ", c, flags=re.IGNORECASE)
                        # Remove stray '%' that OCR sometimes inserts before a fiber word,
                        # but do NOT strip valid percent tokens like '80% VISCOSE'.
                        c = re.sub(r"(?<!\d)%\s+(?=[A-Z])", " ", c)
                        c = re.sub(r"\s*,\s*", ", ", c)
                        c = _cell_str(c)
                        cu = c.upper()
                        if "IRCULOSE" in cu and "CIRCULOSE" not in cu:
                            c = re.sub(r"IRCULOSE", "CIRCULOSE", c, flags=re.IGNORECASE)
                        return _cell_str(c)

                    pct_positions = [m.start() for m in re.finditer(r"\b\d{1,3}\s*%", blob)]
                    clauses: List[str] = []
                    if pct_positions:
                        pct_positions2 = pct_positions + [len(blob)]
                        for a, b in zip(pct_positions2, pct_positions2[1:]):
                            part = _clean_clause(blob[a:b])
                            if not part:
                                continue
                            if len(part) < 5:
                                continue
                            if re.search(r"^\d{1,3}\s*%", part) is None:
                                continue
                            if part not in clauses:
                                clauses.append(part)

                    dbg_comp_pick["pct_clause_count"] = len(clauses)
                    dbg_comp_pick["pct_clauses"] = clauses[:10]

                    recon = ""
                    if clauses:
                        by_pct: Dict[str, str] = {}

                        def _score_clause(s: str) -> int:
                            try:
                                su = _cell_str(s).upper()
                                sc = len(su)
                                if re.search(r"\bPOLYAMIDE\b", su) is not None:
                                    sc += 40
                                if re.search(r"\b(VISCOSE|CIRCULOSE|REVISCO)\b", su) is not None:
                                    sc += 18
                                if re.search(r"\b(POLYESTER|COTTON|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b", su) is not None:
                                    sc += 8
                                if re.search(r"\b\d+\*\d+\b", su) is not None:
                                    sc -= 40
                                if re.search(r"\b\d{2,4}X\d{2,4}\b", su) is not None or re.search(r"\b\d+DX\d+S\b", su) is not None:
                                    sc -= 30
                                if "%NYLON" in su:
                                    sc -= 20
                                return int(sc)
                            except Exception:
                                return len(_cell_str(s))

                        order: List[str] = []
                        for cl in clauses:
                            m0 = re.match(r"\s*(\d{1,3})\s*%\b", cl)
                            if not m0:
                                continue
                            pct = m0.group(1)
                            if pct not in order:
                                order.append(pct)
                            prev = by_pct.get(pct, "")
                            if (not prev) or (_score_clause(cl) > _score_clause(prev)):
                                by_pct[pct] = cl

                        recon = _cell_str(", ".join([by_pct[p] for p in order if by_pct.get(p)]))

                    dbg_comp_pick["recon"] = recon

                    comp_candidates: List[str] = []
                    if recon:
                        comp_candidates.append(recon)
                    if composition:
                        comp_candidates.append(_cell_str(composition))
                    for cc in cells:
                        ccs = _cell_str(cc)
                        if not ccs:
                            continue
                        if _looks_like_composition_text(ccs):
                            comp_candidates.append(ccs)

                    best_final = ""
                    best_len = -1
                    for cand0 in comp_candidates:
                        cand = _clean_hm_composition(cand0)
                        if not cand:
                            continue
                        cu = cand.upper()
                        if re.search(r"\b\d{1,3}\s*%", cu) is None:
                            continue
                        if re.search(r"\b(POLYAMIDE|VISCOSE|CIRCULOSE|REVISCO|POLYESTER|COTTON|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b", cu) is None:
                            continue
                        if len(cand) > best_len:
                            best_final = cand
                            best_len = len(cand)

                    dbg_comp_pick["candidate_count"] = len(comp_candidates)
                    dbg_comp_pick["best_final"] = best_final

                    composition = _cell_str(best_final) if best_final else _cell_str(_clean_hm_composition(composition))

                    try:
                        cu = _cell_str(composition).upper()
                        if re.search(r"\bWITH\s+C_?\b", cu) is not None:
                            if re.search(r"C\s*IR?CULOSE|IRCULOSE", blob.upper()) is not None:
                                composition = re.sub(r"\bWITH\s+C_?\b", "WITH CIRCULOSE", composition, flags=re.IGNORECASE)
                    except Exception:
                        pass

                    if composition:
                        composition = _maybe_join_composition_continuation(composition)
                except Exception:
                    dbg_comp_pick["error"] = "hm_comp_exception"
                    pass

            if dbg_comp:
                try:
                    blob2 = " ".join([_cell_str(x) for x in cells if _cell_str(x)])
                    b2u = blob2.upper()
                    if (
                        ("%" in b2u)
                        or (re.search(r"\b(POLYAMIDE|NYLON|ZCX\d+)\b", b2u) is not None)
                        or (re.search(r"\b\d+\*\d+\b", b2u) is not None)
                    ):
                        print(
                            "debug_bom_final_composition_pick",
                            {
                                "page": tbl.get("page"),
                                "component": component,
                                "composition": composition,
                                "blob": blob2[:600],
                                "pick": dbg_comp_pick,
                            },
                        )
                except Exception:
                    pass

            # Normalize common HM OCR artifacts for polyester when the word is split across lines/cells.
            # Examples:
            # - '100% S0 POL' + 'YESTER' -> '100% S0 POLYESTER'
            # - 'YESTER 100% S0 POL' -> '100% S0 POLYESTER'
            cu_norm = _cell_str(composition).upper()
            if "POLYESTER" not in cu_norm:
                if re.search(r"\bS0\s+POL\b", cu_norm) is not None and re.search(r"\bYESTER\b", cu_norm) is not None:
                    cu_norm = re.sub(r"\bYESTER\b", "", cu_norm)
                    cu_norm = re.sub(r"\bS0\s+POL\b", "S0 POLYESTER", cu_norm)
                    composition = " ".join(cu_norm.split()).strip()
                elif re.search(r"\bS0\s+POL\b", cu_norm) is not None:
                    # If YESTER was lost, still treat it as POLYESTER (matches the source table pattern).
                    cu_norm = re.sub(r"\bS0\s+POL\b", "S0 POLYESTER", cu_norm)
                    composition = " ".join(cu_norm.split()).strip()

            qty_num = _to_number(qty_raw)

            # Row-level classification
            supplier_blob = " ".join([x for x in [supplier, production_unit, qty_raw] if x]).strip()
            is_supplier_row = _looks_like_supplier(supplier_blob) or _looks_like_supplier(material) or _looks_like_supplier(desc)
            # Only consider numeric-only cells as consumption when we can attach a valid unit.
            has_consumption_unit = bool(uom or consumption_uom)
            is_consumption_row = _looks_like_consumption_value(consumption_raw) or (consumption_qty is not None and has_consumption_unit)

            # HM Supplementary: stitch extra description fragments for the consumption row.
            # This is used for the main fabric line where description spans multiple physical rows/cells.
            try:
                if is_hm and is_consumption_row:
                    d0 = _cell_str(desc)
                    # If description is actually composition (common OCR column shift),
                    # keep only the prefix before the first percent token.
                    try:
                        d0u = _cell_str(d0).upper()
                        if d0 and ("%" in d0u) and _looks_like_composition_text(d0):
                            m_pct = re.search(r"\b\d{1,3}\s*%", d0u)
                            if m_pct is not None and m_pct.start() > 0:
                                d0 = _cell_str(d0[: m_pct.start()])
                            else:
                                d0 = ""
                    except Exception:
                        pass
                    if d0:
                        m_zcx = re.search(r"\bZCX\d+\b", d0.upper())
                        if m_zcx is not None:
                            d0 = d0[m_zcx.start() :].strip()
                    add_desc: List[str] = []

                    for cc in cells:
                        ccs = _cell_str(cc)
                        if not ccs:
                            continue
                        if _looks_like_supplier(ccs) or _looks_like_consumption_value(ccs):
                            continue
                        cu = ccs.upper()
                        if re.search(r"\b\d{2,4}X\d{2,4}\b", cu) is not None or re.search(r"\b\d+DX\d+S\b", cu) is not None:
                            add_desc.append(ccs)
                            continue
                        # Allow weight-like tokens into description for HM supplementary main fabric.
                        if re.search(r"\b\d+(?:[\.,]\d+)?\s*G\s*/\s*(?:M2|M|SM)\b", cu.replace(" ", "")) is not None or re.search(r"\bGSM\b", cu) is not None:
                            add_desc.append(ccs)
                            continue
                        if 'CW' in cu or '"' in ccs:
                            add_desc.append(ccs)
                            continue

                    if ridx + 1 < len(rm):
                        for j in range(ridx + 1, min(len(rm), ridx + 14)):
                            rr = rm[j]
                            if not isinstance(rr, list):
                                continue
                            row_cells = [_cell_str(x) for x in rr]
                            if not any(row_cells):
                                continue

                            pos_idx = cols.get("position")
                            row_pos = ""
                            if pos_idx is not None and 0 <= pos_idx < len(row_cells):
                                row_pos = _cell_str(row_cells[pos_idx])
                            if row_pos:
                                break

                            key_hit = False
                            for k in ["placement", "type", "material", "supplier", "consumption", "weight", "qty"]:
                                ki = cols.get(k)
                                if ki is None or ki < 0 or ki >= len(row_cells):
                                    continue
                                v = _cell_str(row_cells[ki])
                                if not v:
                                    continue
                                # In HM supplementary, the inferred qty column can hold composition fragments
                                # like 'RECYCLED P'. Treat that as a stitchable continuation (not a new key row).
                                if k == "qty":
                                    v_fixed = _fix_split_fiber_words(v)
                                    vu = _cell_str(v_fixed).upper()
                                    if _is_composition_fragment_only(v_fixed) or _looks_like_composition_text(v_fixed) or re.search(r"\bRECYCLED\b", vu) is not None:
                                        continue
                                key_hit = True
                                break
                            if key_hit:
                                continue

                            d_frag = ""
                            di = cols.get("description")
                            if di is not None and 0 <= di < len(row_cells):
                                d_frag = _cell_str(row_cells[di])
                            if d_frag and (not _looks_like_composition_text(d_frag)) and d_frag not in add_desc and d_frag not in d0:
                                add_desc.append(d_frag)

                            # Continuation rows sometimes carry RECYCLED POLYESTER in a non-composition column.
                            # Scan all cells and pick the best recycled fiber fragment.
                            best_recycled = ""
                            for rc in row_cells:
                                if not rc:
                                    continue
                                if _looks_like_supplier(rc) or _looks_like_consumption_value(rc):
                                    continue
                                rc_fixed = _fix_split_fiber_words(rc)
                                if _is_composition_fragment_only(rc_fixed):
                                    continue
                                ru = _cell_str(rc_fixed).upper()
                                if "RECYCLED" in ru and ("POLYESTER" in ru or re.search(r"\bRECYCLED\s+P\b", ru) is not None):
                                    # Prefer the longer recycled fragment.
                                    if len(rc_fixed) > len(best_recycled):
                                        best_recycled = rc_fixed
                            # If we only captured a short recycled fragment (e.g. 'RECYCLED P'),
                            # try to complete it by peeking at the next physical row for continuation tokens.
                            try:
                                bu = _cell_str(best_recycled).upper()
                                if re.search(r"\bRECYCLED\s+P\b", bu) is not None and (j + 1) < len(rm):
                                    rr2 = rm[j + 1]
                                    if isinstance(rr2, list):
                                        row2 = [_cell_str(x) for x in rr2]
                                        tok2 = ""
                                        for c2 in row2:
                                            c2u = _cell_str(c2).upper()
                                            if c2u in {"OLYESTER", "YESTER", "ESTER", "OLYAMIDE", "AMIDE", "COSE"}:
                                                tok2 = c2
                                                break
                                        if tok2:
                                            best_recycled = _fix_split_fiber_words(f"{best_recycled} {tok2}")
                                            skip_row_idx.add(j + 1)
                            except Exception:
                                pass
                            if best_recycled:
                                if best_recycled not in add_desc and best_recycled not in d0:
                                    add_desc.append(best_recycled)

                            if add_desc:
                                skip_row_idx.add(j)

                    if add_desc:
                        d0 = " ".join([x for x in [d0] + add_desc if x]).strip()
                        d0 = _cell_str(d0)
                    if d0:
                        desc = d0

                    if dbg_comp:
                        try:
                            if re.search(r"\bZCX\d+\b", _cell_str(desc).upper()) is not None:
                                print(
                                    "debug_bom_final_consumption_row",
                                    {
                                        "page": tbl.get("page"),
                                        "component": component,
                                        "description": desc,
                                        "composition": composition,
                                        "consumption_raw": consumption_raw,
                                        "uom": uom,
                                        "weight": weight,
                                    },
                                )
                        except Exception:
                            pass
            except Exception:
                pass

            # HM Supplementary: sometimes the extracted "Description" column is empty while
            # the actual descriptor lives in the Material/Material Appearance column (e.g. Elastic/Tape/Buckle).
            # Only apply this fallback for consumption rows to avoid filling non-consumption rows.
            try:
                if is_hm and is_consumption_row and (not _cell_str(desc)) and _cell_str(material):
                    if (
                        (not _looks_like_supplier(material))
                        and (not _extract_weight_value(material))
                        and (not _looks_like_consumption_value(material))
                        and (not _looks_like_composition_text(material))
                        and (not _is_composition_fragment_only(material))
                    ):
                        desc = material
            except Exception:
                pass

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
                        try:
                            compu = _cell_str(line_c.get("composition") or "").upper()
                            if re.search(r"\b(POLYESTER|POLYAMIDE|COTTON|VISCOSE|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b", compu) is not None:
                                q += 6
                            # Penalize truncated fibers like trailing ' POL'
                            if re.search(r"\bPOL\b$", compu) is not None:
                                q -= 6
                            if compu.startswith("YESTER "):
                                q -= 6
                        except Exception:
                            pass
                        if (kc not in cons_map) or (q > int(cons_q.get(kc, -10**9))):
                            cons_map[kc] = line_c
                            cons_q[kc] = q

            # HM Supplementary: do not emit composition-only rows. Output should only contain rows
            # that have Consumption per Unit.
            if False and (not is_supplier_row) and (not is_consumption_row) and component and composition:
                pass

        score_out = _score_bom_table(headers, rm)
        return (list(cons_map.values()), list(prod_map.values()), int(score_out))

    merged_map: Dict[str, Dict[str, Any]] = {}
    merged_q: Dict[str, int] = {}
    merged_prod_map: Dict[str, Dict[str, Any]] = {}
    merged_prod_q: Dict[str, int] = {}
    sources: List[Dict[str, Any]] = []
    best_score = 0
    best_kind = None
    best_comp_by_component: Dict[str, Tuple[int, str]] = {}

    def _composition_quality_score(s: str) -> int:
        try:
            su = _cell_str(s).upper()
            if not su:
                return -10**6
            sc = len(su)
            if re.search(r"\b\d{1,3}\s*%", su) is not None:
                sc += 60
            if re.search(r"\b(POLYAMIDE|VISCOSE|CIRCULOSE|REVISCO|POLYESTER|COTTON|NYLON|ELASTANE|WOOL|LINEN|ACRYLIC|RAYON|SILK)\b", su) is not None:
                sc += 40
            if re.search(r"\b20\*\d+\b", su) is not None or re.search(r"\b\d+\*\d+\b", su) is not None:
                sc -= 80
            if "%NYLON" in su:
                sc -= 80
            if re.search(r"\bC\s+X\b", su) is not None:
                sc -= 35
            if re.search(r"\b\d{2,4}X\d{2,4}\b", su) is not None or re.search(r"\b\d+DX\d+S\b", su) is not None:
                sc -= 50
            if _extract_weight_value(su):
                sc -= 40
            if _looks_like_consumption_value(su):
                sc -= 40
            if _looks_like_supplier(su):
                sc -= 80
            return int(sc)
        except Exception:
            return -10**6

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
            try:
                comp0 = _cell_str(line.get("composition") or "")
                if comp0:
                    ck0 = _norm_key(str(line.get("component") or ""))
                    if ck0:
                        sc0 = _composition_quality_score(comp0)
                        cur0 = best_comp_by_component.get(ck0)
                        if (cur0 is None) or (sc0 > int(cur0[0])):
                            best_comp_by_component[ck0] = (int(sc0), comp0)
            except Exception:
                pass
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
            q_comp = _composition_quality_score(str(line.get("composition") or ""))
            q_total = int(q) + int(q_comp // 20)
            if k in merged_map:
                try:
                    cur = merged_map.get(k) or {}
                    cur_comp = str(cur.get("composition") or "")
                    if q_comp > _composition_quality_score(cur_comp):
                        cur["composition"] = line.get("composition")
                except Exception:
                    pass
            if (k not in merged_map) or (q_total > int(merged_q.get(k, -10**9))):
                merged_map[k] = line
                merged_q[k] = q_total

        for line in prod_i:
            try:
                comp0 = _cell_str(line.get("composition") or "")
                if comp0:
                    ck0 = _norm_key(str(line.get("component") or ""))
                    if ck0:
                        sc0 = _composition_quality_score(comp0)
                        cur0 = best_comp_by_component.get(ck0)
                        if (cur0 is None) or (sc0 > int(cur0[0])):
                            best_comp_by_component[ck0] = (int(sc0), comp0)
            except Exception:
                pass
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

    # If we have a clean composition candidate for the same component from a non-consumption row
    # (common in HM Supplementary where consumption and composition appear on different tables/pages),
    # override the merged consumption row composition.
    try:
        dbg_comp = os.getenv("BOM_DEBUG_COMPOSITION", "").strip().lower() in {"1", "true", "yes", "y", "on"}
        for ln in merged:
            ck = _norm_key(str(ln.get("component") or ""))
            if not ck:
                continue
            best = best_comp_by_component.get(ck)
            if not best:
                continue
            best_sc, best_comp = best
            cur_comp = _cell_str(ln.get("composition") or "")
            cur_sc = _composition_quality_score(cur_comp)
            if best_sc > cur_sc:
                ln["composition"] = best_comp
                if dbg_comp:
                    try:
                        comp_name = _cell_str(ln.get("component") or "")
                        if re.search(r"\bSHELL\b", comp_name.upper()) is not None:
                            print(
                                "debug_bom_merge_comp_override",
                                {
                                    "component": comp_name,
                                    "from": cur_comp,
                                    "to": best_comp,
                                    "from_sc": int(cur_sc),
                                    "to_sc": int(best_sc),
                                },
                            )
                    except Exception:
                        pass
    except Exception:
        pass

    if not merged and not merged_prod:
        return None

    try:
        orphan_frags: List[str] = []

        def _looks_like_hm_orphan_fragment(s: str) -> bool:
            try:
                su = _cell_str(s).upper()
                if not su:
                    return False
                if _looks_like_supplier(su):
                    return False
                if _looks_like_consumption_value(su):
                    return False
                if _extract_weight_value(su):
                    return False
                if re.search(r"\b(RECYCLED|CIRCULOSE|REVISCO)\b", su) is not None:
                    return True
                if re.search(r"\b\d+DX\d+S\b", su) is not None:
                    return True
                if re.search(r"\b\d{2,4}X\d{2,4}\b", su) is not None:
                    return True
                if "CW" in su or "\"" in su:
                    return True
                if re.search(r"\b\d+(?:[\.,]\d+)?\s*G\s*/\s*(?:M2|M|SM)\b", su.replace(" ", "")) is not None:
                    return True
                if re.search(r"\bGSM\b", su) is not None:
                    return True
                if re.search(r"\bRECY\b", su) is not None or "RECY" in su:
                    return True
                return False
            except Exception:
                return False

        for _page_i, _neg_score, tt in candidates:
            try:
                headers = tt.get("headers") or []
                rm = tt.get("rows_matrix") or []
                is_hm_tbl = isinstance(headers, list) and any(
                    re.search(r"\bPOSITION\b", str(h or ""), flags=re.IGNORECASE) for h in headers
                )
                if not is_hm_tbl or not isinstance(rm, list) or not rm:
                    continue
                cols0 = _infer_bom_columns(headers)
                if not isinstance(cols0, dict):
                    continue

                for rr in rm[1:]:
                    if not isinstance(rr, list):
                        continue
                    row_cells = [_cell_str(x) for x in rr]
                    if not any(row_cells):
                        continue

                    pos = ""
                    for kpos in ["position", "placement", "type"]:
                        ki = cols0.get(kpos)
                        if ki is not None and 0 <= ki < len(row_cells):
                            pos = pos + _cell_str(row_cells[ki])
                    if _cell_str(pos):
                        continue

                    has_consumption = False
                    ci = cols0.get("consumption")
                    if ci is not None and 0 <= ci < len(row_cells):
                        if _looks_like_consumption_value(_cell_str(row_cells[ci])):
                            has_consumption = True
                    if has_consumption:
                        continue

                    sup_blob = ""
                    si = cols0.get("supplier")
                    if si is not None and 0 <= si < len(row_cells):
                        sup_blob = _cell_str(row_cells[si])
                    if _looks_like_supplier(sup_blob):
                        continue

                    desc_i = cols0.get("description")
                    comp_i = cols0.get("composition")
                    seed: List[str] = []
                    if desc_i is not None and 0 <= desc_i < len(row_cells):
                        d = _cell_str(row_cells[desc_i])
                        if d:
                            seed.append(d)
                    if comp_i is not None and 0 <= comp_i < len(row_cells):
                        c = _cell_str(row_cells[comp_i])
                        if c:
                            seed.append(c)
                    for cc in row_cells:
                        if not cc:
                            continue
                        if _looks_like_hm_orphan_fragment(cc):
                            seed.append(cc)

                    for s in seed:
                        sf = _fix_split_fiber_words(s)
                        if not sf:
                            continue
                        if not _looks_like_hm_orphan_fragment(sf):
                            continue
                        if sf not in orphan_frags:
                            orphan_frags.append(sf)
            except Exception:
                continue

        if orphan_frags:
            for ln in merged:
                try:
                    d0 = _cell_str(ln.get("description") or "")
                    if re.search(r"\bZCX\d+\b", d0.upper()) is None:
                        continue
                    add2: List[str] = []
                    for f in orphan_frags:
                        if not f:
                            continue
                        fu = _cell_str(f).upper()
                        if ("RECYCLED" in fu) or ("RECY" in fu) or ("CW" in fu) or (re.search(r"\b\d+DX\d+S\b", fu) is not None):
                            if f not in d0 and f not in add2:
                                add2.append(f)
                    if add2:
                        ln["description"] = _cell_str(" ".join([x for x in [d0] + add2 if x]).strip())
                except Exception:
                    continue
    except Exception:
        pass

    out: Dict[str, Any] = {"lines": merged, "source": {"table_kind": best_kind, "score": best_score}}
    if merged_prod:
        out["production_units_lines"] = merged_prod
    if len(sources) > 1:
        out["sources"] = sources
    return out
