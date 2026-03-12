import json
import logging
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional

from .bom_parser import build_bom_payload


def try_pdf_digital_fastpath(
    *,
    request_id: str,
    filename: str,
    file_bytes: bytes,
    so_dbg_tables: bool,
    warnings: List[Dict[str, Any]],
    errors: List[Dict[str, Any]],
    logger: logging.Logger,
    log_json: Optional[Callable[..., None]],
    pdf_text_pages: Callable[[bytes], Optional[List[str]]],
    pdf_tables_pages_tabula: Callable[[bytes, int], Optional[List[List[Dict[str, Any]]]]],
    postprocess_ocr_text: Callable[[str], str],
    extract_fields_smart: Callable[[str, List[Dict[str, Any]]], Dict[str, Any]],
    fields_to_pairs: Callable[[Dict[str, Any]], List[Dict[str, Any]]],
    build_ai_kv_table_from_fields: Callable[[Dict[str, Any]], Dict[str, Any]],
    table_add_rows_matrix: Callable[[Dict[str, Any]], Dict[str, Any]],
    filter_tables_for_sales_order: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
    normalize_size_grid_columns: Callable[[Dict[str, Any]], None],
    dedup_top_level_ai_kv_tables: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
    build_sales_order_payload: Callable[[List[Dict[str, Any]]], Dict[str, Any]],
    parse_total_order_from_text: Callable[[str], Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    def _is_hm_supplementary(text: str, filename0: str) -> bool:
        try:
            t = str(text or "")
            fn = str(filename0 or "")
            if re.search(r"Supplementary\s+Product\s+Information", fn, flags=re.IGNORECASE) is not None:
                return True
            if not t.strip():
                return False
            # Main marker
            if re.search(r"\bSupplementary\s+Product\s+Information\b", t, flags=re.IGNORECASE) is None:
                return False
            # 'HM' token is helpful but not always present in embedded text extraction
            return True
        except Exception:
            return False

    def _merge_header_if_missing(dst_header: Dict[str, Any], src_header: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(dst_header, dict):
                dst_header = {}
            if not isinstance(src_header, dict):
                return dst_header
            for k, v in src_header.items():
                if v in (None, ""):
                    continue
                if str(dst_header.get(k) or "").strip():
                    continue
                dst_header[k] = v
            return dst_header
        except Exception:
            return dst_header

    def _hm_is_bad_value(key: str, value: str) -> bool:
        """Detect obviously wrong values for HM Supplementary header fields."""
        try:
            k = str(key or "").strip().lower()
            v = re.sub(r"\s+", " ", str(value or "").strip())
            if not v:
                return True
            v_l = v.lower()

            if k == "date":
                # Reject label-like contamination
                if "season" in v_l or "supplier" in v_l or "order" in v_l:
                    return True
                # Accept common formats: 2025-07-11, 11/07/2025, 31 Oct 2025
                if re.search(r"\b\d{4}-\d{2}-\d{2}\b", v):
                    return False
                if re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", v):
                    return False
                if re.search(r"\b\d{1,2}\s+[A-Z]{3,9}\s+\d{4}\b", v, flags=re.IGNORECASE):
                    return False
                return True

            if k == "season":
                # Typically 3-2026, W 2025, etc.
                if re.search(r"\b\d\s*[-/]\s*\d{4}\b", v):
                    return False
                if re.search(r"\b[SW]\s*\d{4}\b", v, flags=re.IGNORECASE):
                    return False
                if "season" in v_l and re.search(r"\d{4}", v_l):
                    return False
                return True

            if k == "supplier":
                if v_l in {"send to", "sendto", "ship to", "shipto"}:
                    return True
                # Company-like should contain letters and usually CO/LTD/INC or multiple words
                if len(v) <= 3:
                    return True
                if re.search(r"\d{4,}", v) and re.search(r"\b(ltd|co|company|inc|trading)\b", v_l) is None and v.count(" ") <= 1:
                    return True
                return False

            if k == "article":
                # HM product no is numeric-like; reject material types like 'Elastic'
                if re.search(r"\d", v) is None:
                    return True
                if _hm_norm_label(v) in {"elastic", "buckle", "shell", "trim"}:
                    return True
                return False

            if k == "supplierref":
                # Supplier code is usually numeric
                if re.fullmatch(r"\d{2,10}", re.sub(r"\s+", "", v)):
                    return False
                return True

            # Default: keep
            return False
        except Exception:
            return False

    def _hm_merge_header_prefer_patch(dst_header: Dict[str, Any], patch_header: Dict[str, Any]) -> Dict[str, Any]:
        """For HM Supplementary, override bad existing values using patch values."""
        try:
            if not isinstance(dst_header, dict):
                dst_header = {}
            if not isinstance(patch_header, dict):
                return dst_header
            for k, v in patch_header.items():
                vv = str(v or "").strip()
                if not vv:
                    continue
                cur = str(dst_header.get(k) or "").strip()
                if (not cur) or _hm_is_bad_value(k, cur):
                    dst_header[k] = vv
            return dst_header
        except Exception:
            return dst_header

    def _hm_norm_label(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(s or "").strip().lower())

    def _hm_header_from_kv_tables(tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract key header fields for HM Supplementary from key/value tables.

        Handles split labels like: ['Date', 'of Order', '31 Oct 2025'] or ['Supplier','Code','1277'].
        Returns canon keys matching existing payload header keys.
        """
        out: Dict[str, Any] = {}
        if not isinstance(tables, list):
            return out

        key_map = {
            "orderno": "ordernr",
            "ordernr": "ordernr",
            "dateoforder": "date",
            "suppliercode": "supplierref",
            "suppliername": "supplier",
            "productno": "article",
            "productname": "description",
            "producttype": "description",
            "season": "season",
            "customscustomergroup": "buyer",
            "typeofconstruction": "description",
        }

        def push(k_raw: str, v_raw: str) -> None:
            nk = _hm_norm_label(k_raw)
            if not nk:
                return
            ck = key_map.get(nk)
            if not ck:
                return
            vv = str(v_raw or "").strip()
            if not vv:
                return
            if str(out.get(ck) or "").strip():
                return
            out[ck] = vv

        def looks_like_kv_table(tt: Dict[str, Any]) -> bool:
            try:
                hs = tt.get("headers") or []
                if not isinstance(hs, list) or len(hs) < 2:
                    return False
                h0 = _hm_norm_label(hs[0])
                h1 = _hm_norm_label(hs[1])
                if (h0, h1) == ("key", "value"):
                    return True
                # some tabula outputs use blank headers; allow by content
                return False
            except Exception:
                return False

        def push_from_row(cells: List[str]) -> None:
            if len(cells) < 2:
                return
            # Common split label: first two cells are label fragments, third is value
            if len(cells) >= 3:
                k12 = (cells[0] + " " + cells[1]).strip()
                if _hm_norm_label(k12) in key_map:
                    push(k12, cells[2])
                    return
            # Typical KV layout
            if _hm_norm_label(cells[0]) in key_map:
                push(cells[0], cells[1])
                return
            # Label might appear later in the row (multi-column kv)
            for i in range(0, len(cells) - 1):
                k = cells[i]
                v = cells[i + 1]
                if _hm_norm_label(k) in key_map:
                    push(k, v)

        for t in tables:
            if not isinstance(t, dict):
                continue
            rm = t.get("rows_matrix")
            if not isinstance(rm, list):
                continue
            # Prefer key/value tables, but also scan other tables because HM blocks can be exported with generic COL_x headers.
            prefer = looks_like_kv_table(t)
            for r in rm:
                if not isinstance(r, list) or len(r) < 2:
                    continue
                cells = [str(x or "").strip() for x in r]
                cells = [c for c in cells if c]
                if len(cells) < 2:
                    continue

                # Only scan non-kv tables if row seems to contain one of our keys (cheap prefilter)
                if not prefer:
                    row_blob = " ".join(cells[:6])
                    if re.search(r"\b(Order\s*No|Order\s*Nr|Date\s+of\s+Order|Supplier\s+Name|Supplier\s+Code|Product\s+No|Product\s+Name|Season)\b", row_blob, flags=re.IGNORECASE) is None:
                        continue

                push_from_row(cells)

        return out

    def _find_hm_materials_trims_table(tables: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Pick the BoM 'Materials and Trims' table from combined_tables.

        Tabula often stores the section title in headers[0] and/or the first rows_matrix row.
        """
        if not isinstance(tables, list):
            return None
        best = None
        best_score = 0
        for t in tables:
            if not isinstance(t, dict):
                continue
            headers = t.get("headers") or []
            rm = t.get("rows_matrix") or []
            if not isinstance(headers, list) or not isinstance(rm, list) or len(rm) < 2:
                continue
            head_blob = " ".join([str(h or "") for h in headers[:8]]).strip()
            row0 = rm[0] if (rm and isinstance(rm[0], list)) else []
            row_blob = " ".join([str(x or "") for x in (row0 or [])]).strip()
            blob = (head_blob + " " + row_blob).strip()
            if not blob:
                continue
            score = 0
            if re.search(r"\bBill\s+of\s+Material\b", blob, flags=re.IGNORECASE):
                score += 6
            if re.search(r"\bMaterials\b", blob, flags=re.IGNORECASE):
                score += 4
            if re.search(r"\bTrims\b", blob, flags=re.IGNORECASE):
                score += 4
            # bonus if typical columns exist
            if re.search(r"\b(Composition|Consumption|Weight|Supplier)\b", blob, flags=re.IGNORECASE):
                score += 2
            if score > best_score:
                best_score = score
                best = t
        return best if best_score >= 10 else None

    pages_text = pdf_text_pages(file_bytes)
    joined = "\n\n".join([t for t in (pages_text or []) if (t or "").strip()]).strip()
    page_count = len(pages_text or [])
    pdf_tables_pages = pdf_tables_pages_tabula(file_bytes, page_count) if page_count else None
    has_pdf_tables = any((isinstance(p, list) and len(p) > 0) for p in (pdf_tables_pages or []))

    is_hm_supp = _is_hm_supplementary(joined, filename)

    if not ((joined and len(joined) >= 50) or has_pdf_tables):
        return None

    try:
        if log_json is not None:
            log_json(
                logger,
                logging.INFO,
                "ocr_pdf_fastpath",
                {
                    "request_id": request_id,
                    "doc_filename": filename,
                    "page_count": page_count,
                    "has_pdf_tables": has_pdf_tables,
                    "embedded_text_len": len(joined or ""),
                },
            )
    except Exception:
        pass

    t0 = time.perf_counter()

    all_pages: List[Dict[str, Any]] = []
    combined_texts: List[str] = []

    for i, t in enumerate(pages_text or [], start=1):
        pp_text = postprocess_ocr_text(t or "")
        page_tables: List[Dict[str, Any]] = []
        if isinstance(pdf_tables_pages, list) and i - 1 < len(pdf_tables_pages):
            for tt in (pdf_tables_pages[i - 1] or []):
                if isinstance(tt, dict) and tt.get("headers") and tt.get("rows"):
                    page_tables.append(tt)

        pp_fields = extract_fields_smart(pp_text, page_tables)
        page_res: Dict[str, Any] = {
            "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
            "page": i,
            "text": pp_text,
            "tables": page_tables,
            "fields": pp_fields,
            "field_pairs": fields_to_pairs(pp_fields),
            "preprocess": {"enabled": False, "target": "pdf_text"},
        }
        try:
            page_res["tables"] = [build_ai_kv_table_from_fields(pp_fields)] + (page_res.get("tables") or [])
        except Exception:
            try:
                warnings.append({"page": i, "code": "AI_TABLE_BUILD_FAILED", "message": "Failed to build AI key/value table"})
            except Exception:
                pass
            page_res["tables"] = page_res.get("tables") or []

        all_pages.append(page_res)
        if pp_text.strip():
            combined_texts.append(pp_text.strip())

    combined_fields: List[Dict[str, Any]] = []
    for p in all_pages:
        if p.get("fields"):
            combined_fields.append({"page": p.get("page"), **(p.get("fields") or {})})

    combined_field_pairs: List[Dict[str, Any]] = []
    for p in all_pages:
        for pair in (p.get("field_pairs") or []):
            if isinstance(pair, dict) and pair.get("key") is not None and pair.get("value") is not None:
                combined_field_pairs.append({"page": p.get("page"), **pair})

    combined_tables: List[Dict[str, Any]] = []
    for p in all_pages:
        for t in (p.get("tables") or []):
            tt = {"page": p.get("page"), **t}
            try:
                tt = table_add_rows_matrix(tt)
            except Exception:
                pass
            combined_tables.append(tt)

    if so_dbg_tables:
        try:
            debug_tables: List[Dict[str, Any]] = []
            for t in combined_tables[:12]:
                if not isinstance(t, dict):
                    continue
                headers = t.get("headers") or []
                debug_tables.append(
                    {
                        "page": t.get("page"),
                        "table_kind": str(t.get("table_kind") or ""),
                        "headers": [str(h or "") for h in headers[:12]],
                        "row_count": len(t.get("rows") or []) if isinstance(t.get("rows"), list) else None,
                    }
                )
            _payload = {
                "event": "so_pdf_fastpath_tables_pre_filter",
                "request_id": request_id,
                "table_count": len(combined_tables),
                "tables": debug_tables,
            }
            logger.info("so_pdf_fastpath_tables_pre_filter %s", json.dumps(_payload, ensure_ascii=False))
        except Exception:
            pass

    # HM Supplementary: extract BoM from raw tables BEFORE filtering, otherwise it may be dropped.
    hm_bom_payload: Optional[Dict[str, Any]] = None
    hm_header_patch: Dict[str, Any] = {}
    if is_hm_supp:
        try:
            hm_header_patch = _hm_header_from_kv_tables(combined_tables)
        except Exception:
            hm_header_patch = {}

        try:
            mt_tbl = _find_hm_materials_trims_table(combined_tables)
            if mt_tbl is not None:
                hm_bom_payload = build_bom_payload(tables=[mt_tbl])
        except Exception:
            hm_bom_payload = None

        # Fallback: section title may be missing/flattened in extracted tables (e.g., Camelot stream).
        # Try scoring across all tables before bypassing fast-path.
        if hm_bom_payload is None:
            try:
                hm_bom_payload = build_bom_payload(tables=combined_tables)
            except Exception:
                hm_bom_payload = None

        # If we can't find the core Materials & Trims BoM table, bypass fast-path so OCR pipeline can try.
        if hm_bom_payload is None:
            return None

        if so_dbg_tables:
            try:
                logger.info(
                    "hm_supp_debug %s",
                    json.dumps(
                        {
                            "event": "hm_supp_debug",
                            "request_id": request_id,
                            "hm_header_patch": hm_header_patch,
                            "has_bom_payload": hm_bom_payload is not None,
                        },
                        ensure_ascii=False,
                    ),
                )
            except Exception:
                pass

    combined_tables = filter_tables_for_sales_order(combined_tables)
    combined_tables = [table_add_rows_matrix(t) for t in combined_tables]
    combined_tables = filter_tables_for_sales_order(combined_tables)

    try:
        for t in combined_tables:
            if not isinstance(t, dict):
                continue
            if str(t.get("table_kind") or "").strip().lower() in {"total_order_grid", "partial_deliveries_grid"}:
                try:
                    normalize_size_grid_columns(t)
                except Exception:
                    pass
    except Exception:
        pass

    combined_tables = dedup_top_level_ai_kv_tables(combined_tables)

    try:
        payload0 = build_sales_order_payload(combined_tables)
        grid0 = ((payload0.get("total_order") or {}).get("grid")) if isinstance(payload0, dict) else None
        if not (isinstance(grid0, list) and len(grid0) > 0):
            parsed = parse_total_order_from_text("\n".join(combined_texts))
            if isinstance(parsed, dict):
                unit_lot = parsed.get("unit_lot") if isinstance(parsed.get("unit_lot"), str) else None
                if unit_lot is not None:
                    try:
                        rows_p = parsed.get("rows")
                        if isinstance(rows_p, list):
                            rows_p.append({"COLOUR": "UNIT LOT", "XS": str(unit_lot)})
                            parsed["rows"] = rows_p
                    except Exception:
                        pass
                combined_tables.append(parsed)
                combined_tables = [table_add_rows_matrix(t) for t in combined_tables]
                try:
                    for t in combined_tables:
                        if isinstance(t, dict) and str(t.get("table_kind") or "").strip().lower() == "total_order_grid":
                            normalize_size_grid_columns(t)
                except Exception:
                    pass
    except Exception:
        pass

    dt = time.perf_counter() - t0
    try:
        if log_json is not None:
            log_json(
                logger,
                logging.INFO,
                "ocr_extract_done",
                {
                    "request_id": request_id,
                    "doc_filename": filename,
                    "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
                    "page_count": len(all_pages),
                    "duration_sec": round(dt, 4),
                    "warnings": len(warnings),
                    "errors": len(errors),
                },
            )
    except Exception:
        pass

    out: Dict[str, Any] = {
        "schema_version": "1.0",
        "document_meta": {
            "request_id": request_id,
            "filename": filename,
            "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
            "preprocess": {"enabled": False, "mode": None, "target": "pdf_text"},
            "page_count": len(all_pages),
            "timings": {"total_sec": round(dt, 4)},
        },
        "warnings": warnings,
        "errors": errors,
        "filename": filename,
        "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
        "pages": all_pages,
        "text": "\n\n".join(combined_texts).strip(),
        "tables": combined_tables,
        "fields": combined_fields,
        "field_pairs": combined_field_pairs,
        "sales_order_payload": build_sales_order_payload(combined_tables),
    }

    if is_hm_supp:
        try:
            so = out.get("sales_order_payload")
            if isinstance(so, dict):
                hdr = so.get("header") if isinstance(so.get("header"), dict) else {}
                # HM Supplementary: override obvious mis-mapped values (contamination) using HM header table.
                hdr2 = _hm_merge_header_prefer_patch(hdr, hm_header_patch)
                so["header"] = _merge_header_if_missing(hdr2, hm_header_patch)
                out["sales_order_payload"] = so
        except Exception:
            pass
        if hm_bom_payload is not None:
            out["bom_payload"] = hm_bom_payload

    return out
