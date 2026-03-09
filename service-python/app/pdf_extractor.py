import json
import logging
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional


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
    pages_text = pdf_text_pages(file_bytes)
    joined = "\n\n".join([t for t in (pages_text or []) if (t or "").strip()]).strip()
    page_count = len(pages_text or [])
    pdf_tables_pages = pdf_tables_pages_tabula(file_bytes, page_count) if page_count else None
    has_pdf_tables = any((isinstance(p, list) and len(p) > 0) for p in (pdf_tables_pages or []))

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

    return {
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
