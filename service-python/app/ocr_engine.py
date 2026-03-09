from typing import Any, Callable, Dict


def run_page_ocr(
    *,
    engine: str,
    input_for_ocr: Any,
    image_for_tables: Any,
    run_tesseract: Callable[[Any], Dict[str, Any]],
    run_paddle: Callable[[Any], Dict[str, Any]],
    run_paddle_structure: Callable[[Any], Dict[str, Any]],
    ensure_bgr: Callable[[Any], Any],
    merge_text: Callable[[str, str, Any], str],
    postprocess_ocr_text: Callable[[str], str],
    extract_fields_smart: Callable[[str, Any], Dict[str, Any]],
    extract_tables_from_paddle_page: Callable[[Any, Dict[str, Any]], Any],
) -> Dict[str, Any]:
    if engine == "tesseract":
        page_res = run_tesseract(input_for_ocr)
    elif engine == "paddle_structure":
        page_res = run_paddle_structure(ensure_bgr(input_for_ocr))
        page_res["avg_confidence"] = None
        page_res["text"] = postprocess_ocr_text(page_res.get("text") or "")
        page_res["fields"] = extract_fields_smart(page_res.get("text") or "", page_res.get("tables") or [])
    elif engine == "paddle_ensemble":
        bgr = ensure_bgr(input_for_ocr)
        struct_res = run_paddle_structure(bgr)
        paddle_res = run_paddle(bgr)

        merged_text = merge_text(struct_res.get("text") or "", paddle_res.get("text") or "", paddle_res.get("avg_confidence"))
        merged_text = postprocess_ocr_text(merged_text)

        page_res = {
            "engine": "paddle_ensemble",
            "layout": struct_res.get("layout") or [],
            "tables": struct_res.get("tables") or [],
            "text": merged_text,
            "avg_confidence": paddle_res.get("avg_confidence"),
            "lines": paddle_res.get("lines") or [],
        }

        if not page_res.get("tables"):
            page_res["tables"] = extract_tables_from_paddle_page(image_for_tables, {"lines": page_res.get("lines")})
        page_res["fields"] = extract_fields_smart(merged_text, page_res.get("tables") or [])
    else:
        page_res = run_paddle(ensure_bgr(input_for_ocr))

    if engine == "paddle":
        page_res["tables"] = extract_tables_from_paddle_page(image_for_tables, page_res)
        page_res["text"] = postprocess_ocr_text(page_res.get("text") or "")
        page_res["fields"] = extract_fields_smart(page_res.get("text") or "", page_res.get("tables") or [])

    return page_res
