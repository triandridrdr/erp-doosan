from typing import Any, Callable, Dict, Tuple


def preprocess_for_engine(
    *,
    engine: str,
    preprocess: bool,
    preprocess_mode: str,
    img_bgr: Any,
    preprocess_paddle_mode: Callable[[Any, str], Tuple[Any, Dict[str, Any]]],
    preprocess_opencv_mode: Callable[[Any, str], Tuple[Any, Dict[str, Any]]],
) -> Tuple[Any, Any, Dict[str, Any]]:
    prep_meta: Dict[str, Any] = {"enabled": preprocess}

    if preprocess:
        if engine in ("paddle", "paddle_structure", "paddle_ensemble"):
            processed_bgr, meta = preprocess_paddle_mode(img_bgr, preprocess_mode)
            prep_meta.update({"target": "paddle"})
            prep_meta.update(meta)
            input_for_ocr = processed_bgr
            image_for_tables = processed_bgr
        else:
            processed_gray, meta = preprocess_opencv_mode(img_bgr, preprocess_mode)
            prep_meta.update({"target": "tesseract"})
            prep_meta.update(meta)
            input_for_ocr = processed_gray
            image_for_tables = processed_gray
    else:
        input_for_ocr = img_bgr
        image_for_tables = img_bgr

    return input_for_ocr, image_for_tables, prep_meta
