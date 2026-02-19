import os
from typing import Any, Dict, List, Literal, Optional, Tuple

import cv2
import numpy as np
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from PIL import Image

try:
    import pytesseract
except Exception:  # pragma: no cover
    pytesseract = None

try:
    from paddleocr import PaddleOCR
except Exception:  # pragma: no cover
    PaddleOCR = None

try:
    from pdf2image import convert_from_bytes
except Exception:  # pragma: no cover
    convert_from_bytes = None

app = FastAPI(title="Python OCR Service", version="0.1.0")


_ENGINE = Literal["tesseract", "paddle"]

_paddle_ocr_singleton: Optional["PaddleOCR"] = None


def _ensure_bgr(image: np.ndarray) -> np.ndarray:
    if image.ndim == 2:
        return cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
    if image.shape[2] == 4:
        return cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)
    return image


def preprocess_opencv(image_bgr: np.ndarray) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Simple, safe preprocessing pipeline for receipts/forms.

    Returns:
        processed_gray: np.ndarray (H,W)
        meta: dict
    """

    meta: Dict[str, Any] = {}
    bgr = _ensure_bgr(image_bgr)

    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    meta["step_gray"] = True

    # denoise a bit
    den = cv2.bilateralFilter(gray, d=7, sigmaColor=50, sigmaSpace=50)
    meta["step_denoise"] = "bilateralFilter(d=7, sigmaColor=50, sigmaSpace=50)"

    # adaptive threshold
    thr = cv2.adaptiveThreshold(
        den,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        9,
    )
    meta["step_threshold"] = "adaptiveThreshold(blockSize=31, C=9)"

    return thr, meta


def _decode_image_bytes(file_bytes: bytes) -> np.ndarray:
    arr = np.frombuffer(file_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
    if img is None:
        raise ValueError("Unable to decode image bytes")
    return img


def _images_from_upload(filename: str, file_bytes: bytes) -> List[np.ndarray]:
    lower = filename.lower()

    if lower.endswith(".pdf"):
        if convert_from_bytes is None:
            raise RuntimeError("pdf2image is not available. Install pdf2image and poppler.")

        pages = convert_from_bytes(file_bytes, dpi=250)
        images: List[np.ndarray] = []
        for page in pages:
            rgb = np.array(page.convert("RGB"))
            bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
            images.append(bgr)
        return images

    # image
    return [_decode_image_bytes(file_bytes)]


def _run_tesseract(image_gray_or_bgr: np.ndarray) -> Dict[str, Any]:
    if pytesseract is None:
        raise RuntimeError("pytesseract not installed")

    cmd = os.getenv("TESSERACT_CMD")
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd

    # pytesseract expects RGB or grayscale PIL
    if image_gray_or_bgr.ndim == 2:
        pil_img = Image.fromarray(image_gray_or_bgr)
    else:
        rgb = cv2.cvtColor(_ensure_bgr(image_gray_or_bgr), cv2.COLOR_BGR2RGB)
        pil_img = Image.fromarray(rgb)

    text = pytesseract.image_to_string(pil_img)

    # word-level boxes with confidence
    data = pytesseract.image_to_data(pil_img, output_type=pytesseract.Output.DICT)
    words: List[Dict[str, Any]] = []
    n = len(data.get("text", []))
    for i in range(n):
        w = (data["text"][i] or "").strip()
        if not w:
            continue
        conf_raw = data.get("conf", [None])[i]
        try:
            conf = float(conf_raw)
        except Exception:
            conf = None

        words.append(
            {
                "text": w,
                "confidence": conf,
                "bbox": {
                    "left": int(data["left"][i]),
                    "top": int(data["top"][i]),
                    "width": int(data["width"][i]),
                    "height": int(data["height"][i]),
                },
                "line_num": int(data.get("line_num", [0])[i]) if "line_num" in data else None,
                "word_num": int(data.get("word_num", [0])[i]) if "word_num" in data else None,
            }
        )

    avg_conf = None
    confs = [w["confidence"] for w in words if isinstance(w.get("confidence"), (int, float)) and w["confidence"] >= 0]
    if confs:
        avg_conf = float(sum(confs) / len(confs))

    return {
        "engine": "tesseract",
        "text": text,
        "avg_confidence": avg_conf,
        "words": words,
    }


def _get_paddle_ocr() -> "PaddleOCR":
    global _paddle_ocr_singleton
    if _paddle_ocr_singleton is None:
        if PaddleOCR is None:
            raise RuntimeError("paddleocr not installed")

        # Use English by default; adjust later if needed.
        _paddle_ocr_singleton = PaddleOCR(use_angle_cls=True, lang="en")
    return _paddle_ocr_singleton


def _run_paddle(image_bgr: np.ndarray) -> Dict[str, Any]:
    ocr = _get_paddle_ocr()

    bgr = _ensure_bgr(image_bgr)
    result = ocr.ocr(bgr, cls=True)

    lines: List[Dict[str, Any]] = []
    texts: List[str] = []

    # result shape: List[ [ [box], (text, conf) ], ... ] per image
    for item in (result or []):
        for line in item:
            box, (text, conf) = line
            texts.append(text)
            lines.append(
                {
                    "text": text,
                    "confidence": float(conf) if conf is not None else None,
                    "polygon": [{"x": float(p[0]), "y": float(p[1])} for p in box],
                }
            )

    avg_conf = None
    confs = [l["confidence"] for l in lines if isinstance(l.get("confidence"), (int, float))]
    if confs:
        avg_conf = float(sum(confs) / len(confs))

    return {
        "engine": "paddle",
        "text": "\n".join(texts),
        "avg_confidence": avg_conf,
        "lines": lines,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/ocr/extract")
async def ocr_extract(
    request: Request,
    file: Optional[UploadFile] = File(None),
    engine: _ENGINE = Query("tesseract"),
    preprocess: bool = Query(True),
) -> JSONResponse:
    content_type = request.headers.get("content-type")

    if file is not None:
        filename = file.filename or "uploaded"
        file_bytes = await file.read()
    else:
        filename = request.headers.get("x-filename") or "uploaded"
        file_bytes = await request.body()

    if not file_bytes:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Empty file",
                "content_type": content_type,
            },
        )

    try:
        images_bgr = _images_from_upload(filename, file_bytes)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"Failed to load file: {e}",
                "content_type": content_type,
                "filename": filename,
                "body_size": len(file_bytes),
                "has_multipart_file": file is not None,
            },
        )

    all_pages: List[Dict[str, Any]] = []
    combined_texts: List[str] = []

    for page_idx, img_bgr in enumerate(images_bgr, start=1):
        prep_meta: Dict[str, Any] = {"enabled": preprocess}
        input_for_ocr: np.ndarray

        if preprocess:
            processed_gray, meta = preprocess_opencv(img_bgr)
            prep_meta.update(meta)
            input_for_ocr = processed_gray
        else:
            input_for_ocr = img_bgr

        try:
            if engine == "tesseract":
                page_res = _run_tesseract(input_for_ocr)
            else:
                # paddle expects bgr
                if input_for_ocr.ndim == 2:
                    bgr = cv2.cvtColor(input_for_ocr, cv2.COLOR_GRAY2BGR)
                else:
                    bgr = input_for_ocr
                page_res = _run_paddle(bgr)

            page_res["page"] = page_idx
            page_res["preprocess"] = prep_meta

            all_pages.append(page_res)
            combined_texts.append(page_res.get("text") or "")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"OCR failed on page {page_idx}: {e}")

    return JSONResponse(
        {
            "filename": filename,
            "engine": engine,
            "pages": all_pages,
            "text": "\n\n".join(combined_texts).strip(),
        }
    )
