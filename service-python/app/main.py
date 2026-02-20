import os
import re
from html.parser import HTMLParser
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
    from paddleocr import PPStructure
except Exception:  # pragma: no cover
    PPStructure = None

try:
    from pdf2image import convert_from_bytes
except Exception:  # pragma: no cover
    convert_from_bytes = None

app = FastAPI(title="Python OCR Service", version="0.1.0")


_ENGINE = Literal["tesseract", "paddle", "paddle_structure", "paddle_ensemble"]
_PREPROCESS_MODE = Literal["basic", "photo"]

_paddle_ocr_singleton: Optional["PaddleOCR"] = None
_ppstructure_singleton: Optional["PPStructure"] = None


class _TableHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_td = False
        self.in_th = False
        self.in_tr = False
        self.current_cell: List[str] = []
        self.current_row: List[str] = []
        self.rows: List[List[str]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        t = tag.lower()
        if t == "tr":
            self.in_tr = True
            self.current_row = []
        elif t == "td":
            self.in_td = True
            self.current_cell = []
        elif t == "th":
            self.in_th = True
            self.current_cell = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in ("td", "th"):
            cell = " ".join("".join(self.current_cell).split()).strip()
            self.current_row.append(cell)
            self.current_cell = []
            self.in_td = False
            self.in_th = False
        elif t == "tr":
            if self.current_row:
                self.rows.append(self.current_row)
            self.current_row = []
            self.in_tr = False

    def handle_data(self, data: str) -> None:
        if self.in_td or self.in_th:
            self.current_cell.append(data)


def _parse_table_html(html: str) -> Optional[Dict[str, Any]]:
    if not html:
        return None

    cleaned = re.sub(r"<br\s*/?>", " ", html, flags=re.IGNORECASE)
    parser = _TableHtmlParser()
    try:
        parser.feed(cleaned)
    except Exception:
        return None

    rows = [r for r in parser.rows if any((c or "").strip() for c in r)]
    if len(rows) < 2:
        return None

    max_cols = max(len(r) for r in rows)
    norm_rows = [r + [""] * (max_cols - len(r)) for r in rows]

    header_idx = 0
    for i, r in enumerate(norm_rows[:5]):
        filled = sum(1 for c in r if c.strip())
        if filled >= max(2, int(0.6 * max_cols)):
            header_idx = i
            break

    headers = [c.strip() if c.strip() else f"col_{i+1}" for i, c in enumerate(norm_rows[header_idx])]
    out_rows: List[Dict[str, str]] = []
    for r in norm_rows[header_idx + 1 :]:
        obj: Dict[str, str] = {}
        for h, v in zip(headers, r):
            obj[h] = (v or "").strip()
        if any(v.strip() for v in obj.values()):
            out_rows.append(obj)

    if not out_rows:
        return None

    return {
        "headers": headers,
        "rows": out_rows,
        "row_count": len(out_rows),
        "column_count": len(headers),
    }


def _polygon_to_bbox(polygon: List[Dict[str, float]]) -> Dict[str, float]:
    xs = [float(p["x"]) for p in polygon]
    ys = [float(p["y"]) for p in polygon]
    x0 = float(min(xs))
    x1 = float(max(xs))
    y0 = float(min(ys))
    y1 = float(max(ys))
    return {
        "x": x0,
        "y": y0,
        "w": max(0.0, x1 - x0),
        "h": max(0.0, y1 - y0),
        "x_center": (x0 + x1) / 2.0,
        "y_center": (y0 + y1) / 2.0,
        "x2": x1,
        "y2": y1,
    }


def _cluster_1d(values: List[float], tol: float) -> List[float]:
    if not values:
        return []
    v = sorted(values)
    clusters: List[List[float]] = [[v[0]]]
    for x in v[1:]:
        if abs(x - clusters[-1][-1]) <= tol:
            clusters[-1].append(x)
        else:
            clusters.append([x])
    return [float(sum(c) / len(c)) for c in clusters]


def _assign_to_nearest(value: float, centers: List[float]) -> int:
    best_i = 0
    best_d = float("inf")
    for i, c in enumerate(centers):
        d = abs(value - c)
        if d < best_d:
            best_d = d
            best_i = i
    return best_i


def _order_points(pts: np.ndarray) -> np.ndarray:
    rect = np.zeros((4, 2), dtype="float32")
    s = pts.sum(axis=1)
    rect[0] = pts[np.argmin(s)]
    rect[2] = pts[np.argmax(s)]
    diff = np.diff(pts, axis=1)
    rect[1] = pts[np.argmin(diff)]
    rect[3] = pts[np.argmax(diff)]
    return rect


def _four_point_transform(image: np.ndarray, pts: np.ndarray) -> np.ndarray:
    rect = _order_points(pts)
    (tl, tr, br, bl) = rect

    width_a = np.linalg.norm(br - bl)
    width_b = np.linalg.norm(tr - tl)
    max_width = int(max(width_a, width_b))

    height_a = np.linalg.norm(tr - br)
    height_b = np.linalg.norm(tl - bl)
    max_height = int(max(height_a, height_b))

    if max_width < 10 or max_height < 10:
        return image

    dst = np.array(
        [[0, 0], [max_width - 1, 0], [max_width - 1, max_height - 1], [0, max_height - 1]], dtype="float32"
    )

    m = cv2.getPerspectiveTransform(rect, dst)
    warped = cv2.warpPerspective(image, m, (max_width, max_height))
    return warped


def _try_perspective_normalize(image_bgr: np.ndarray) -> np.ndarray:
    bgr = _ensure_bgr(image_bgr)
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)

    edges = cv2.Canny(gray, 50, 150)
    edges = cv2.dilate(edges, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=2)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return bgr

    contours = sorted(contours, key=cv2.contourArea, reverse=True)
    h, w = bgr.shape[:2]
    img_area = float(h * w)

    for c in contours[:10]:
        area = float(cv2.contourArea(c))
        if area < 0.2 * img_area:
            continue
        peri = cv2.arcLength(c, True)
        approx = cv2.approxPolyDP(c, 0.02 * peri, True)
        if len(approx) == 4:
            pts = approx.reshape(4, 2).astype("float32")
            return _four_point_transform(bgr, pts)

    return bgr


def _estimate_skew_angle_deg(binary_img: np.ndarray) -> float:
    img = binary_img
    if img.ndim != 2:
        img = cv2.cvtColor(_ensure_bgr(img), cv2.COLOR_BGR2GRAY)
    if img.dtype != np.uint8:
        img = np.clip(img, 0, 255).astype(np.uint8)

    ys, xs = np.where(img < 128)
    if len(xs) < 2000:
        return 0.0

    coords = np.column_stack((xs, ys)).astype(np.float32)
    mean = np.mean(coords, axis=0)
    centered = coords - mean
    cov = np.cov(centered.T)
    eigvals, eigvecs = np.linalg.eig(cov)
    idx = int(np.argmax(eigvals))
    vx, vy = eigvecs[:, idx]
    angle = float(np.degrees(np.arctan2(vy, vx)))

    while angle < -90:
        angle += 180
    while angle > 90:
        angle -= 180

    if abs(angle) > 15:
        return 0.0
    return angle


def _rotate_bound(image: np.ndarray, angle_deg: float) -> np.ndarray:
    if abs(angle_deg) < 0.05:
        return image
    (h, w) = image.shape[:2]
    center = (w / 2.0, h / 2.0)
    m = cv2.getRotationMatrix2D(center, angle_deg, 1.0)
    cos = abs(m[0, 0])
    sin = abs(m[0, 1])
    n_w = int((h * sin) + (w * cos))
    n_h = int((h * cos) + (w * sin))
    m[0, 2] += (n_w / 2.0) - center[0]
    m[1, 2] += (n_h / 2.0) - center[1]
    return cv2.warpAffine(image, m, (n_w, n_h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)


def _detect_table_regions(binary_or_gray: np.ndarray) -> List[Dict[str, int]]:
    img = binary_or_gray
    if img.ndim != 2:
        img = cv2.cvtColor(_ensure_bgr(img), cv2.COLOR_BGR2GRAY)

    if img.dtype != np.uint8:
        img = np.clip(img, 0, 255).astype(np.uint8)

    _, thr = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    if float(np.mean(thr)) < 127.0:
        thr = 255 - thr

    h, w = thr.shape[:2]
    if h < 10 or w < 10:
        return []

    kx = max(10, w // 40)
    ky = max(10, h // 40)

    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kx, 1))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, ky))

    horiz = cv2.erode(thr, horizontal_kernel, iterations=1)
    horiz = cv2.dilate(horiz, horizontal_kernel, iterations=2)

    vert = cv2.erode(thr, vertical_kernel, iterations=1)
    vert = cv2.dilate(vert, vertical_kernel, iterations=2)

    grid = cv2.bitwise_or(horiz, vert)
    grid = cv2.dilate(grid, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)

    contours, _ = cv2.findContours(grid, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    regions: List[Dict[str, int]] = []
    min_area = max(5000, int(0.01 * w * h))
    for c in contours:
        x, y, ww, hh = cv2.boundingRect(c)
        area = ww * hh
        if area < min_area:
            continue
        if ww < int(0.2 * w) or hh < int(0.08 * h):
            continue
        regions.append({"x": int(x), "y": int(y), "w": int(ww), "h": int(hh)})

    regions.sort(key=lambda r: (r["y"], r["x"]))

    merged: List[Dict[str, int]] = []
    for r in regions:
        rx0, ry0 = r["x"], r["y"]
        rx1, ry1 = r["x"] + r["w"], r["y"] + r["h"]

        did_merge = False
        for m in merged:
            mx0, my0 = m["x"], m["y"]
            mx1, my1 = m["x"] + m["w"], m["y"] + m["h"]

            ix0 = max(rx0, mx0)
            iy0 = max(ry0, my0)
            ix1 = min(rx1, mx1)
            iy1 = min(ry1, my1)

            if ix1 > ix0 and iy1 > iy0:
                nx0 = min(rx0, mx0)
                ny0 = min(ry0, my0)
                nx1 = max(rx1, mx1)
                ny1 = max(ry1, my1)
                m["x"], m["y"], m["w"], m["h"] = int(nx0), int(ny0), int(nx1 - nx0), int(ny1 - ny0)
                did_merge = True
                break

        if not did_merge:
            merged.append(r)

    return merged


def _reconstruct_table_from_boxes(boxes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(boxes) < 6:
        return None

    heights = [float(b["bbox"]["h"]) for b in boxes if b.get("bbox") is not None]
    if not heights:
        return None

    med_h = float(np.median(np.array(heights)))
    row_tol = max(8.0, med_h * 0.9)

    boxes_sorted = sorted(boxes, key=lambda b: (float(b["bbox"]["y_center"]), float(b["bbox"]["x_center"])))

    rows: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    current_y: Optional[float] = None

    for b in boxes_sorted:
        y = float(b["bbox"]["y_center"])
        if current_y is None:
            current_y = y
            current = [b]
            continue
        if abs(y - current_y) <= row_tol:
            current.append(b)
            current_y = float(sum(float(x["bbox"]["y_center"]) for x in current) / len(current))
        else:
            rows.append(sorted(current, key=lambda x: float(x["bbox"]["x_center"])))
            current_y = y
            current = [b]

    if current:
        rows.append(sorted(current, key=lambda x: float(x["bbox"]["x_center"])))

    rows = [r for r in rows if len(r) >= 2]
    if len(rows) < 2:
        return None

    max_cols = max(len(r) for r in rows)
    candidate_rows = [r for r in rows if len(r) >= max(2, int(0.6 * max_cols))]
    if not candidate_rows:
        candidate_rows = rows
    header_row = min(candidate_rows, key=lambda r: float(np.median([float(b["bbox"]["y_center"]) for b in r])))

    x_centers: List[float] = []
    for r in rows:
        if len(r) < 2:
            continue
        for b in r:
            x_centers.append(float(b["bbox"]["x_center"]))

    if not x_centers:
        return None
    col_tol = max(12.0, med_h * 1.2)
    col_centers = _cluster_1d(x_centers, tol=col_tol)
    if len(col_centers) < 2:
        return None

    headers_by_col: List[str] = ["" for _ in col_centers]
    for b in header_row:
        idx = _assign_to_nearest(float(b["bbox"]["x_center"]), col_centers)
        txt = (b.get("text") or "").strip()
        if not txt:
            continue
        if headers_by_col[idx]:
            headers_by_col[idx] = (headers_by_col[idx] + " " + txt).strip()
        else:
            headers_by_col[idx] = txt

    headers = [h.strip() if h.strip() else f"col_{i+1}" for i, h in enumerate(headers_by_col)]

    kv_rows: List[Dict[str, str]] = []
    for r in rows:
        if r is header_row:
            continue
        row_obj: Dict[str, str] = {h: "" for h in headers}
        for b in r:
            idx = _assign_to_nearest(float(b["bbox"]["x_center"]), col_centers)
            key = headers[idx]
            txt = (b.get("text") or "").strip()
            if not txt:
                continue
            if row_obj[key]:
                row_obj[key] = (row_obj[key] + " " + txt).strip()
            else:
                row_obj[key] = txt
        if any(v.strip() for v in row_obj.values()):
            kv_rows.append(row_obj)

    if not kv_rows:
        return None

    return {
        "headers": headers,
        "rows": kv_rows,
        "row_count": len(kv_rows),
        "column_count": len(headers),
    }


def _extract_tables_from_paddle_page(image_gray_or_bgr: np.ndarray, page_res: Dict[str, Any]) -> List[Dict[str, Any]]:
    lines = page_res.get("lines") or []
    if not isinstance(lines, list) or not lines:
        return []

    boxes: List[Dict[str, Any]] = []
    for l in lines:
        poly = l.get("polygon")
        if not poly:
            continue
        bbox = _polygon_to_bbox(poly)
        boxes.append(
            {
                "text": l.get("text"),
                "confidence": l.get("confidence"),
                "bbox": bbox,
            }
        )

    if not boxes:
        return []

    regions = _detect_table_regions(image_gray_or_bgr)

    tables: List[Dict[str, Any]] = []
    if regions:
        for i, r in enumerate(regions, start=1):
            rx0, ry0 = float(r["x"]), float(r["y"])
            rx1, ry1 = float(r["x"] + r["w"]), float(r["y"] + r["h"])
            in_region = [
                b
                for b in boxes
                if (rx0 <= float(b["bbox"]["x_center"]) <= rx1 and ry0 <= float(b["bbox"]["y_center"]) <= ry1)
            ]
            reconstructed = _reconstruct_table_from_boxes(in_region)
            if reconstructed is None:
                continue
            reconstructed["table_index"] = i
            reconstructed["bbox"] = {"x": r["x"], "y": r["y"], "w": r["w"], "h": r["h"]}
            tables.append(reconstructed)

    if tables:
        return tables

    reconstructed = _reconstruct_table_from_boxes(boxes)
    if reconstructed is None:
        return []
    reconstructed["table_index"] = 1
    reconstructed["bbox"] = None
    return [reconstructed]


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

    raise RuntimeError("Use preprocess_opencv_mode")


def preprocess_opencv_mode(image_bgr: np.ndarray, mode: _PREPROCESS_MODE) -> Tuple[np.ndarray, Dict[str, Any]]:
    meta: Dict[str, Any] = {"mode": mode}
    bgr = _ensure_bgr(image_bgr)

    if mode == "basic":
        h, w = bgr.shape[:2]
        if max(h, w) < 900:
            scale = 2.0
            bgr = cv2.resize(bgr, dsize=None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
            meta["step_upscale"] = f"resize(fx={scale}, fy={scale}, interpolation=INTER_CUBIC)"

        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        meta["step_gray"] = True

        den = cv2.bilateralFilter(gray, d=7, sigmaColor=50, sigmaSpace=50)
        meta["step_denoise"] = "bilateralFilter(d=7, sigmaColor=50, sigmaSpace=50)"

        thr = cv2.adaptiveThreshold(
            den,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            9,
        )
        meta["step_threshold"] = "adaptiveThreshold(blockSize=31, C=9)"

        if float(np.mean(thr)) < 127.0:
            thr = 255 - thr
            meta["step_invert"] = True

        return thr, meta


def preprocess_paddle_mode(image_bgr: np.ndarray, mode: _PREPROCESS_MODE) -> Tuple[np.ndarray, Dict[str, Any]]:
    meta: Dict[str, Any] = {"mode": mode}
    bgr = _ensure_bgr(image_bgr)

    h0, w0 = bgr.shape[:2]
    if max(h0, w0) < 1400:
        scale = 2.0
        bgr = cv2.resize(bgr, dsize=None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
        meta["step_upscale"] = f"resize(fx={scale}, fy={scale}, interpolation=INTER_CUBIC)"

    if mode == "photo":
        bgr = _try_perspective_normalize(bgr)
        meta["step_perspective"] = True

    lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    l2 = clahe.apply(l)
    lab2 = cv2.merge([l2, a, b])
    bgr = cv2.cvtColor(lab2, cv2.COLOR_LAB2BGR)
    meta["step_contrast"] = "CLAHE_L_channel(clipLimit=2.0, tileGridSize=(8,8))"

    bgr = cv2.bilateralFilter(bgr, d=7, sigmaColor=50, sigmaSpace=50)
    meta["step_denoise"] = "bilateralFilter_color(d=7, sigmaColor=50, sigmaSpace=50)"

    # mild unsharp mask
    blur = cv2.GaussianBlur(bgr, (0, 0), 1.0)
    bgr = cv2.addWeighted(bgr, 1.5, blur, -0.5, 0)
    meta["step_sharpen"] = "unsharp(amount=0.5, sigma=1.0)"

    return bgr, meta


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

        # Use latin model by default (works better for Indonesian than 'en').
        lang = os.getenv("PADDLE_OCR_LANG") or "latin"
        _paddle_ocr_singleton = PaddleOCR(use_angle_cls=True, lang=lang, use_space_char=True)
    return _paddle_ocr_singleton


def _get_ppstructure() -> "PPStructure":
    global _ppstructure_singleton
    if _ppstructure_singleton is None:
        if PPStructure is None:
            raise RuntimeError("PPStructure not available in paddleocr")

        lang = os.getenv("PADDLE_OCR_LANG") or "latin"
        _ppstructure_singleton = PPStructure(
            lang=lang,
            show_log=False,
        )
    return _ppstructure_singleton


def _run_paddle_structure(image_bgr: np.ndarray) -> Dict[str, Any]:
    engine = _get_ppstructure()
    bgr = _ensure_bgr(image_bgr)

    # Returns list of blocks: text, title, figure, table, etc.
    result = engine(bgr)
    layout: List[Dict[str, Any]] = []
    tables: List[Dict[str, Any]] = []
    text_parts: List[str] = []

    for idx, block in enumerate(result or [], start=1):
        b_type = block.get("type")
        bbox = block.get("bbox")
        layout.append({"index": idx, "type": b_type, "bbox": bbox})

        if b_type in ("text", "title"):
            res = block.get("res")
            if isinstance(res, list):
                for item in res:
                    if isinstance(item, dict) and item.get("text"):
                        text_parts.append(str(item.get("text")))
                    elif isinstance(item, str):
                        text_parts.append(item)
            elif isinstance(res, dict) and res.get("text"):
                text_parts.append(str(res.get("text")))
            elif isinstance(res, str):
                text_parts.append(res)

        if b_type == "table":
            html = None
            res = block.get("res")
            if isinstance(res, dict):
                html = res.get("html")
            parsed = _parse_table_html(html or "")
            if parsed is None:
                continue
            parsed["table_index"] = len(tables) + 1
            parsed["bbox"] = None
            if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                parsed["bbox"] = {"x": int(bbox[0]), "y": int(bbox[1]), "w": int(bbox[2] - bbox[0]), "h": int(bbox[3] - bbox[1])}
            tables.append(parsed)

    # We keep text empty here; caller can still compute combined text from blocks if needed.
    return {
        "engine": "paddle_structure",
        "layout": layout,
        "tables": tables,
        "text": "\n".join([t for t in text_parts if str(t).strip()]).strip(),
    }


def _extract_fields_from_text(text: str) -> Dict[str, Any]:
    t = (text or "").replace("\r", "\n")
    t_norm = "\n".join(" ".join(line.split()) for line in t.split("\n"))

    def _first(pattern: str) -> Optional[str]:
        m = re.search(pattern, t_norm, flags=re.IGNORECASE)
        if not m:
            return None
        g = m.group(1) if m.lastindex else m.group(0)
        g = (g or "").strip()
        return g or None

    fields: Dict[str, Any] = {}
    fields["order_no"] = _first(r"\border\s*nr\b\s*[:\-]?\s*([A-Z0-9\-\/]+)")
    fields["date"] = _first(r"\bdate\b\s*[:\-]?\s*([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})")
    fields["supplier"] = _first(r"\bsupplier\b\s*[:\-]?\s*(.+)")
    fields["season"] = _first(r"\bseason\b\s*[:\-]?\s*([A-Z0-9\s]+)")
    fields["buyer"] = _first(r"\bbuyer\b\s*[:\-]?\s*([A-Z0-9\s]+)")
    fields["payment_terms"] = _first(r"\bpayment\s*terms\b\s*[:\-]?\s*(.+)")
    fields["purchaser"] = _first(r"\bpurchaser\b\s*[:\-]?\s*(.+)")

    cleaned = {k: v for k, v in fields.items() if v is not None}
    return cleaned


def _text_quality_score(text: str) -> float:
    s = (text or "").strip()
    if not s:
        return 0.0

    n = len(s)
    letters = sum(1 for ch in s if ch.isalpha())
    digits = sum(1 for ch in s if ch.isdigit())
    spaces = sum(1 for ch in s if ch.isspace())
    good = letters + digits + spaces
    weird = n - good

    lines = [ln for ln in s.split("\n") if ln.strip()]
    line_bonus = min(30.0, float(len(lines)))
    score = (letters * 1.0) + (digits * 0.6) + (spaces * 0.1) - (weird * 1.5) + line_bonus
    score += min(50.0, float(n) / 50.0)
    return float(score)


def _merge_text(struct_text: str, paddle_text: str, paddle_avg_conf: Optional[float]) -> str:
    st = (struct_text or "").strip()
    pt = (paddle_text or "").strip()

    s_score = _text_quality_score(st)
    p_score = _text_quality_score(pt)
    if isinstance(paddle_avg_conf, (int, float)):
        p_score += float(paddle_avg_conf) / 10.0

    if p_score > s_score * 1.05:
        return pt
    if s_score > p_score * 1.05:
        return st

    # Similar quality: merge unique lines
    st_lines = [ln.strip() for ln in st.split("\n") if ln.strip()]
    pt_lines = [ln.strip() for ln in pt.split("\n") if ln.strip()]
    seen = set()
    merged: List[str] = []
    for ln in st_lines + pt_lines:
        key = re.sub(r"\s+", " ", ln).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(ln)
    return "\n".join(merged).strip()


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
    preprocess_mode: _PREPROCESS_MODE = Query("basic"),
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

        image_for_tables: np.ndarray

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

        try:
            if engine == "tesseract":
                page_res = _run_tesseract(input_for_ocr)
            elif engine == "paddle_structure":
                # structure expects bgr
                page_res = _run_paddle_structure(_ensure_bgr(input_for_ocr))
                page_res["avg_confidence"] = None
                page_res["fields"] = _extract_fields_from_text(page_res.get("text") or "")
            elif engine == "paddle_ensemble":
                bgr = _ensure_bgr(input_for_ocr)
                struct_res = _run_paddle_structure(bgr)
                paddle_res = _run_paddle(bgr)

                merged_text = _merge_text(struct_res.get("text") or "", paddle_res.get("text") or "", paddle_res.get("avg_confidence"))

                page_res = {
                    "engine": "paddle_ensemble",
                    "layout": struct_res.get("layout") or [],
                    "tables": struct_res.get("tables") or [],
                    "text": merged_text,
                    "avg_confidence": paddle_res.get("avg_confidence"),
                    "lines": paddle_res.get("lines") or [],
                }

                # If structure didn't output tables, fall back to heuristic paddle tables
                if not page_res.get("tables"):
                    page_res["tables"] = _extract_tables_from_paddle_page(image_for_tables, {"lines": page_res.get("lines")})

                page_res["fields"] = _extract_fields_from_text(merged_text)
            else:
                # paddle expects bgr
                page_res = _run_paddle(_ensure_bgr(input_for_ocr))

            page_res["page"] = page_idx
            page_res["preprocess"] = prep_meta

            if engine == "paddle":
                page_res["tables"] = _extract_tables_from_paddle_page(image_for_tables, page_res)

            all_pages.append(page_res)
            combined_texts.append(page_res.get("text") or "")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"OCR failed on page {page_idx}: {e}")

    combined_tables: List[Dict[str, Any]] = []
    for p in all_pages:
        for t in (p.get("tables") or []):
            combined_tables.append({"page": p.get("page"), **t})

    combined_fields: List[Dict[str, Any]] = []
    for p in all_pages:
        if p.get("fields"):
            combined_fields.append({"page": p.get("page"), **(p.get("fields") or {})})

    return JSONResponse(
        {
            "filename": filename,
            "engine": engine,
            "pages": all_pages,
            "text": "\n\n".join(combined_texts).strip(),
            "tables": combined_tables,
            "fields": combined_fields,
        }
    )
