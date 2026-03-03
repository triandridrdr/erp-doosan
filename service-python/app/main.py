import io
import json
import os
import re
import base64
import time
import uuid
import logging
import sys
import tempfile
from html.parser import HTMLParser
from typing import Any, Dict, List, Literal, Optional, Tuple

import cv2
import numpy as np
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from PIL import Image

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None

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

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

try:
    import pdfplumber
except Exception:  # pragma: no cover
    pdfplumber = None

try:
    import tabula
except Exception:  # pragma: no cover
    tabula = None


logger = logging.getLogger("python_ocr")
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

try:
    _so_dbg_tables_boot = str(os.environ.get("SO_DEBUG_PDF_TABLES") or "").strip().lower() in {"1", "true", "yes", "on"}
    if _so_dbg_tables_boot:
        logging.getLogger().setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
except Exception:
    pass

app = FastAPI(title="Python OCR Service", version="0.1.0")

try:
    from celery.result import AsyncResult

    from .celery_app import celery_app
    from .tasks import ocr_extract_task
except Exception:  # pragma: no cover
    AsyncResult = None
    celery_app = None
    ocr_extract_task = None


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


def _looks_like_pdf_bytes(file_bytes: bytes) -> bool:
    try:
        if not isinstance(file_bytes, (bytes, bytearray)):
            return False
        if len(file_bytes) < 5:
            return False
        return bytes(file_bytes[:5]) == b"%PDF-"
    except Exception:
        return False


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


def _pdf_tables_pages_tabula(file_bytes: bytes, page_count: int) -> Optional[List[List[Dict[str, Any]]]]:
    if tabula is None:
        return None

    if page_count <= 0:
        return []

    def _cell_str(v: Any) -> str:
        if v is None:
            return ""
        try:
            if isinstance(v, float) and np.isnan(v):
                return ""
        except Exception:
            pass
        return str(v).strip()

    def _looks_like_header_row(row: List[str]) -> bool:
        if not row:
            return False
        joined = " ".join([c for c in row if c]).strip()
        if not joined:
            return False
        alpha = len(re.findall(r"[A-Z]", joined.upper()))
        digits = len(re.findall(r"\d", joined))
        return alpha >= 3 and alpha >= digits

    def _df_to_table(df: Any) -> Optional[Dict[str, Any]]:
        try:
            values = df.values.tolist()
        except Exception:
            return None
        if not isinstance(values, list) or not values:
            return None

        rm: List[List[str]] = []
        for r in values:
            if not isinstance(r, list):
                continue
            rr = [_cell_str(c) for c in r]
            if any(c.strip() for c in rr):
                rm.append(rr)
        if not rm:
            return None

        cols = max((len(r) for r in rm), default=0)
        if cols <= 0:
            return None
        rm = [r + [""] * (cols - len(r)) for r in rm]

        headers: List[str]
        start_idx = 0
        if _looks_like_header_row(rm[0]):
            headers = [c if c else f"COL_{i+1}" for i, c in enumerate(rm[0])]
            start_idx = 1
        else:
            headers = [f"COL_{i+1}" for i in range(cols)]

        rows: List[Dict[str, Any]] = []
        for r in rm[start_idx:]:
            row_obj: Dict[str, Any] = {}
            for i, h in enumerate(headers):
                if i < len(r):
                    row_obj[h] = r[i]
            if any(str(v or "").strip() for v in row_obj.values()):
                rows.append(row_obj)

        if not rows:
            return None
        return {"headers": headers, "rows": rows}

    tmp_path: Optional[str] = None
    try:
        # NOTE: On Windows, NamedTemporaryFile keeps the file handle open and tabula-java
        # cannot open it ("being used by another process"). Use a closed temp file path.
        fd, tmp_path = tempfile.mkstemp(suffix=".pdf")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(file_bytes)
                f.flush()
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
            raise

        out: List[List[Dict[str, Any]]] = []
        for p in range(1, page_count + 1):
            try:
                dfs = tabula.read_pdf(
                    tmp_path,
                    pages=p,
                    multiple_tables=True,
                    guess=True,
                    pandas_options={"header": None},
                )
            except Exception as e:
                dfs = None
                try:
                    so_dbg_tables = str(os.environ.get("SO_DEBUG_PDF_TABLES") or "").strip().lower() in {"1", "true", "yes", "on"}
                    if so_dbg_tables:
                        logger.info(
                            "so_pdf_tabula_read_pdf_failed %s",
                            json.dumps(
                                {
                                    "event": "so_pdf_tabula_read_pdf_failed",
                                    "page": p,
                                    "error": str(e),
                                },
                                ensure_ascii=False,
                            ),
                        )
                except Exception:
                    pass

            page_tables: List[Dict[str, Any]] = []
            if isinstance(dfs, list):
                for df in dfs:
                    t = _df_to_table(df)
                    if isinstance(t, dict) and t.get("headers") and t.get("rows"):
                        page_tables.append(t)
            out.append(page_tables)
        return out
    except Exception:
        return None
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


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


def _canon_num_token(s: Any) -> str:
    t = str(s or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    # common pdfplumber/tabula artifact: thousands separator with a space after comma: '5, 338'
    t = re.sub(r"(\d),(\s+)(\d)", r"\1\3", t)
    t = t.replace(" ", "")
    return t


def _parse_total_order_from_text(txt: str) -> Optional[Dict[str, Any]]:
    if not isinstance(txt, str) or not txt.strip():
        return None
    s = txt.replace("\r", "\n")
    s = re.sub(r"\u00A0", " ", s)
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in s.split("\n")]
    lines = [ln for ln in lines if ln]
    if not lines:
        return None

    start = None
    for i, ln in enumerate(lines):
        if re.search(r"\bTOTAL\s+ORDER\b", ln, flags=re.IGNORECASE):
            start = i
            break
    if start is None:
        return None

    end = len(lines)
    for j in range(start + 1, len(lines)):
        if re.search(r"\bPARTIAL\s+DELIVERIES\b", lines[j], flags=re.IGNORECASE):
            end = j
            break
        if re.search(r"\bpage\s+\d+\s+of\s+\d+\b", lines[j], flags=re.IGNORECASE):
            end = j
            break

    chunk = lines[start:end]
    if not chunk:
        return None

    unit_lot = None
    for ln in chunk:
        m = re.search(r"\bUNIT\s*LOT\b\s*(\d{1,6})\b", ln, flags=re.IGNORECASE)
        if m:
            unit_lot = str(m.group(1) or "").strip() or None
            break

    # Find the header row with size tokens
    header_idx = None
    for i, ln in enumerate(chunk[:10]):
        if re.search(r"\bCOLOU?R\b", ln, flags=re.IGNORECASE) and re.search(r"\bXS\b", ln) and re.search(r"\bXL\b", ln, flags=re.IGNORECASE):
            header_idx = i
            break
    if header_idx is None:
        return None

    out_rows: List[Dict[str, str]] = []

    def _extract_nums(ln: str) -> List[str]:
        try:
            s0 = str(ln or "")
            if not s0.strip():
                return []
            # Normalize common thousands formatting: '5, 338' -> '5,338'
            s0 = re.sub(r"(\d),\s+(\d{3})\b", r"\1,\2", s0)
            s0 = re.sub(r"\s+", " ", s0).strip()
            toks = [t for t in s0.split(" ") if t and re.search(r"\d", t) is not None]
            out: List[str] = []
            for t in toks:
                # keep only digits and separators, then canonicalize
                t2 = re.sub(r"[^0-9,\.\s]", "", t)
                c = _canon_num_token(t2)
                if c and re.search(r"\d", c) is not None:
                    out.append(c)
            return out
        except Exception:
            return []

    i = header_idx + 1
    while i < len(chunk):
        ln = chunk[i]
        if re.fullmatch(r"TOTAL", ln.strip(), flags=re.IGNORECASE):
            i += 1
            continue
        if re.search(r"\bUNIT\s*LOT\b", ln, flags=re.IGNORECASE):
            i += 1
            continue
        if re.search(r"\bTHIS\s+GARMENT\b", ln, flags=re.IGNORECASE):
            break
        if re.search(r"\bIMAGE\s+AND\s+MEASURES\b", ln, flags=re.IGNORECASE):
            break
        if re.search(r"\bLOGISTIC\s+ORDER\b", ln, flags=re.IGNORECASE):
            break

        m = re.search(r"\b(\d{2,6})\s*[-–—]\s*([A-Z][A-Z0-9\s/]+?)\b(.*)$", ln, flags=re.IGNORECASE)
        if not m:
            i += 1
            continue

        colour = f"{(m.group(1) or '').strip()} - {(m.group(2) or '').strip()}".strip(" -")
        tail = (m.group(3) or "").strip()
        nums: List[str] = []
        if tail:
            nums.extend(_extract_nums(tail))

        # pdf text sometimes wraps numbers onto next lines; accumulate until we have XS..Total (6 nums)
        j = i + 1
        while len(nums) < 6 and j < len(chunk):
            nxt = chunk[j]
            if re.search(r"\bUNIT\s*LOT\b", nxt, flags=re.IGNORECASE):
                break
            if re.fullmatch(r"TOTAL", nxt.strip(), flags=re.IGNORECASE):
                break
            if re.search(r"\bTHIS\s+GARMENT\b", nxt, flags=re.IGNORECASE):
                break
            if re.search(r"\bIMAGE\s+AND\s+MEASURES\b", nxt, flags=re.IGNORECASE):
                break
            if re.search(r"\bLOGISTIC\s+ORDER\b", nxt, flags=re.IGNORECASE):
                break
            # stop if we hit the next colour row
            if re.search(r"\b\d{2,6}\s*[-–—]\s*[A-Z]", nxt, flags=re.IGNORECASE):
                break
            nums.extend(_extract_nums(nxt))
            j += 1

        if len(nums) >= 6:
            xs, s2, m2, l2, xl2, tot = nums[0], nums[1], nums[2], nums[3], nums[4], nums[5]
            out_rows.append({"COLOUR": colour, "XS": xs, "S": s2, "M": m2, "L": l2, "XL": xl2, "Total": tot})
            i = j
            continue

        i += 1

    if not out_rows:
        return None
    return {
        "headers": ["COLOUR", "XS", "S", "M", "L", "XL", "Total"],
        "rows": out_rows,
        "table_kind": "total_order_grid",
        "include_headers_in_rows_matrix": True,
        "_source": "pdf_text_total_order",
        "unit_lot": unit_lot,
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


def _pdf_text_pages(file_bytes: bytes) -> Optional[List[str]]:
    if pdfplumber is None:
        return None

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            pages_text: List[str] = []
            for p in pdf.pages:
                txt = p.extract_text() or ""
                txt = txt.replace("\r", "\n").strip()
                pages_text.append(txt)
        return pages_text
    except Exception:
        return None


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


def _fields_to_pairs(fields: Any) -> List[Dict[str, Any]]:
    if not isinstance(fields, dict):
        return []
    out: List[Dict[str, Any]] = []
    for k, v in fields.items():
        if v is None:
            continue
        sv = str(v).strip() if not isinstance(v, (dict, list)) else v
        if isinstance(sv, str) and not sv:
            continue
        out.append({"key": str(k), "value": sv})
    return out


def _build_ai_kv_table_from_fields(fields: Any) -> Dict[str, Any]:
    canonical_keys = [
        "ORDER-NR",
        "DATE",
        "SEASON",
        "BUYER",
        "PURCHASER",
        "SUPPLIER",
        "SEND TO",
        "PAYMENT TERMS",
        "SUPPLIER REF",
        "ARTICLE",
        "DESCRIPTION",
        "MARKET OF ORIGIN",
        "PVP",
        "COMPOSITION LABEL / PART COLORS",
        "COMPOSITIONS INFORMATION",
        "CARE INSTRUCTIONS",
        "HANGTAG LABEL",
        "MAIN LABEL",
        "EXTERNAL FABRIC",
        "HANGING",
        "TOTAL ORDER",
    ]

    field_to_key = {
        "order_no": "ORDER-NR",
        "date": "DATE",
        "season": "SEASON",
        "buyer": "BUYER",
        "purchaser": "PURCHASER",
        "supplier": "SUPPLIER",
        "send_to": "SEND TO",
        "payment_terms": "PAYMENT TERMS",
        "supplier_ref": "SUPPLIER REF",
        "article": "ARTICLE",
        "description": "DESCRIPTION",
        "market_of_origin": "MARKET OF ORIGIN",
        "pvp": "PVP",
        "composition_label_part_colors": "COMPOSITION LABEL / PART COLORS",
        "compositions_information": "COMPOSITIONS INFORMATION",
        "care_instructions": "CARE INSTRUCTIONS",
        "hangtag_label": "HANGTAG LABEL",
        "main_label": "MAIN LABEL",
        "external_fabric": "EXTERNAL FABRIC",
        "hanging": "HANGING",
        "total_order": "TOTAL ORDER",
    }

    values: Dict[str, str] = {}
    if isinstance(fields, dict):
        for fk, kk in field_to_key.items():
            v = fields.get(fk)
            if isinstance(v, str) and v.strip():
                values[str(kk).strip().upper()] = v.strip()

    # guardrails for frequently polluted keys
    try:
        v = (values.get("SUPPLIER REF") or "").strip()
        if v:
            vv = v.replace(" ", "").upper()
            if re.fullmatch(r"\d{2,4}\-?[A-Z]{2,}", vv):
                values["SUPPLIER REF"] = ""
            if re.search(r"\b(TOTAL|OUTER\s+SHELL|LINING|COMPOSITION|COMPOSITIONS|CARE|INSTRUCTIONS)\b", v, flags=re.IGNORECASE):
                values["SUPPLIER REF"] = ""
    except Exception:
        pass

    kv_pairs_all = [{"key": k, "value": values.get(k, "")} for k in canonical_keys]
    return {
        "headers": ["key", "value"],
        "rows": [{"key": p.get("key"), "value": p.get("value")} for p in kv_pairs_all],
        "rows_matrix": [[str(p.get("key") or ""), str(p.get("value") or "")] for p in kv_pairs_all],
        "kv_pairs_all": kv_pairs_all,
    }


def _infer_table_kind_generic(tbl: Any) -> Optional[str]:
    if not isinstance(tbl, dict):
        return None
    if str(tbl.get("table_kind") or "").strip():
        return str(tbl.get("table_kind") or "").strip()

    try:
        headers = tbl.get("headers") or []
        header_text = " ".join([str(h or "") for h in headers]).upper()
    except Exception:
        header_text = ""

    try:
        rm = tbl.get("rows_matrix") or []
        head = (
            " ".join([str(x or "") for row in rm[:6] if isinstance(row, list) for x in row]).upper()
            if isinstance(rm, list)
            else ""
        )
    except Exception:
        head = ""

    blob = (header_text + " " + head).strip()
    if not blob:
        return None

    size_tokens = ["XXS", "XS", "S", "M", "L", "XL", "XXL"]
    size_hits = sum(1 for tok in size_tokens if re.search(r"\b" + re.escape(tok) + r"\b", blob) is not None)

    has_colour = re.search(r"\bCOLOU?R\b", blob, flags=re.IGNORECASE) is not None
    has_total = re.search(r"\bTOTAL\b", blob, flags=re.IGNORECASE) is not None
    has_unit_lot = re.search(r"\bUNIT\s*LOT\b", blob, flags=re.IGNORECASE) is not None

    has_logistic = re.search(r"\bLOGISTIC\s+ORDER\b", blob, flags=re.IGNORECASE) is not None
    has_delivery = re.search(r"\bDELIVERY\b", blob, flags=re.IGNORECASE) is not None
    has_incoterm = re.search(r"\bINCOTERM\b", blob, flags=re.IGNORECASE) is not None
    has_handover = re.search(r"\bHANDOVER\s+DATE\b", blob, flags=re.IGNORECASE) is not None
    has_transport = re.search(r"\bTRANSPORT\s+MODE\b", blob, flags=re.IGNORECASE) is not None
    has_presentation = re.search(r"\bPRESENTATION\s+TYPE\b", blob, flags=re.IGNORECASE) is not None

    has_article = re.search(r"\bARTICLE\b", blob, flags=re.IGNORECASE) is not None
    has_option = re.search(r"\bOPTION\b", blob, flags=re.IGNORECASE) is not None
    has_cost = (
        re.search(r"\bCOST\b", blob, flags=re.IGNORECASE) is not None
        or re.search(r"\bCOST\s*PRICE\b", blob, flags=re.IGNORECASE) is not None
    )
    has_qty = re.search(r"\bQ(TY|UANTITY)\b", blob, flags=re.IGNORECASE) is not None
    has_unit_price = re.search(r"\bUNIT\s*PRICE\b", blob, flags=re.IGNORECASE) is not None

    pd_score = 0
    if has_logistic:
        pd_score += 4
    if has_delivery:
        pd_score += 2
    if has_incoterm:
        pd_score += 2
    if has_handover:
        pd_score += 1
    if has_transport:
        pd_score += 1
    if has_presentation:
        pd_score += 1
    if size_hits >= 2:
        pd_score += 2
    if has_colour:
        pd_score += 1

    grid_score = 0
    if size_hits >= 2:
        grid_score += 4
    if has_colour:
        grid_score += 2
    if has_total:
        grid_score += 1
    if has_unit_lot:
        grid_score += 1

    line_item_score = 0
    if has_article:
        line_item_score += 3
    if has_option:
        line_item_score += 2
    if has_cost or has_unit_price:
        line_item_score += 2
    if has_qty:
        line_item_score += 1

    if pd_score >= 6:
        return "partial_deliveries_grid"
    if grid_score >= 6 and pd_score < 5:
        return "total_order_grid"
    if line_item_score >= 5 and grid_score < 6:
        return "line_item_table"
    return None


def _table_add_rows_matrix(tbl: Any) -> Any:
    if not isinstance(tbl, dict):
        return tbl
    headers = tbl.get("headers") or []
    rows = tbl.get("rows") or []
    if not isinstance(headers, list) or not isinstance(rows, list):
        return tbl

    headers_s = [str(h) for h in headers]
    rows_matrix: List[List[str]] = []
    for r in rows:
        if isinstance(r, dict):
            rows_matrix.append([str(r.get(h, "") or "") for h in headers_s])
        elif isinstance(r, list):
            rows_matrix.append([str(x or "") for x in r])
        else:
            rows_matrix.append([str(r)])

    if tbl.get("include_headers_in_rows_matrix") is True and headers_s:
        try:
            pre = tbl.get("pre_rows_matrix")
            pre_s: List[List[str]] = []
            if isinstance(pre, list) and pre:
                pre_s: List[List[str]] = []
                for rr in pre:
                    if isinstance(rr, list):
                        pre_s.append([str(x or "") for x in rr])
                    elif isinstance(rr, dict):
                        pre_s.append([str(rr.get(h, "") or "") for h in headers_s])
                    else:
                        pre_s.append([str(rr or "")])
                rows_matrix = pre_s + rows_matrix

            # Allow placing the header after the pre-rows (metadata first, then header row)
            if tbl.get("header_row_after_pre") is True and pre_s:
                # Ensure we don't double-insert
                if len(rows_matrix) < (len(pre_s) + 1) or rows_matrix[len(pre_s)] != headers_s:
                    rows_matrix = rows_matrix[: len(pre_s)] + [headers_s] + rows_matrix[len(pre_s) :]
            else:
                # Default behavior: header on top
                if not rows_matrix or rows_matrix[0] != headers_s:
                    rows_matrix = [headers_s] + rows_matrix
        except Exception:
            pass

    tbl["rows_matrix"] = rows_matrix
    try:
        infer_fn = globals().get("_infer_table_kind_generic")
        if callable(infer_fn):
            tk = infer_fn(tbl)
            if tk and not str(tbl.get("table_kind") or "").strip():
                tbl["table_kind"] = tk
                if str(tk).strip().lower() in {"total_order_grid", "partial_deliveries_grid"}:
                    tbl["include_headers_in_rows_matrix"] = True
    except Exception:
        pass

    # Extract key/value pairs from form-like tables (Sales Order / Purchase Order headers)
    label_aliases = {
        "ORDER-NR": {"order-nr", "order nr", "order no", "order number", "sales order", "sales order no", "nomor so", "no so"},
        "DATE": {"date", "tanggal", "tgl"},
        "SUPPLIER": {"supplier", "vendor", "pemasok"},
        "SEASON": {"season"},
        "BUYER": {"buyer", "pembeli"},
        "PAYMENT TERMS": {"payment terms", "terms of payment", "syarat pembayaran"},
        "PURCHASER": {"purchaser", "pemesan"},
        "SEND TO": {"send to", "sendto", "send t0", "send t o", "ship to", "shipto", "kirim ke"},
        "SUPPLIER REF": {"supplier ref", "supplierref", "supplier-re f", "supplierre f"},
        "ARTICLE": {"article", "articie", "artlcle"},
        "DESCRIPTION": {"description", "descripton", "descriplion", "desc"},
        "MARKET OF ORIGIN": {"market of origin", "market origin", "marketoforigin", "market/origin", "origin"},
        "PVP": {"pvp"},
        "COMPOSITIONS INFORMATION": {
            "compositions information",
            "composition information",
            "compotitions information",
            "compotition information",
            "compositions infomation",
            "composition weighted colors",
        },
        "CARE INSTRUCTIONS": {
            "care instructions",
            "care instruction",
            "care introtion",
            "care intruction",
            "care instruction s",
        },
    }

    def _cell_norm(s: str) -> str:
        return _norm_key(s)

    def _looks_like_label(s: str) -> Optional[str]:
        ns = _cell_norm(s)
        if not ns:
            return None
        # Special-case: SEND TO / SHIP TO is frequently OCR'd with missing spaces or 0/O swaps
        if ("send" in ns and ("to" in ns or ns.endswith("t0"))) or ("ship" in ns and "to" in ns) or ns in {"sendto", "shipto"}:
            return "SEND TO"
        for key, aliases in label_aliases.items():
            for a in aliases:
                if ns == _cell_norm(a):
                    return key
        if fuzz is None:
            return None
        best_key = None
        best = 0.0
        for key, aliases in label_aliases.items():
            for a in aliases:
                na = _cell_norm(a)
                if not na:
                    continue
                score = float(fuzz.ratio(ns, na))
                if score > best:
                    best = score
                    best_key = key
        return best_key if best_key is not None and best >= 90.0 else None

    def _split_value_label(s: str) -> Optional[Tuple[str, str]]:
        # Return (label, value)
        t = (s or "").strip()
        if not t:
            return None

        label = _looks_like_label(t)
        if label is not None:
            return (label, "")

        # Check label at the end by token split first (handles OCR spacing/punctuation)
        tokens = [tok for tok in re.split(r"\s+", t) if tok]
        if len(tokens) >= 2:
            for take in (1, 2, 3):
                if len(tokens) <= take:
                    continue
                tail = " ".join(tokens[-take:])
                head = " ".join(tokens[:-take]).strip(" :-\t")
                lbl = _looks_like_label(tail)
                if lbl is not None and head:
                    return (lbl, head)

        # Check label at the end: "54721-D ORDER-NR" or "1517 BUYER"
        for key, aliases in label_aliases.items():
            for a in aliases:
                a_txt = str(a).strip()
                if not a_txt:
                    continue
                pat = re.compile(r"\b" + re.escape(a_txt) + r"\b\s*$", flags=re.IGNORECASE)
                if pat.search(t):
                    value = pat.sub("", t).strip(" :-\t")
                    if value:
                        return (key, value)

        # Check label at the start: "ORDER-NR 54721-D"
        for key, aliases in label_aliases.items():
            for a in aliases:
                a_txt = str(a).strip()
                if not a_txt:
                    continue
                pat = re.compile(r"^\s*" + re.escape(a_txt) + r"\b\s*[:\-]?\s*", flags=re.IGNORECASE)
                if pat.search(t):
                    value = pat.sub("", t).strip()
                    if value:
                        return (key, value)

        return None

    def _value_contains_label(value: str) -> bool:
        # If the value itself looks like it contains another label token (e.g. "DATE 11/07/2025"),
        # treat it as contaminated and don't use it as a value for a different key.
        t = (value or "").strip()
        if not t:
            return False
        try:
            sp = _split_value_label(t)
            if sp is not None and sp[0] and sp[1]:
                return True
        except Exception:
            return False

        # Additional: if any tail token group looks like a label, consider contaminated
        tokens = [tok for tok in re.split(r"\s+", t) if tok]
        if len(tokens) >= 2:
            for take in (1, 2, 3):
                if len(tokens) <= take:
                    continue
                tail = " ".join(tokens[-take:])
                if _looks_like_label(tail) is not None:
                    return True
        return False

    kv_pairs: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, list):
            continue
        cells = [(c or "").strip() for c in row if isinstance(c, str) and (c or "").strip()]
        if not cells:
            continue

        # 1) Single-cell patterns: "value LABEL" or "LABEL value"
        for c in cells:
            split = _split_value_label(c)
            if split is not None:
                k, v = split
                if v:
                    kv_pairs.append({"key": k, "value": v})

        # 2) Adjacent cell pairing: [LABEL, VALUE] or [VALUE, LABEL]
        for idx in range(0, len(cells) - 1):
            a = cells[idx]
            b = cells[idx + 1]
            la = _looks_like_label(a)
            lb = _looks_like_label(b)
            if la is not None and lb is None and b and not _value_contains_label(b):
                kv_pairs.append({"key": la, "value": b})
            elif lb is not None and la is None and a and not _value_contains_label(a):
                kv_pairs.append({"key": lb, "value": a})

    # 2.5) Item header mapping: SUPPLIER REF | ARTICLE | DESCRIPTION | MARKET OF ORIGIN | PVP then values below
    # This is common in garment SO/PO and is more reliable than generic vertical capture.
    # Robust to merged/colspan tables by using index ranges between labels.
    try:
        item_keys = {"SUPPLIER REF", "ARTICLE", "DESCRIPTION", "MARKET OF ORIGIN", "PVP"}
        header_row_idx: Optional[int] = None
        header_cols: List[Tuple[int, str]] = []

        for r in range(0, len(rows_matrix)):
            row = rows_matrix[r]
            if not isinstance(row, list) or not row:
                continue
            found_cols: List[Tuple[int, str]] = []
            for c, cell in enumerate(row):
                if not isinstance(cell, str):
                    continue
                k = _looks_like_label(cell)
                if k is not None and k in item_keys:
                    found_cols.append((c, k))
            found_keys = {k for _, k in found_cols}
            if len(found_keys) >= 3 and ("ARTICLE" in found_keys or "DESCRIPTION" in found_keys):
                header_row_idx = r
                header_cols = sorted(found_cols, key=lambda x: x[0])
                break

        def _row_has_any_label(row_any: List[Any]) -> bool:
            for vv in row_any:
                if not isinstance(vv, str):
                    continue
                if _looks_like_label((vv or "").strip()) is not None:
                    return True
            return False

        if header_row_idx is not None and header_cols:
            # Build index ranges per key: [col_i, col_{i+1})
            ranges: List[Tuple[str, int, int]] = []
            for i, (c, k) in enumerate(header_cols):
                start = c
                end = header_cols[i + 1][0] if i + 1 < len(header_cols) else 10_000
                ranges.append((k, start, end))

            def _reject_supplier_ref(v: str) -> bool:
                # SUPPLIER REF is often blank in the document; don't accidentally take values from the composition/care sections.
                if not v:
                    return True
                if re.search(r"\b(OUTER\s+SHELL|LINING|HANGTAG|LABEL|COLOUR|COLOR|COMPOSITION|COMPOSITIONS|CARE\s+INSTRUCTIONS|INSTRUCTIONS)\b", v, flags=re.IGNORECASE) is not None:
                    return True
                # Prices/currencies belong to PVP, not supplier ref
                if re.search(r"\b(EUR|USD|GBP)\b", v, flags=re.IGNORECASE) is not None and re.search(r"\b\d+[\.,]\d{2}\b", v) is not None:
                    return True
                # Weighted color / color code patterns (e.g. "660-WINE") are not supplier refs
                if re.fullmatch(r"\d{2,4}\s*\-\s*[A-Z]{2,}", v.replace(" ", "").upper()):
                    return True
                return False

            pvp_parts: List[str] = []
            max_rows_scan = 10
            for rr in range(header_row_idx + 1, min(len(rows_matrix), header_row_idx + 1 + max_rows_scan)):
                next_row = rows_matrix[rr]
                if not isinstance(next_row, list):
                    break
                # Stop if we hit another labels row
                if _row_has_any_label(next_row):
                    break

                for key, start, end in ranges:
                    # already filled (except PVP multi-line)
                    if key != "PVP" and any(p.get("key") == key for p in kv_pairs):
                        continue

                    picked: Optional[str] = None
                    for cc in range(start, min(end, len(next_row))):
                        v = next_row[cc]
                        if not isinstance(v, str):
                            continue
                        v = (v or "").strip()
                        if not v:
                            continue
                        if _value_contains_label(v):
                            continue
                        if key == "SUPPLIER REF" and _reject_supplier_ref(v):
                            continue
                        picked = v
                        break

                    if not picked:
                        continue

                    if key == "PVP":
                        if re.search(r"\b(EUR|USD|GBP)\b", picked, flags=re.IGNORECASE) or re.search(r"\b\d+[\.,]\d{2}\b", picked):
                            pvp_parts.append(picked)
                    else:
                        kv_pairs.append({"key": key, "value": picked})

            if pvp_parts:
                kv_pairs.append({"key": "PVP", "value": " ".join(pvp_parts).strip()})
    except Exception:
        pass

    # 3) Vertical same-column pairing: header label on row r, value(s) below on row r+1.. (common in SO/PO)
    try:
        max_follow_rows = 6
        item_header_keys = {"SUPPLIER REF", "ARTICLE", "DESCRIPTION", "MARKET OF ORIGIN", "PVP"}
        long_text_keys = {"SEND TO"}
        boundary_labels = {
            "COMPOSITIONS INFORMATION",
            "CARE INSTRUCTIONS",
            "ORDER-NR",
            "DATE",
            "SUPPLIER",
            "SEASON",
            "BUYER",
            "PAYMENT TERMS",
            "PURCHASER",
            "SEND TO",
        }
        for r in range(0, len(rows_matrix) - 1):
            row = rows_matrix[r]
            if not isinstance(row, list):
                continue
            for c in range(0, len(row)):
                cell = row[c]
                if not isinstance(cell, str):
                    continue
                label = _looks_like_label(cell)
                if label is None:
                    continue

                parts: List[str] = []
                if label in item_header_keys:
                    local_max = 2
                elif label in long_text_keys:
                    local_max = 5
                else:
                    local_max = max_follow_rows
                for rr in range(r + 1, min(len(rows_matrix), r + 1 + local_max)):
                    next_row = rows_matrix[rr]
                    if not isinstance(next_row, list) or c >= len(next_row):
                        break
                    v = next_row[c]
                    if not isinstance(v, str):
                        break
                    v = (v or "").strip()
                    if not v:
                        # allow one empty row (OCR sometimes inserts blank spacer)
                        if parts:
                            break
                        continue
                    v_label = _looks_like_label(v)
                    if v_label is not None:
                        break
                    # Stop if the row contains a known boundary label somewhere else.
                    # Exception: for SEND TO, the address block often shares rows with ORDER-NR/DATE/etc in other columns.
                    if label not in long_text_keys:
                        try:
                            row_has_boundary = False
                            for vv in next_row:
                                if not isinstance(vv, str):
                                    continue
                                kx = _looks_like_label((vv or "").strip())
                                if kx is not None and kx in boundary_labels:
                                    row_has_boundary = True
                                    break
                            if row_has_boundary:
                                break
                        except Exception:
                            pass
                    if _value_contains_label(v):
                        break
                    parts.append(v)
                if parts:
                    kv_pairs.append({"key": label, "value": " ".join(parts).strip()})
    except Exception:
        pass

    # 4) Section capture: some keys (COMPOSITIONS INFORMATION / CARE INSTRUCTIONS) span multiple columns/rows
    try:
        section_keys = {"COMPOSITIONS INFORMATION", "CARE INSTRUCTIONS"}
        max_section_rows = 10
        for r in range(0, len(rows_matrix) - 1):
            row = rows_matrix[r]
            if not isinstance(row, list):
                continue
            for c in range(0, len(row)):
                cell = row[c]
                if not isinstance(cell, str):
                    continue
                key = _looks_like_label(cell)
                if key is None or key not in section_keys:
                    continue

                parts: List[str] = []
                for rr in range(r + 1, min(len(rows_matrix), r + 1 + max_section_rows)):
                    next_row = rows_matrix[rr]
                    if not isinstance(next_row, list):
                        break
                    row_texts: List[str] = []
                    for vv in next_row:
                        if not isinstance(vv, str):
                            continue
                        vvs = (vv or "").strip()
                        if not vvs:
                            continue
                        if _looks_like_label(vvs) is not None:
                            continue
                        if _value_contains_label(vvs):
                            continue
                        row_texts.append(vvs)
                    if not row_texts:
                        if parts:
                            break
                        continue
                    parts.append(" ".join(row_texts))
                if parts:
                    kv_pairs.append({"key": key, "value": " ".join(parts).strip()})
    except Exception:
        pass

    # Deduplicate
    seen_kv = set()
    dedup: List[Dict[str, Any]] = []
    for p in kv_pairs:
        k = str(p.get("key") or "").strip().upper()
        v = str(p.get("value") or "").strip()
        if not k or not v:
            continue
        sig = (k, v)
        if sig in seen_kv:
            continue
        seen_kv.add(sig)
        dedup.append({"key": k, "value": v})

    tbl["kv_pairs"] = dedup

    # Group by key and select best candidate value (real SO/PO headers often yield duplicates)
    def _score_value(key: str, value: str) -> float:
        k2 = (key or "").upper()
        v2 = (value or "").strip()
        if not v2:
            return 0.0
        score = float(len(v2))
        if _value_contains_label(v2):
            score -= 50.0
        if k2 in ("BUYER", "PURCHASER"):
            if re.fullmatch(r"[0-9]{2,}", v2.replace(" ", "")):
                score += 30.0
        if k2 == "DATE":
            if re.search(r"\b[0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4}\b", v2):
                score += 30.0
        if k2 == "ORDER-NR":
            if re.search(r"\b[A-Z0-9\-\/]{3,}\b", v2, flags=re.IGNORECASE):
                score += 20.0
        if k2 in ("SUPPLIER", "SEND TO"):
            if re.search(r"\b(LTD|FZE|CO\.?|TRADING)\b", v2, flags=re.IGNORECASE):
                score += 15.0
        if k2 == "PAYMENT TERMS":
            if re.search(r"\bDAYS\b", v2, flags=re.IGNORECASE):
                score += 20.0
        if k2 == "PVP":
            if re.search(r"\b(EUR|USD|GBP)\b", v2, flags=re.IGNORECASE):
                score += 40.0
            if re.search(r"\b\d+[\.,]\d{2}\b", v2):
                score += 25.0
        if k2 == "DESCRIPTION":
            if re.search(r"\b(POLYESTER|VISCOSE|RECYCLED|FILAMENT|ELASTANE|COTTON|NYLON)\b", v2, flags=re.IGNORECASE):
                score -= 35.0
            if re.search(r"\b[A-Z]\-\b", v2):
                score += 10.0
        if k2 == "MARKET OF ORIGIN":
            if re.search(r"\bINDONESIA\b", v2, flags=re.IGNORECASE):
                score += 25.0
        if k2 == "SUPPLIER REF":
            if re.fullmatch(r"[A-Z0-9\-\/]{3,}", v2.replace(" ", ""), flags=re.IGNORECASE):
                score += 10.0
            if re.search(r"\b(OUTER\s+SHELL|LINING|HANGTAG|LABEL|COLOUR|COLOR)\b", v2, flags=re.IGNORECASE):
                score -= 40.0
            if re.fullmatch(r"\d{2,4}\-?[A-Z]{2,}", v2.replace(" ", "").upper()):
                score -= 50.0
        return score

    grouped_best: Dict[str, str] = {}
    grouped_score: Dict[str, float] = {}
    for p in dedup:
        k = str(p.get("key") or "").strip().upper()
        v = str(p.get("value") or "").strip()
        if not k or not v:
            continue
        s = _score_value(k, v)
        if k not in grouped_score or s > grouped_score[k]:
            grouped_score[k] = s
            grouped_best[k] = v

    # Drop low-confidence candidates to avoid filling blanks with unrelated section text
    if "SUPPLIER REF" in grouped_best and grouped_score.get("SUPPLIER REF", 0.0) < 15.0:
        grouped_best.pop("SUPPLIER REF", None)
        grouped_score.pop("SUPPLIER REF", None)

    tbl["kv_pairs_grouped"] = [{"key": k, "value": v} for k, v in grouped_best.items()]

    # Canonical key order for Garment ERP SO/PO header/form tables
    canonical_keys = [
        "ORDER-NR",
        "DATE",
        "SEASON",
        "BUYER",
        "PURCHASER",
        "SUPPLIER",
        "SEND TO",
        "PAYMENT TERMS",
        "SUPPLIER REF",
        "ARTICLE",
        "DESCRIPTION",
        "MARKET OF ORIGIN",
        "PVP",
        "COMPOSITIONS INFORMATION",
        "CARE INSTRUCTIONS",
        "HANGTAG LABEL",
        "MAIN LABEL",
        "EXTERNAL FABRIC",
        "HANGING",
        "TOTAL ORDER",
    ]

    grouped_best_u = {str(k or "").strip().upper(): str(v or "").strip() for k, v in grouped_best.items()}
    kv_all = [{"key": k, "value": grouped_best_u.get(k, "")} for k in canonical_keys]
    # Final guardrails for keys that are frequently blank in the document but get polluted by nearby sections
    try:
        for p in kv_all:
            if not isinstance(p, dict):
                continue
            k = str(p.get("key") or "").strip().upper()
            v = str(p.get("value") or "").strip()
            if k == "SUPPLIER REF" and v:
                vv = v.replace(" ", "").upper()
                if re.fullmatch(r"\d{2,4}\-?[A-Z]{2,}", vv):
                    p["value"] = ""
                    continue
                if re.search(r"\b(OUTER\s+SHELL|LINING|HANGTAG|LABEL|COLOUR|COLOR|COMPOSITION|COMPOSITIONS|CARE\s+INSTRUCTIONS|INSTRUCTIONS)\b", v, flags=re.IGNORECASE):
                    p["value"] = ""
    except Exception:
        pass
    tbl["kv_pairs_all"] = kv_all

    try:
        generic_headers = False
        if headers_s:
            norms = [_norm_key(h) for h in headers_s]
            col_count = sum(1 for nh in norms if nh.startswith("col"))
            non_empty = sum(1 for nh in norms if nh)
            # Consider as generic/form table if most headers are col_* (even if the first header is a title like SEND TO ...)
            if non_empty > 0 and (col_count / float(non_empty)) >= 0.6:
                generic_headers = True
        if generic_headers and len(dedup) >= 3:
            tbl["raw_headers"] = tbl.get("headers")
            tbl["raw_rows"] = tbl.get("rows")
            tbl["raw_rows_matrix"] = tbl.get("rows_matrix")

            tbl["headers"] = ["key", "value"]
            use_pairs = tbl.get("kv_pairs_all") or tbl.get("kv_pairs_grouped") or dedup
            tbl["rows"] = [{"key": p.get("key"), "value": p.get("value")} for p in use_pairs]
            tbl["rows_matrix"] = [[str(p.get("key") or ""), str(p.get("value") or "")] for p in use_pairs]
    except Exception:
        pass
    return tbl


def _dedup_top_level_ai_kv_tables(tables: Any) -> Any:
    if not isinstance(tables, list) or not tables:
        return tables

    def _is_ai_kv(t: Any) -> bool:
        return isinstance(t, dict) and t.get("headers") == ["key", "value"]

    # pick the most complete AI table across pages; keep other non-AI tables untouched
    bad_value_norm = {
        "date",
        "ordernr",
        "orderno",
        "order",
        "supplier",
        "season",
        "buyer",
        "paymentterms",
        "purchaser",
        "sendto",
        "shipto",
        "taxofficenumber",
    }

    def _score_ai_kv(t: Dict[str, Any]) -> int:
        rows_matrix = t.get("rows_matrix")
        if not isinstance(rows_matrix, list):
            return 0
        score = 0
        for r in rows_matrix:
            if not isinstance(r, list) or len(r) < 2:
                continue
            v = str(r[1] or "").strip()
            if not v:
                continue
            vn = re.sub(r"[^a-z0-9]+", "", v.lower())
            if vn in bad_value_norm:
                continue
            score += 1
        return score

    ai_tables: List[Tuple[int, int, Dict[str, Any]]] = []
    non_ai: List[Dict[str, Any]] = []
    for i, t in enumerate(tables):
        if _is_ai_kv(t):
            ai_tables.append((_score_ai_kv(t), i, t))
        else:
            if isinstance(t, dict):
                non_ai.append(t)

    if not ai_tables:
        return tables

    # Higher score first; if tie, prefer earlier table (usually page 1)
    ai_tables.sort(key=lambda x: (-x[0], x[1]))
    best = ai_tables[0][2]
    return [best] + non_ai


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


def _normalize_size_grid_columns(reco: Dict[str, Any]) -> None:
    try:
        headers0 = reco.get("headers") or []
        rows0 = reco.get("rows") or []
        if not (isinstance(headers0, list) and isinstance(rows0, list) and headers0 and rows0):
            return

        headers0_s = [str(h or "").strip() for h in headers0]
        n = len(headers0_s)

        def _is_num(s: str) -> bool:
            t = (s or "").strip()
            if not t:
                return False
            if re.search(r"\d", t) is None:
                return False
            if re.search(
                r"\b(UNIT|LOT|COLOU?R|TOTAL|LOGISTIC|DELIVERY|INCOTERM|FROM|HANDOVER|TRANSPORT|PRESENTATION|COST|PRICE)\b",
                t,
                flags=re.IGNORECASE,
            ):
                return False
            return True

        def _is_text(s: str) -> bool:
            t = (s or "").strip()
            if not t:
                return False
            if re.search(r"\d", t) is not None and re.fullmatch(r"[\d,\.]+", t):
                return False
            return True

        col_numeric = [0] * n
        col_text = [0] * n
        for r in rows0:
            if not isinstance(r, dict):
                continue
            for i, hk in enumerate(headers0_s):
                v = str(r.get(hk, "") or "")
                if _is_num(v):
                    col_numeric[i] += 1
                if _is_text(v):
                    col_text[i] += 1

        def _find_header_idx(pat: str) -> Optional[int]:
            for i, h in enumerate(headers0_s):
                if re.search(pat, h, flags=re.IGNORECASE):
                    return i
            return None

        idx_colour = _find_header_idx(r"\bCOLOU?R\b")
        idx_xs = _find_header_idx(r"\bXS\b")
        idx_s = _find_header_idx(r"\bS\b")
        idx_m = _find_header_idx(r"\bM\b")
        idx_l = _find_header_idx(r"\bL\b")
        idx_xl = _find_header_idx(r"\bXL\b")
        idx_total = _find_header_idx(r"\bTOTAL\b")
        if idx_l is None:
            for i, h in enumerate(headers0_s):
                if h.strip() == "1" and col_numeric[i] > 0:
                    idx_l = i
                    break

        # Some extractions mis-detect the COLOUR header or map it to the wrong column.
        # Infer the COLOUR column by scoring which column contains colour-like strings
        # (e.g. '800 - BLACK') while being text-heavy.
        try:
            colour_pat = re.compile(r"\b\d{2,6}\s*[-–—]\s*[A-Z][A-Z0-9\s/]*\b", flags=re.IGNORECASE)
            best_idx: Optional[int] = None
            best_score: float = -1.0
            for i, hk in enumerate(headers0_s):
                if col_text[i] <= 0:
                    continue
                hits = 0
                for r in rows0:
                    if not isinstance(r, dict):
                        continue
                    s = str(r.get(hk, "") or "").strip()
                    if not s:
                        continue
                    if colour_pat.search(s) is not None:
                        hits += 1
                # Prefer columns with more matches and that appear left-most (small i).
                score = float(hits) * 10.0 + float(col_text[i]) - float(i) * 0.1
                if hits > 0 and score > best_score:
                    best_score = score
                    best_idx = i
            if best_idx is not None and (idx_colour is None or col_text[int(idx_colour)] == 0):
                idx_colour = best_idx
        except Exception:
            pass

        def _shift_to_data(i: Optional[int], kind: str) -> Optional[int]:
            if i is None:
                return None
            if kind == "colour":
                if col_text[i] > 0:
                    return i
                best_i = i
                best_score = col_text[i]
                for di in [1, -1, 2, -2]:
                    j = i + di
                    if 0 <= j < n and col_text[j] > best_score:
                        best_i = j
                        best_score = col_text[j]
                return best_i
            if col_numeric[i] > 0:
                return i
            best_i = i
            best_score = col_numeric[i]
            for di in [1, -1, 2, -2]:
                j = i + di
                if 0 <= j < n and col_numeric[j] > best_score:
                    best_i = j
                    best_score = col_numeric[j]
            return best_i

        idx_colour = _shift_to_data(idx_colour, "colour")
        idx_xs = _shift_to_data(idx_xs, "num")
        idx_s = _shift_to_data(idx_s, "num")
        idx_m = _shift_to_data(idx_m, "num")
        idx_l = _shift_to_data(idx_l, "num")
        idx_xl = _shift_to_data(idx_xl, "num")
        idx_total = _shift_to_data(idx_total, "num")

        chosen = [i for i in [idx_colour, idx_xs, idx_s, idx_m, idx_l, idx_xl, idx_total] if isinstance(i, int)]
        if idx_colour is not None and idx_total is not None:
            numeric_between = [
                i
                for i in range(idx_colour + 1, idx_total)
                if col_numeric[i] > 0 and i not in chosen
            ]
        else:
            numeric_between = [i for i in range(n) if col_numeric[i] > 0 and i not in chosen]
        numeric_between = sorted(numeric_between)

        def _fill_if_none(cur: Optional[int]) -> Optional[int]:
            if cur is not None:
                return cur
            if numeric_between:
                return numeric_between.pop(0)
            return None

        idx_xs = _fill_if_none(idx_xs)
        idx_s = _fill_if_none(idx_s)
        idx_m = _fill_if_none(idx_m)
        idx_l = _fill_if_none(idx_l)
        idx_xl = _fill_if_none(idx_xl)

        # Some Partial Deliveries grids have an unlabeled Total column (header OCR missing).
        # Infer it as the rightmost numeric-heavy column that is not already used by sizes.
        if idx_total is None:
            used = {i for i in [idx_colour, idx_xs, idx_s, idx_m, idx_l, idx_xl] if isinstance(i, int)}
            min_after = max([i for i in [idx_xl, idx_l, idx_m, idx_s, idx_xs] if isinstance(i, int)] or [-1])
            candidates = [
                i
                for i in range(n)
                if i not in used and i > min_after and col_numeric[i] > 0
            ]
            if candidates:
                idx_total = max(candidates)

        if not (idx_colour is not None and idx_xs is not None and idx_total is not None):
            return

        canonical = [
            ("COLOUR", idx_colour),
            ("XS", idx_xs),
            ("S", idx_s),
            ("M", idx_m),
            ("L", idx_l),
            ("XL", idx_xl),
            ("Total", idx_total),
        ]

        new_headers = [h for h, oi in canonical if oi is not None]
        new_rows: List[Dict[str, str]] = []
        colour_pat_fallback = re.compile(
            r"\b(\d{2,6})\s*(?:[-–—]\s*)?([A-Z][A-Z0-9\s/]+?)\b",
            flags=re.IGNORECASE,
        )
        for r in rows0:
            if not isinstance(r, dict):
                continue
            nr: Dict[str, str] = {}
            for nh, oi in canonical:
                if oi is None:
                    continue
                ok = headers0_s[int(oi)]
                nr[nh] = str(r.get(ok, "") or "")
            try:
                if (nr.get("COLOUR") or "").strip() == "":
                    blob = " ".join([str(v or "").strip() for v in r.values() if str(v or "").strip()])
                    blob = re.sub(r"\s+", " ", blob).strip()
                    if blob and re.search(r"\bUNIT\s*LOT\b", blob, flags=re.IGNORECASE) is None:
                        m = colour_pat_fallback.search(blob)
                        if m:
                            code = (m.group(1) or "").strip()
                            name = (m.group(2) or "").strip()
                            if code and name and re.search(r"\bTOTAL\b", name, flags=re.IGNORECASE) is None:
                                nr["COLOUR"] = f"{code} - {name}"
            except Exception:
                pass
            if any((v or "").strip() for v in nr.values()):
                new_rows.append(nr)

        if new_rows:
            reco["headers"] = new_headers
            reco["rows"] = new_rows
            reco["row_count"] = len(new_rows)
            reco["column_count"] = len(new_headers)
    except Exception:
        return


def _extract_total_order_grid_from_boxes(boxes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(boxes, list) or len(boxes) < 8:
        return None

    def _t(s: Any) -> str:
        return str(s or "").strip()

    # find title "TOTAL ORDER" to anchor the region
    title_y: Optional[float] = None
    for b in boxes:
        txt = _t(b.get("text"))
        if not txt:
            continue
        if re.search(r"\bTOTAL\s+ORDER\b", txt, flags=re.IGNORECASE):
            try:
                title_y = float(b.get("bbox", {}).get("y_center"))
            except Exception:
                title_y = None
            if title_y is not None:
                break

    if title_y is None:
        return None

    # take content below the title within a bounded window
    region = [
        b
        for b in boxes
        if isinstance(b, dict)
        and b.get("bbox") is not None
        and float(b["bbox"]["y_center"]) >= (title_y + 5.0)
        and float(b["bbox"]["y_center"]) <= (title_y + 520.0)
    ]

    if len(region) < 8:
        return None

    reconstructed = _reconstruct_table_from_boxes(region)
    if reconstructed is None:
        return None

    headers = reconstructed.get("headers") or []
    header_text = " ".join([str(h or "") for h in headers]).upper()
    size_tokens = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}
    hits = sum(1 for t in size_tokens if re.search(r"\b" + re.escape(t) + r"\b", header_text))
    if hits < 2:
        # Sometimes headers are not detected; try to infer from the first row values
        try:
            rows0 = reconstructed.get("rows") or []
            if isinstance(rows0, list) and rows0:
                first_row = rows0[0]
                if isinstance(first_row, dict):
                    row_text = " ".join([str(v or "") for v in first_row.values()]).upper()
                    hits = sum(1 for t in size_tokens if re.search(r"\b" + re.escape(t) + r"\b", row_text))
        except Exception:
            pass
    if hits < 2:
        return None

    # Strong hint: should include colour/color or total
    if not (re.search(r"\bCOLOU?R\b", header_text) or re.search(r"\bTOTAL\b", header_text)):
        # allow if any row contains a TOTAL marker
        has_total_row = False
        for r in (reconstructed.get("rows") or []):
            if not isinstance(r, dict):
                continue
            rt = " ".join([str(v or "") for v in r.values()]).upper()
            if re.search(r"\bTOTAL\b", rt):
                has_total_row = True
                break
        if not has_total_row:
            return None

    # tag for debugging/selection
    reconstructed["table_kind"] = "total_order_grid"
    reconstructed["include_headers_in_rows_matrix"] = True

    _normalize_size_grid_columns(reconstructed)

    # Some documents print a footer line below the grid like "UNIT LOT 1".
    # This often falls outside the inferred numeric columns and gets dropped during normalization.
    try:
        rows0 = reconstructed.get("rows") or []
        if isinstance(rows0, list):
            already = False
            for r in rows0:
                if not isinstance(r, dict):
                    continue
                if re.search(r"\bUNIT\s*LOT\b", str(r.get("COLOUR", "") or ""), flags=re.IGNORECASE):
                    already = True
                    break
            if not already:
                # Compute a y-tolerance based on median box height
                hs = []
                for b in region:
                    try:
                        hs.append(float((b.get("bbox") or {}).get("h") or 0.0))
                    except Exception:
                        pass
                med_h = float(np.median(np.array([h for h in hs if h > 0.0]))) if any(h > 0.0 for h in hs) else 18.0
                y_tol = max(10.0, med_h * 1.2)

                label_box: Optional[Dict[str, Any]] = None
                for b in sorted(region, key=lambda x: float((x.get("bbox") or {}).get("y_center") or 0.0), reverse=True):
                    txt = _t(b.get("text"))
                    if not txt:
                        continue
                    if re.search(r"\bUNIT\s*LOT\b", txt, flags=re.IGNORECASE):
                        label_box = b
                        break

                if label_box is not None:
                    lx = float(label_box.get("bbox", {}).get("x_center") or 0.0)
                    ly = float(label_box.get("bbox", {}).get("y_center") or 0.0)

                    def _is_lot_val(s: str) -> bool:
                        t2 = (s or "").strip()
                        if not t2:
                            return False
                        t2 = re.sub(r"[^0-9]", "", t2)
                        return bool(re.fullmatch(r"\d{1,6}", t2))

                    val_box: Optional[Dict[str, Any]] = None
                    for b in region:
                        if not isinstance(b, dict) or b.get("bbox") is None:
                            continue
                        by = float(b.get("bbox", {}).get("y_center") or 0.0)
                        if abs(by - ly) > y_tol:
                            continue
                        bx = float(b.get("bbox", {}).get("x_center") or 0.0)
                        if bx <= lx:
                            continue
                        txt = _t(b.get("text"))
                        if _is_lot_val(txt):
                            if val_box is None or bx < float(val_box.get("bbox", {}).get("x_center") or 0.0):
                                val_box = b

                    if val_box is not None:
                        v_raw = _t(val_box.get("text"))
                        v_norm = re.sub(r"[^0-9]", "", v_raw)
                        unit_row: Dict[str, str] = {h: "" for h in (reconstructed.get("headers") or [])}
                        # Best-effort mapping into existing canonical columns
                        if "COLOUR" in unit_row:
                            unit_row["COLOUR"] = "UNIT LOT"
                        else:
                            unit_row[str((reconstructed.get("headers") or ["COLOUR"])[0])] = "UNIT LOT"
                        if "XS" in unit_row:
                            unit_row["XS"] = v_norm
                        else:
                            # fallback: place into first non-COLOUR column
                            for h in (reconstructed.get("headers") or []):
                                if str(h).strip().upper() != "COLOUR":
                                    unit_row[str(h)] = v_norm
                                    break
                        rows0.append(unit_row)
                        reconstructed["rows"] = rows0
                        reconstructed["row_count"] = len(rows0)
    except Exception:
        pass
    return reconstructed


def _extract_partial_deliveries_grids_from_boxes(boxes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(boxes, list) or len(boxes) < 12:
        return []

    def _t(s: Any) -> str:
        return str(s or "").strip()

    anchors: List[float] = []
    for b in boxes:
        txt = _t(b.get("text"))
        if not txt:
            continue
        if re.search(r"\bLOGISTIC\s+ORDER\b", txt, flags=re.IGNORECASE):
            try:
                anchors.append(float(b.get("bbox", {}).get("y_center")))
            except Exception:
                pass

    anchors = sorted(list({a for a in anchors if isinstance(a, (int, float))}))
    if not anchors:
        return []

    out: List[Dict[str, Any]] = []
    for i, y0 in enumerate(anchors):
        y1 = anchors[i + 1] if (i + 1) < len(anchors) else (y0 + 900.0)
        region = [
            b
            for b in boxes
            if isinstance(b, dict)
            and b.get("bbox") is not None
            and float(b["bbox"]["y_center"]) >= (y0 - 10.0)
            and float(b["bbox"]["y_center"]) <= (y1 - 10.0)
        ]
        if len(region) < 12:
            continue

        colour_y: Optional[float] = None
        cost_y: Optional[float] = None
        for b in region:
            txt = _t(b.get("text"))
            if not txt:
                continue
            if colour_y is None and re.search(r"\bCOLOU?R\b", txt, flags=re.IGNORECASE):
                try:
                    colour_y = float(b.get("bbox", {}).get("y_center"))
                except Exception:
                    colour_y = None
            if cost_y is None and re.search(r"\bCOST\s*PRICE\b", txt, flags=re.IGNORECASE):
                try:
                    cost_y = float(b.get("bbox", {}).get("y_center"))
                except Exception:
                    cost_y = None

        if colour_y is None:
            continue
        y_bottom = (cost_y - 5.0) if isinstance(cost_y, (int, float)) else (y1 - 10.0)
        grid_region = [
            b
            for b in region
            if float(b["bbox"]["y_center"]) >= (colour_y - 10.0) and float(b["bbox"]["y_center"]) <= y_bottom
        ]
        if len(grid_region) < 10:
            continue

        reconstructed = _reconstruct_table_from_boxes(grid_region)
        if reconstructed is None:
            continue

        headers = reconstructed.get("headers") or []
        header_text = " ".join([str(h or "") for h in headers]).upper()
        size_tokens = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}
        hits = sum(1 for t in size_tokens if re.search(r"\b" + re.escape(t) + r"\b", header_text))
        if hits < 2:
            continue

        reconstructed["table_kind"] = "partial_deliveries_grid"
        reconstructed["include_headers_in_rows_matrix"] = True
        _normalize_size_grid_columns(reconstructed)

        # Attach delivery header metadata (printed above the grid) as extra rows so UI can display it.
        try:
            rows0 = reconstructed.get("rows") or []
            headers = reconstructed.get("headers") or []
            if isinstance(rows0, list) and isinstance(headers, list) and headers:
                meta_specs = [
                    ("LOGISTIC ORDER", r"\bLOGISTIC\s+ORDER\b"),
                    ("DELIVERY", r"\bDELIVERY\b"),
                    ("INCOTERM", r"\bINCOTERM\b"),
                    ("FROM", r"\bFROM\b"),
                    ("HANDOVER DATE", r"\bHANDOVER\s+DATE\b"),
                    ("TRANSPORT MODE", r"\bTRANSPORT\s+MODE\b"),
                    ("PRESENTATION TYPE", r"\bPRESENTATION\s+TYPE\b"),
                ]

                # prevent duplicates when rerunning / merging
                existing_keys = set()
                for r in rows0:
                    if isinstance(r, dict):
                        existing_keys.add(str(r.get("COLOUR", "") or "").strip().upper())

                # collect label boxes within header band above the COLOUR header
                header_band = [
                    b
                    for b in region
                    if isinstance(b, dict)
                    and b.get("bbox") is not None
                    and float(b["bbox"]["y_center"]) < (colour_y - 12.0)
                ]

                def _pick_value_below(lbl_box: Dict[str, Any]) -> Optional[str]:
                    try:
                        lx = float(lbl_box.get("bbox", {}).get("x_center") or 0.0)
                        ly = float(lbl_box.get("bbox", {}).get("y_center") or 0.0)
                    except Exception:
                        return None

                    best: Optional[Dict[str, Any]] = None
                    best_score: float = 1e18
                    for bb in header_band:
                        if not isinstance(bb, dict) or bb.get("bbox") is None:
                            continue
                        try:
                            by = float(bb["bbox"]["y_center"])
                            bx = float(bb["bbox"]["x_center"])
                        except Exception:
                            continue
                        if by <= ly:
                            continue
                        # keep the value close vertically and roughly same column
                        dx = abs(bx - lx)
                        dy = by - ly
                        if dx > 140.0:
                            continue
                        if dy > 140.0:
                            continue
                        txt2 = _t(bb.get("text"))
                        if not txt2:
                            continue
                        if re.search(r"\b(LOGISTIC\s+ORDER|DELIVERY|INCOTERM|FROM|HANDOVER\s+DATE|TRANSPORT\s+MODE|PRESENTATION\s+TYPE)\b", txt2, flags=re.IGNORECASE):
                            continue
                        score = dy * 2.0 + dx
                        if best is None or score < best_score:
                            best = bb
                            best_score = score
                    if best is None:
                        return None
                    return _t(best.get("text"))

                pre_rows: List[List[str]] = []

                for key, pat in meta_specs:
                    if key.upper() in existing_keys:
                        continue
                    lbl: Optional[Dict[str, Any]] = None
                    for b in header_band:
                        txt = _t(b.get("text"))
                        if txt and re.search(pat, txt, flags=re.IGNORECASE):
                            lbl = b
                            break
                    if lbl is None:
                        continue
                    val = _pick_value_below(lbl)
                    if not val:
                        continue

                    # Store as pre-rows so it renders BEFORE the header row in rows_matrix
                    row_list = ["" for _ in headers]
                    try:
                        idx_c = next((ii for ii, hh in enumerate(headers) if str(hh).strip().upper() == "COLOUR"), 0)
                    except Exception:
                        idx_c = 0
                    row_list[idx_c] = key
                    # Put value into XS column if exists else first non-colour column
                    idx_v: Optional[int] = None
                    for ii, hh in enumerate(headers):
                        if str(hh).strip().upper() == "XS":
                            idx_v = ii
                            break
                    if idx_v is None:
                        for ii, hh in enumerate(headers):
                            if str(hh).strip().upper() != "COLOUR":
                                idx_v = ii
                                break
                    if idx_v is not None:
                        row_list[int(idx_v)] = val
                    pre_rows.append(row_list)

                if pre_rows:
                    reconstructed["pre_rows_matrix"] = pre_rows
                    reconstructed["header_row_after_pre"] = True
        except Exception:
            pass

        # Attach COST PRICE (printed below the grid) as an extra row so UI can display it.
        try:
            rows0 = reconstructed.get("rows") or []
            if isinstance(rows0, list):
                already_cp = False
                for r in rows0:
                    if not isinstance(r, dict):
                        continue
                    if re.search(r"\bCOST\s*PRICE\b", str(r.get("COLOUR", "") or ""), flags=re.IGNORECASE):
                        already_cp = True
                        break

                if not already_cp:
                    cp_text: Optional[str] = None
                    for b in region:
                        txt = _t(b.get("text"))
                        if not txt:
                            continue
                        if re.search(r"\bCOST\s*PRICE\b", txt, flags=re.IGNORECASE):
                            cp_text = txt
                            break

                    if cp_text:
                        mcp = re.search(
                            r"\bCOST\s*PRICE\b\s*[:\-]?\s*(\d+(?:[\.,]\d{1,2})?)\s*\b(EUR|USD|GBP)\b",
                            cp_text,
                            flags=re.IGNORECASE,
                        )
                        if mcp:
                            amt = (mcp.group(1) or "").replace(",", ".").strip()
                            ccy = (mcp.group(2) or "").upper().strip()
                            cp_val = f"{amt} {ccy}".strip()
                            if cp_val:
                                headers = reconstructed.get("headers") or []
                                cp_row: Dict[str, str] = {h: "" for h in headers} if isinstance(headers, list) else {}
                                if "COLOUR" in cp_row:
                                    cp_row["COLOUR"] = "COST PRICE"
                                elif isinstance(headers, list) and headers:
                                    cp_row[str(headers[0])] = "COST PRICE"

                                if "XS" in cp_row:
                                    cp_row["XS"] = cp_val
                                elif isinstance(headers, list):
                                    for h in headers:
                                        if str(h).strip().upper() != "COLOUR":
                                            cp_row[str(h)] = cp_val
                                            break
                                if cp_row and any((v or "").strip() for v in cp_row.values()):
                                    rows0.append(cp_row)
                                    reconstructed["rows"] = rows0
                                    reconstructed["row_count"] = len(rows0)
        except Exception:
            pass
        out.append(reconstructed)

    return out


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
    pd_tables_found = False
    try:
        grid_tbl = _extract_total_order_grid_from_boxes(boxes)
        if grid_tbl is not None:
            grid_tbl["table_index"] = 900
            grid_tbl["bbox"] = None
            tables.append(grid_tbl)
    except Exception:
        pass
    try:
        pd_tables = _extract_partial_deliveries_grids_from_boxes(boxes)
        if pd_tables:
            pd_tables_found = True
            for j, t in enumerate(pd_tables, start=1):
                t["table_index"] = 910 + j
                t["bbox"] = None
                tables.append(t)
    except Exception:
        pass

    def _infer_table_kind(tbl: Any) -> Optional[str]:
        if not isinstance(tbl, dict):
            return None
        if str(tbl.get("table_kind") or "").strip():
            return str(tbl.get("table_kind") or "").strip()

        try:
            headers = tbl.get("headers") or []
            header_text = " ".join([str(h or "") for h in headers]).upper()
        except Exception:
            header_text = ""

        try:
            rm = tbl.get("rows_matrix") or []
            head = " ".join([str(x or "") for row in rm[:6] if isinstance(row, list) for x in row]).upper() if isinstance(rm, list) else ""
        except Exception:
            head = ""

        blob = (header_text + " " + head).strip()
        if not blob:
            return None

        size_tokens = ["XXS", "XS", "S", "M", "L", "XL", "XXL"]
        size_hits = sum(1 for tok in size_tokens if re.search(r"\b" + re.escape(tok) + r"\b", blob) is not None)

        has_colour = re.search(r"\bCOLOU?R\b", blob, flags=re.IGNORECASE) is not None
        has_total = re.search(r"\bTOTAL\b", blob, flags=re.IGNORECASE) is not None
        has_logistic = re.search(r"\bLOGISTIC\s+ORDER\b", blob, flags=re.IGNORECASE) is not None
        has_delivery = re.search(r"\bDELIVERY\b", blob, flags=re.IGNORECASE) is not None
        has_incoterm = re.search(r"\bINCOTERM\b", blob, flags=re.IGNORECASE) is not None
        has_handover = re.search(r"\bHANDOVER\s+DATE\b", blob, flags=re.IGNORECASE) is not None
        has_transport = re.search(r"\bTRANSPORT\s+MODE\b", blob, flags=re.IGNORECASE) is not None
        has_presentation = re.search(r"\bPRESENTATION\s+TYPE\b", blob, flags=re.IGNORECASE) is not None

        has_article = re.search(r"\bARTICLE\b", blob, flags=re.IGNORECASE) is not None
        has_option = re.search(r"\bOPTION\b", blob, flags=re.IGNORECASE) is not None
        has_cost = re.search(r"\bCOST\b", blob, flags=re.IGNORECASE) is not None or re.search(r"\bCOST\s*PRICE\b", blob, flags=re.IGNORECASE) is not None
        has_qty = re.search(r"\bQ(TY|UANTITY)\b", blob, flags=re.IGNORECASE) is not None
        has_unit_price = re.search(r"\bUNIT\s*PRICE\b", blob, flags=re.IGNORECASE) is not None

        pd_score = 0
        if has_logistic:
            pd_score += 4
        if has_delivery:
            pd_score += 2
        if has_incoterm:
            pd_score += 2
        if has_handover:
            pd_score += 1
        if has_transport:
            pd_score += 1
        if has_presentation:
            pd_score += 1
        if size_hits >= 2:
            pd_score += 2
        if has_colour:
            pd_score += 1

        grid_score = 0
        if size_hits >= 2:
            grid_score += 4
        if has_colour:
            grid_score += 2
        if has_total:
            grid_score += 1

        line_item_score = 0
        if has_article:
            line_item_score += 3
        if has_option:
            line_item_score += 2
        if has_cost or has_unit_price:
            line_item_score += 2
        if has_qty:
            line_item_score += 1

        if pd_score >= 6:
            return "partial_deliveries_grid"
        if grid_score >= 6 and pd_score < 5:
            return "total_order_grid"
        if line_item_score >= 5 and grid_score < 6:
            return "line_item_table"
        return None

    def _is_partial_deliveries_like_table(t: Any) -> bool:
        if not isinstance(t, dict):
            return False
        tk = str(t.get("table_kind") or "").strip().lower()
        if tk in {"partial_deliveries_grid"}:
            return True
        try:
            header_text = " ".join([str(h or "") for h in (t.get("headers") or [])]).upper()
            if "LOGISTIC ORDER" in header_text or "PARTIAL" in header_text:
                return True
        except Exception:
            pass
        try:
            rm = t.get("rows_matrix") or []
            if isinstance(rm, list):
                head = " ".join([" ".join([str(x or "") for x in (r or [])]) if isinstance(r, list) else str(r or "") for r in rm[:3]]).upper()
                if "LOGISTIC ORDER" in head and "DELIVERY" in head:
                    return True
        except Exception:
            pass
        return False

    def _is_image_desc_table(t: Any) -> bool:
        if not isinstance(t, dict):
            return False
        tk = str(t.get("table_kind") or "").strip().lower()
        if tk in {"total_order_grid", "partial_deliveries_grid"}:
            return False
        try:
            headers = t.get("headers") or []
            header_text = " ".join([str(h or "") for h in headers]).upper()
        except Exception:
            header_text = ""

        try:
            rm = t.get("rows_matrix") or []
            if isinstance(rm, list) and rm:
                head = " ".join([str(x or "") for row in rm[:6] if isinstance(row, list) for x in row]).upper()
            else:
                head = ""
        except Exception:
            head = ""

        blob = (header_text + " " + head).strip()
        if not blob:
            return False

        try:
            size_tokens = ["XS", "S", "M", "L", "XL", "XXL", "XXS"]
            hits = sum(1 for tok in size_tokens if re.search(r"\b" + re.escape(tok) + r"\b", blob) is not None)
            has_grid_hint = re.search(r"\bCOLOU?R\b", blob) is not None or re.search(r"\bTOTAL\b", blob) is not None
            # Only treat it as a size grid when it clearly looks like a grid (not e.g. 'EUR/USA S MEX 26')
            if hits >= 2 and has_grid_hint:
                return False
        except Exception:
            pass

        kw = re.search(
            r"\b(IMAGE\s+AND\s+MEASURES|APPLICATION|PLACEMENT|NECKLINE|LEFT_?BACK_?NECKLINE|WASHES\s+SUPPORTED\s+BY\s+THE\s+LABEL)\b",
            blob,
            flags=re.IGNORECASE,
        )
        if kw is not None:
            return True
        return False

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

            try:
                reconstructed = _table_add_rows_matrix(reconstructed)
            except Exception:
                pass

            try:
                inferred_kind = _infer_table_kind(reconstructed)
                if inferred_kind and not str(reconstructed.get("table_kind") or "").strip():
                    reconstructed["table_kind"] = inferred_kind
                    reconstructed["include_headers_in_rows_matrix"] = True
                    if inferred_kind in {"total_order_grid", "partial_deliveries_grid"}:
                        try:
                            _normalize_size_grid_columns(reconstructed)
                        except Exception:
                            pass
            except Exception:
                pass

            # If we already extracted Partial Deliveries grids, skip generic table regions that
            # represent the same section (they tend to merge metadata + multiple grids and break alignment).
            if pd_tables_found and _is_partial_deliveries_like_table(reconstructed):
                continue

            if _is_image_desc_table(reconstructed):
                continue

            reconstructed["table_index"] = i
            reconstructed["bbox"] = {"x": r["x"], "y": r["y"], "w": r["w"], "h": r["h"]}
            tables.append(reconstructed)

    if tables:
        return tables

    reconstructed = _reconstruct_table_from_boxes(boxes)
    if reconstructed is None:
        return []
    try:
        reconstructed = _table_add_rows_matrix(reconstructed)
    except Exception:
        pass
    try:
        inferred_kind = _infer_table_kind(reconstructed)
        if inferred_kind and not str(reconstructed.get("table_kind") or "").strip():
            reconstructed["table_kind"] = inferred_kind
            reconstructed["include_headers_in_rows_matrix"] = True
            if inferred_kind in {"total_order_grid", "partial_deliveries_grid"}:
                try:
                    _normalize_size_grid_columns(reconstructed)
                except Exception:
                    pass
    except Exception:
        pass
    reconstructed["table_index"] = 1
    reconstructed["bbox"] = None
    return [reconstructed]


def _filter_tables_for_sales_order(tables: Any) -> Any:
    if not isinstance(tables, list):
        return tables

    def _looks_like_image_desc(t: Any) -> bool:
        if not isinstance(t, dict):
            return False
        tk = str(t.get("table_kind") or "").strip().lower()
        if tk in {"total_order_grid", "partial_deliveries_grid"}:
            return False
        try:
            headers = t.get("headers") or []
            header_text = " ".join([str(h or "") for h in headers]).upper()
        except Exception:
            header_text = ""

        try:
            rows0 = t.get("rows") or []
            rows_head = ""
            if isinstance(rows0, list) and rows0:
                parts: List[str] = []
                for rr in rows0[:8]:
                    if isinstance(rr, dict):
                        for vv in rr.values():
                            s = str(vv or "").strip()
                            if s:
                                parts.append(s)
                    elif isinstance(rr, list):
                        for vv in rr:
                            s = str(vv or "").strip()
                            if s:
                                parts.append(s)
                    else:
                        s = str(rr or "").strip()
                        if s:
                            parts.append(s)
                rows_head = " ".join(parts).upper()
        except Exception:
            rows_head = ""
        try:
            rm = t.get("rows_matrix") or []
            if isinstance(rm, list) and rm:
                head = " ".join([str(x or "") for row in rm[:8] if isinstance(row, list) for x in row]).upper()
            else:
                head = ""
        except Exception:
            head = ""

        blob = (header_text + " " + rows_head + " " + head).strip()
        if not blob:
            return False

        # Don't remove size grids; be strict to avoid false negatives on strings like 'EUR/USA S MEX 26'
        try:
            size_tokens = ["XS", "S", "M", "L", "XL", "XXL", "XXS"]
            hits = sum(1 for tok in size_tokens if re.search(r"\b" + re.escape(tok) + r"\b", blob) is not None)
            has_grid_hint = re.search(r"\bCOLOU?R\b", blob) is not None or re.search(r"\bTOTAL\b", blob) is not None
            if hits >= 2 and has_grid_hint:
                return False
        except Exception:
            pass

        return (
            re.search(
                r"\b(IMAGE\s+AND\s+MEASURES|APPLICATION|PLACEMENT|NECKLINE|LEFT_?BACK_?NECKLINE|WASHES\s+SUPPORTED\s+BY\s+THE\s+LABEL)\b",
                blob,
                flags=re.IGNORECASE,
            )
            is not None
        )

    return [t for t in tables if not _looks_like_image_desc(t)]


def _build_sales_order_payload(tables: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "header": {},
        "total_order": {"grid": [], "unit_lot": None},
        "partial_delivery_headers": [],
        "partial_delivery_lines": [],
    }
    if not isinstance(tables, list):
        return payload

    def _levenshtein(a: str, b: str, max_dist: int = 4) -> int:
        aa = str(a or "")
        bb = str(b or "")
        if aa == bb:
            return 0
        if not aa:
            return len(bb)
        if not bb:
            return len(aa)
        if abs(len(aa) - len(bb)) > max_dist:
            return max_dist + 1
        prev = list(range(len(bb) + 1))
        for i, ca in enumerate(aa, start=1):
            cur = [i]
            row_best = max_dist + 1
            for j, cb in enumerate(bb, start=1):
                ins = cur[j - 1] + 1
                dele = prev[j] + 1
                sub = prev[j - 1] + (0 if ca == cb else 1)
                v = ins if ins < dele else dele
                if sub < v:
                    v = sub
                cur.append(v)
                if v < row_best:
                    row_best = v
            if row_best > max_dist:
                return max_dist + 1
            prev = cur
        return prev[-1]

    _HEADER_CANON_KEYS = {
        "ordernr",
        "date",
        "season",
        "buyer",
        "purchaser",
        "supplier",
        "sendto",
        "paymentterms",
        "supplierref",
        "article",
        "description",
        "marketoforigin",
        "pvp",
        "compositionsinformation",
        "careinstructions",
        "hangtaglabel",
        "mainlabel",
        "externalfabric",
        "hanging",
        "totalorder",
    }

    _HEADER_LABEL_TO_CANON = {
        "ordernr": "ordernr",
        "order_nr": "ordernr",
        "orderno": "ordernr",
        "order": "ordernr",
        "orderid": "ordernr",
        "ordernumber": "ordernr",
        "no": "ordernr",
        "noso": "ordernr",
        "nosalesorder": "ordernr",
        "salesorder": "ordernr",
        "salesorderno": "ordernr",
        "salesordernumber": "ordernr",
        "pono": "ordernr",
        "ponumber": "ordernr",
        "purchaseorderno": "ordernr",
        "purchaseordernumber": "ordernr",
        "po": "ordernr",
        "so": "ordernr",
        "date": "date",
        "orderdate": "date",
        "dateoforder": "date",
        "issuedate": "date",
        "issue_date": "date",
        "documentdate": "date",
        "docdate": "date",
        "season": "season",
        "seasoncode": "season",
        "seasonname": "season",
        "buyer": "buyer",
        "customer": "buyer",
        "cust": "buyer",
        "soldto": "buyer",
        "sold_to": "buyer",
        "billto": "buyer",
        "bill_to": "buyer",
        "purchaser": "purchaser",
        "purchace": "purchaser",
        "purchasername": "purchaser",
        "purchasing": "purchaser",
        "purchase": "purchaser",
        "supplier": "supplier",
        "vendor": "supplier",
        "seller": "supplier",
        "factory": "supplier",
        "sendto": "sendto",
        "send_to": "sendto",
        "send": "sendto",
        "shipto": "sendto",
        "ship_to": "sendto",
        "shiptoaddress": "sendto",
        "shippingaddress": "sendto",
        "deliveryaddress": "sendto",
        "deliver_to": "sendto",
        "deliverto": "sendto",
        "paymentterms": "paymentterms",
        "payment_terms": "paymentterms",
        "termsofpayment": "paymentterms",
        "terms": "paymentterms",
        "paymentterm": "paymentterms",
        "payment_term": "paymentterms",
        "payterms": "paymentterms",
        "supplierref": "supplierref",
        "supplier_ref": "supplierref",
        "vendorref": "supplierref",
        "vendor_ref": "supplierref",
        "reference": "supplierref",
        "ref": "supplierref",
        "article": "article",
        "style": "article",
        "styleno": "article",
        "style_no": "article",
        "item": "article",
        "itemno": "article",
        "item_no": "article",
        "description": "description",
        "desc": "description",
        "descripton": "description",
        "descriplion": "description",
        "marketoforigin": "marketoforigin",
        "market_origin": "marketoforigin",
        "origin": "marketoforigin",
        "countryoforigin": "marketoforigin",
        "pvp": "pvp",
        "compositionsinformation": "compositionsinformation",
        "compositioninformation": "compositionsinformation",
        "composition": "compositionsinformation",
        "careinstructions": "careinstructions",
        "care": "careinstructions",
        "hangtaglabel": "hangtaglabel",
        "mainlabel": "mainlabel",
        "externalfabric": "externalfabric",
        "hanging": "hanging",
        "totalorder": "totalorder",
        "total": "totalorder",
    }

    def _canon_header_key_fuzzy(label: str) -> str:
        nk = _norm_key(label)
        if not nk:
            return ""
        direct = _HEADER_LABEL_TO_CANON.get(nk)
        if direct:
            return direct
        if nk in _HEADER_CANON_KEYS:
            return nk
        # Fuzzy-match only against known label tokens to reduce false positives.
        best_key = ""
        best_d = 5
        for cand in _HEADER_LABEL_TO_CANON.keys():
            d = _levenshtein(nk, cand, max_dist=4)
            if d < best_d:
                best_d = d
                best_key = cand
                if best_d <= 1:
                    break
        if best_key and best_d <= 2:
            return _HEADER_LABEL_TO_CANON.get(best_key, "")
        return ""

    def _looks_like_care_noise(s: str) -> bool:
        try:
            ss = str(s or "")
            if not ss.strip():
                return False
            label_hits = 0
            for tok in [
                "HANGTAG",
                "MAIN LABEL",
                "EXTERNAL FABRIC",
                "HANGING",
                "TOTAL ORDER",
                "COMPOSITION",
            ]:
                if re.search(r"\b" + re.escape(tok) + r"\b", ss, flags=re.IGNORECASE):
                    label_hits += 1
            has_care_verbs = re.search(r"\b(WASH|BLEACH|IRON|DRY|TUMBLE|DRY\s*CLEAN)\b", ss, flags=re.IGNORECASE) is not None
            has_pipes = ss.count("|") >= 2
            has_temp = re.search(r"\b\d{1,3}\s*(?:°\s*)?(?:C|F)\b", ss, flags=re.IGNORECASE) is not None
            return (label_hits >= 2 and (not (has_care_verbs or has_pipes or has_temp))) or (label_hits >= 3 and not has_care_verbs)
        except Exception:
            return False

    def _looks_like_care_value(s: str) -> bool:
        try:
            ss = str(s or "")
            if not ss.strip():
                return False
            if re.search(r"\b(WASH|BLEACH|IRON|DRY|TUMBLE|DRY\s*CLEAN|DO\s*NOT)\b", ss, flags=re.IGNORECASE):
                return True
            if ss.count("|") >= 2:
                return True
            if re.search(r"\b\d{1,3}\s*(?:°\s*)?(?:C|F)\b", ss, flags=re.IGNORECASE) is not None:
                return True
            return False
        except Exception:
            return False

    def _maybe_set_header(ck: str, v: str) -> None:
        if not ck:
            return
        if ck not in _HEADER_CANON_KEYS:
            return
        vv = str(v or "").strip()
        if ck == "careinstructions":
            existing = str(payload["header"].get("careinstructions") or "").strip()
            if existing and _looks_like_care_value(existing):
                return
            if _looks_like_care_noise(vv):
                return
        payload["header"][ck] = vv

    def _infer_colour_from_row(row: Dict[str, Any]) -> str:
        try:
            vals: List[str] = []
            for v in (row or {}).values():
                s = str(v or "").strip()
                if s:
                    vals.append(s)
            if not vals:
                return ""

            # Join all cells so we can detect colour even if OCR split it across columns
            blob = " ".join(vals)
            blob = re.sub(r"\s+", " ", blob).strip()
            if not blob:
                return ""

            # Don't infer colour from UNIT LOT lines
            if re.search(r"\bUNIT\s*LOT\b", blob, flags=re.IGNORECASE):
                return ""

            # Match: 800 - BLACK (support different dash characters)
            m = re.search(
                r"\b(\d{2,6})\s*[-–—]\s*([A-Z][A-Z0-9\s/]+?)\b",
                blob,
                flags=re.IGNORECASE,
            )
            if not m:
                return ""
            code = (m.group(1) or "").strip()
            name = (m.group(2) or "").strip()
            if not (code and name):
                return ""
            return f"{code} - {name}"
        except Exception:
            return ""

    # 1) Header fields from best AI table (if present)
    try:
        ai_tbl: Optional[Dict[str, Any]] = None
        for t in tables:
            if isinstance(t, dict) and t.get("headers") == ["key", "value"]:
                ai_tbl = t
                break
        if ai_tbl is not None:
            kv_all = ai_tbl.get("kv_pairs_all")
            if isinstance(kv_all, list):
                for p in kv_all:
                    if not isinstance(p, dict):
                        continue
                    k = str(p.get("key") or "").strip()
                    v = str(p.get("value") or "").strip()
                    if k:
                        ck = _canon_header_key_fuzzy(k) or _norm_key(k)
                        if ck:
                            _maybe_set_header(ck, v)
            else:
                # fallback to rows_matrix
                rm = ai_tbl.get("rows_matrix")
                if isinstance(rm, list):
                    for r in rm:
                        if not (isinstance(r, list) and len(r) >= 2):
                            continue
                        k = str(r[0] or "").strip()
                        v = str(r[1] or "").strip()
                        if k and k.lower() != "key":
                            ck = _canon_header_key_fuzzy(k) or _norm_key(k)
                            if ck:
                                _maybe_set_header(ck, v)
    except Exception:
        pass

    # 1b) Fallback: extract header-like key/value pairs from any table rows_matrix
    try:
        def _row_blob(row: Any) -> str:
            if not isinstance(row, list):
                return ""
            return " ".join([str(x or "").strip() for x in row if str(x or "").strip()]).strip()

        def _cell_str(x: Any) -> str:
            return str(x or "").strip()

        def _is_probably_header_row(row: List[Any]) -> bool:
            try:
                # Header rows are usually short-ish and mostly text.
                nonempty = [c for c in [_cell_str(x) for x in row] if c]
                if len(nonempty) < 2:
                    return False
                digit_cells = sum(1 for c in nonempty if re.search(r"\d", c) is not None)
                # Allow some digits (e.g., buyer code), but not mostly numeric.
                return digit_cells <= max(1, int(len(nonempty) * 0.5))
            except Exception:
                return False

        for t in tables:
            if not isinstance(t, dict):
                continue
            rm = t.get("rows_matrix")
            if not isinstance(rm, list) or not rm:
                continue

            rows = [r for r in rm if isinstance(r, list) and any(str(x or "").strip() for x in r)]
            if not rows:
                continue

            for r in rows:
                if len(r) < 2:
                    continue
                k0 = str(r[0] or "").strip()
                v0 = str(r[1] or "").strip()
                if not k0 or not v0:
                    continue
                ck0 = _canon_header_key_fuzzy(k0) or ""
                if ck0:
                    _maybe_set_header(ck0, v0)

            for i, r in enumerate(rows):
                blob = _row_blob(r)
                if not blob:
                    continue
                ck = _canon_header_key_fuzzy(blob) or ""
                if not ck:
                    continue
                if i + 1 >= len(rows):
                    continue
                nxt = _row_blob(rows[i + 1])
                if not nxt:
                    continue
                if _canon_header_key_fuzzy(nxt):
                    continue
                _maybe_set_header(ck, nxt)

            # Horizontal header/value tables:
            # Example:
            #   ["ORDER NR", "DATE", "SUPPLIER"]
            #   ["55876-D", "23/07/2025", "D&J ..."]
            try:
                for i, r in enumerate(rows[:-1]):
                    if not isinstance(r, list) or len(r) < 2:
                        continue
                    if not _is_probably_header_row(r):
                        continue

                    # Build column index -> canon key map
                    col_map: Dict[int, str] = {}
                    for ci, cell in enumerate(r):
                        s = _cell_str(cell)
                        if not s:
                            continue
                        ck = _canon_header_key_fuzzy(s) or ""
                        if ck:
                            col_map[int(ci)] = ck

                    # Need at least 2 recognized header keys to be confident
                    uniq = set(col_map.values())
                    if len(uniq) < 2:
                        continue

                    vrow = rows[i + 1]
                    if not isinstance(vrow, list) or not any(_cell_str(x) for x in vrow):
                        continue
                    # Avoid pairing header->value if the next row also looks like headers
                    header_like_next = 0
                    for ci, ck in col_map.items():
                        if ci < len(vrow) and _canon_header_key_fuzzy(_cell_str(vrow[ci]) or ""):
                            header_like_next += 1
                    if header_like_next >= 2:
                        continue

                    for ci, ck in col_map.items():
                        if ci >= len(vrow):
                            continue
                        vv = _cell_str(vrow[ci])
                        if not vv:
                            continue
                        # Don't overwrite good existing values.
                        existing = str(payload["header"].get(ck) or "").strip()
                        if existing:
                            if ck == "careinstructions" and _looks_like_care_value(existing):
                                continue
                            if len(existing) >= len(vv):
                                continue
                        _maybe_set_header(ck, vv)
            except Exception:
                pass
    except Exception:
        pass

    # 2) TOTAL ORDER grid
    try:
        for t in tables:
            if not isinstance(t, dict):
                continue
            if str(t.get("table_kind") or "").strip().lower() != "total_order_grid":
                continue
            rows = t.get("rows") or []
            if isinstance(rows, list):
                grid_out: List[Dict[str, Any]] = []
                unit_lot: Optional[str] = None

                def _get_ci(d: Dict[str, Any], key: str) -> Any:
                    try:
                        if key in d:
                            return d.get(key)
                        lk = str(key or "").strip().lower()
                        if not lk:
                            return None
                        for kk, vv in d.items():
                            if str(kk or "").strip().lower() == lk:
                                return vv
                    except Exception:
                        return None
                    return None

                def _get_any_ci(d: Dict[str, Any], keys: List[str]) -> Any:
                    for k in keys:
                        v = _get_ci(d, k)
                        if v is not None and str(v).strip() != "":
                            return v
                    return None

                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    colour = str(_get_any_ci(r, ["COLOUR", "colour"]) or "").strip()

                    # UNIT LOT row should be handled before any colour inference/override
                    if re.search(r"\bUNIT\s*LOT\b", colour or "", flags=re.IGNORECASE):
                        for k in ["XS", "S", "M", "L", "XL", "Total"]:
                            v = str(_get_any_ci(r, [k, k.lower()]) or "").strip()
                            if v:
                                unit_lot = v
                                break
                        continue

                    # Override if empty or looks like noise and not already a valid colour/TOTAL
                    if (
                        not colour
                        or (
                            re.search(r"\bTOTAL\b", colour, flags=re.IGNORECASE) is None
                            and re.search(r"\b\d{2,6}\s*[-–—]\s*[A-Z]", colour, flags=re.IGNORECASE) is None
                        )
                    ):
                        inferred = _infer_colour_from_row(r)
                        if inferred:
                            colour = inferred
                    if re.search(r"\bUNIT\s*LOT\b", colour, flags=re.IGNORECASE):
                        for k in ["XS", "S", "M", "L", "XL", "Total"]:
                            v = str(_get_any_ci(r, [k, k.lower()]) or "").strip()
                            if v:
                                unit_lot = v
                                break
                        continue

                    xs_v = str(_get_any_ci(r, ["XS", "xs"]) or "").strip()
                    s_v = str(_get_any_ci(r, ["S", "s"]) or "").strip()
                    m_v = str(_get_any_ci(r, ["M", "m"]) or "").strip()
                    l_v = str(_get_any_ci(r, ["L", "l"]) or "").strip()
                    xl_v = str(_get_any_ci(r, ["XL", "xl"]) or "").strip()
                    tot_v = str(_get_any_ci(r, ["Total", "TOTAL", "total"]) or "").strip()

                    # Drop noise rows (commonly from UNIT LOT value splitting into COLOUR column)
                    if re.fullmatch(r"\d{1,6}", colour or "") and not any([xs_v, s_v, m_v, l_v, xl_v, tot_v]):
                        continue

                    # Skip TOTAL summary row
                    if re.fullmatch(r"TOTAL", (colour or "").strip(), flags=re.IGNORECASE):
                        continue
                    grid_out.append(
                        {
                            "colour": colour,
                            "xs": xs_v,
                            "s": s_v,
                            "m": m_v,
                            "l": l_v,
                            "xl": xl_v,
                            "total": tot_v,
                        }
                    )
                payload["total_order"]["grid"] = grid_out
                payload["total_order"]["unit_lot"] = unit_lot
            break
    except Exception:
        pass

    # 2b) LINE ITEM table (Article/Option/Cost) -> map into existing payload fields (no schema changes)
    # Use only as fallback to fill missing values (especially cost_price) when no explicit metadata provides it.
    try:
        def _pick_col_idx(headers: List[str], pats: List[str]) -> Optional[int]:
            for i, h in enumerate(headers):
                hh = str(h or "")
                for pat in pats:
                    if re.search(pat, hh, flags=re.IGNORECASE):
                        return i
            return None

        line_item_tables = [t for t in tables if isinstance(t, dict) and str(t.get("table_kind") or "").strip().lower() == "line_item_table"]
        for t in line_item_tables:
            rm = t.get("rows_matrix")
            if not (isinstance(rm, list) and rm):
                continue
            # Determine header row
            header_row: Optional[List[str]] = None
            for rr in rm[:3]:
                if isinstance(rr, list) and len(rr) >= 3:
                    blob = " ".join([str(x or "") for x in rr])
                    if re.search(r"\bARTICLE\b", blob, flags=re.IGNORECASE) or re.search(r"\bOPTION\b", blob, flags=re.IGNORECASE):
                        header_row = [str(x or "").strip() for x in rr]
                        break
            if header_row is None and isinstance(rm[0], list):
                header_row = [str(x or "").strip() for x in rm[0]]
            if header_row is None:
                continue

            idx_cost = _pick_col_idx(header_row, [r"\bCOST\b", r"\bCOST\s*PRICE\b", r"\bUNIT\s*PRICE\b", r"\bPRICE\b"])
            idx_article = _pick_col_idx(header_row, [r"\bARTICLE\b", r"\bART\b", r"\bSTYLE\b"])
            idx_desc = _pick_col_idx(header_row, [r"\bDESCRIPTION\b", r"\bDESC\b"])

            costs: List[str] = []
            for rr in rm[1:]:
                if not isinstance(rr, list):
                    continue
                if idx_cost is not None and int(idx_cost) < len(rr):
                    c = str(rr[int(idx_cost)] or "").strip()
                    if c:
                        costs.append(c)

            cost_val = ""
            if costs:
                # keep unique in order
                seen = set()
                uniq = []
                for c in costs:
                    if c not in seen:
                        seen.add(c)
                        uniq.append(c)
                cost_val = ", ".join(uniq[:3]).strip()

            # Fill header fields if missing
            if _field_bad("article", payload["header"].get("article")) and idx_article is not None:
                for rr in rm[1:]:
                    if isinstance(rr, list) and int(idx_article) < len(rr):
                        v = str(rr[int(idx_article)] or "").strip()
                        if v:
                            payload["header"]["article"] = v
                            break
            if _field_bad("description", payload["header"].get("description")) and idx_desc is not None:
                for rr in rm[1:]:
                    if isinstance(rr, list) and int(idx_desc) < len(rr):
                        v = str(rr[int(idx_desc)] or "").strip()
                        if v:
                            payload["header"]["description"] = v
                            break

            # Fill cost_price into the first partial_delivery_headers meta if present, else defer (will be set when meta is created)
            if cost_val:
                if payload["partial_delivery_headers"]:
                    if (payload["partial_delivery_headers"][0] or {}).get("cost_price") in (None, ""):
                        payload["partial_delivery_headers"][0]["cost_price"] = cost_val
                else:
                    # stash into header for later pickup by partial delivery loop
                    payload.setdefault("_tmp_cost_price", cost_val)
    except Exception:
        pass

    # 3) Partial Deliveries
    try:
        delivery_seq = 0
        for t in tables:
            if not isinstance(t, dict):
                continue
            if str(t.get("table_kind") or "").strip().lower() != "partial_deliveries_grid":
                continue

            delivery_seq += 1

            meta: Dict[str, Any] = {
                "delivery_seq": delivery_seq,
                "logistic_order": None,
                "delivery": None,
                "incoterm": None,
                "from": None,
                "handover_date": None,
                "transport_mode": None,
                "presentation_type": None,
                "cost_price": None,
            }

            try:
                if meta.get("cost_price") in (None, "") and payload.get("_tmp_cost_price"):
                    meta["cost_price"] = payload.get("_tmp_cost_price")
            except Exception:
                pass

            def _parse_logistic_order(blob: str) -> Optional[str]:
                try:
                    b = str(blob or "").strip()
                    if not b:
                        return None
                    b = re.sub(r"\s+", " ", b)
                    if re.search(r"\bLOGISTIC\s*ORDER\b", b, flags=re.IGNORECASE) is None:
                        return None
                    # Typical: "LOGISTIC ORDER 55876-D / 1" or "LOGISTIC ORDER: 55876-D/1"
                    m = re.search(
                        r"\bLOGISTIC\s*ORDER\b\s*[:\-]?\s*([A-Z0-9\-]+\s*/\s*\d{1,3})\b",
                        b,
                        flags=re.IGNORECASE,
                    )
                    if m:
                        return re.sub(r"\s+", " ", str(m.group(1) or "").strip())
                    # Fallback: capture tail after label
                    m2 = re.search(r"\bLOGISTIC\s*ORDER\b\s*[:\-]?\s*(.+)$", b, flags=re.IGNORECASE)
                    if m2:
                        tail = str(m2.group(1) or "").strip()
                        tail = re.sub(r"\s+", " ", tail)
                        if tail:
                            return tail
                    return None
                except Exception:
                    return None

            def _consume_meta_row(label: str, val: str) -> None:
                lk0 = _norm_key(label)
                lk = lk0
                # Alias common OCR label normalizations to payload field keys
                if lk0 in {"logisticorder", "logisticsorder"}:
                    lk = "logistic_order"
                elif lk0 in {"handoverdate", "handoverdate:"}:
                    lk = "handover_date"
                elif lk0 in {"transportmode", "transportationmode"}:
                    lk = "transport_mode"
                elif lk0 in {"presentationtype"}:
                    lk = "presentation_type"
                else:
                    # Fuzzy-match common typos for partial meta labels
                    try:
                        partial_label_map = {
                            "logisticorder": "logistic_order",
                            "logisticsorder": "logistic_order",
                            "delivery": "delivery",
                            "incoterm": "incoterm",
                            "from": "from",
                            "handoverdate": "handover_date",
                            "transportmode": "transport_mode",
                            "presentationtype": "presentation_type",
                        }
                        if lk0 and lk0 not in partial_label_map:
                            best_key = ""
                            best_d = 5
                            for cand in partial_label_map.keys():
                                d = _levenshtein(lk0, cand, max_dist=4)
                                if d < best_d:
                                    best_d = d
                                    best_key = cand
                                    if best_d <= 1:
                                        break
                            if best_key and best_d <= 2:
                                lk = partial_label_map.get(best_key, lk)
                    except Exception:
                        pass
                if lk in {"logistic_order", "delivery", "incoterm", "from", "handover_date", "transport_mode", "presentation_type"}:
                    meta[lk] = val

            # Metadata rows may live in pre_rows_matrix (preferred)
            pre = t.get("pre_rows_matrix")
            if isinstance(pre, list):
                for r in pre:
                    if not (isinstance(r, list) and len(r) >= 2):
                        continue
                    label = str(r[0] or "").strip()
                    val = str(r[1] or "").strip()
                    if label and val:
                        _consume_meta_row(label, val)
                    # Some OCR outputs merge label+value into the label cell
                    if meta.get("logistic_order") in (None, ""):
                        lo = _parse_logistic_order(label)
                        if lo:
                            meta["logistic_order"] = lo

            # Also scan rows dicts for metadata + cost price
            rows = t.get("rows") or []
            if isinstance(rows, list):
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    c0 = str(r.get("COLOUR") or "").strip()

                    # Override if empty or looks like noise and not already a valid colour/TOTAL
                    if (
                        not c0
                        or (
                            re.search(r"\bTOTAL\b", c0, flags=re.IGNORECASE) is None
                            and re.search(r"\b\d{2,6}\s*[-–—]\s*[A-Z]", c0, flags=re.IGNORECASE) is None
                        )
                    ):
                        inferred = _infer_colour_from_row(r)
                        if inferred:
                            c0 = inferred

                    v0 = str(r.get("XS") or "").strip()
                    s0 = str(r.get("S") or "").strip()
                    m0 = str(r.get("M") or "").strip()
                    l0 = str(r.get("L") or "").strip()
                    xl0 = str(r.get("XL") or "").strip()
                    t0 = str(r.get("Total") or r.get("TOTAL") or "").strip()

                    # Drop noise rows (e.g. "1" row with empty quantities)
                    if re.fullmatch(r"\d{1,6}", c0 or "") and not any([v0, s0, m0, l0, xl0, t0]):
                        continue
                    if c0 and v0:
                        _consume_meta_row(c0, v0)

                    # Fallback: parse LOGISTIC ORDER from entire row blob (value might not be in XS)
                    if meta.get("logistic_order") in (None, ""):
                        try:
                            blob = " ".join([str(v or "").strip() for v in r.values() if str(v or "").strip()])
                            blob = re.sub(r"\s+", " ", blob).strip()
                            lo = _parse_logistic_order(blob)
                            if lo:
                                meta["logistic_order"] = lo
                        except Exception:
                            pass
                    if re.search(r"\bCOST\s*PRICE\b", c0, flags=re.IGNORECASE):
                        meta["cost_price"] = v0 or meta.get("cost_price")
                        continue

                    # Skip TOTAL summary row in payload lines
                    if re.fullmatch(r"TOTAL", (c0 or "").strip(), flags=re.IGNORECASE):
                        continue
                    # Only keep real grid rows
                    if c0 and re.search(r"\b(LOGISTIC\s+ORDER|DELIVERY|INCOTERM|FROM|HANDOVER\s+DATE|TRANSPORT\s+MODE|PRESENTATION\s+TYPE)\b", c0, flags=re.IGNORECASE):
                        continue

                    payload["partial_delivery_lines"].append(
                        {
                            "delivery_seq": delivery_seq,
                            "colour": c0,
                            "xs": v0,
                            "s": s0,
                            "m": m0,
                            "l": l0,
                            "xl": xl0,
                            "total": t0,
                        }
                    )

            payload["partial_delivery_headers"].append(meta)
    except Exception:
        pass

    try:
        if "_tmp_cost_price" in payload:
            del payload["_tmp_cost_price"]
    except Exception:
        pass

    return payload


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
        images: List[np.ndarray] = []
        # Primary: pdf2image (requires Poppler)
        if convert_from_bytes is not None:
            try:
                pages = convert_from_bytes(file_bytes, dpi=250)
                for page in pages:
                    rgb = np.array(page.convert("RGB"))
                    bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
                    images.append(bgr)
                if images:
                    return images
            except Exception:
                images = []

        # Fallback: PyMuPDF (no Poppler)
        if fitz is None:
            raise RuntimeError("Unable to rasterize PDF. Install poppler (for pdf2image) or PyMuPDF.")

        try:
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            # 250dpi ~= 250/72 zoom
            zoom = 250.0 / 72.0
            mat = fitz.Matrix(zoom, zoom)
            for i in range(len(doc)):
                page = doc.load_page(i)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                img = np.frombuffer(pix.samples, dtype=np.uint8)
                img = img.reshape((pix.height, pix.width, pix.n))
                # pix.n should be 3 (RGB)
                if img.ndim == 3 and img.shape[2] >= 3:
                    rgb = img[:, :, :3]
                else:
                    rgb = cv2.cvtColor(img, cv2.COLOR_GRAY2RGB)
                bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
                images.append(bgr)
        except Exception as e:
            raise RuntimeError(f"Unable to rasterize PDF via PyMuPDF: {e}")

        if not images:
            raise RuntimeError("Unable to rasterize PDF pages")
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
        g = m.group(m.lastindex) if m.lastindex else m.group(0)
        g = (g or "").strip()
        return g or None

    fields: Dict[str, Any] = {}
    fields["order_no"] = _first(r"\b(order\s*(?:nr|no|number)|no\.?\s*order|nomor\s*order|no\.?\s*so|sales\s*order\s*(?:no|number)|nomor\s*so)\b\s*[:\-]?\s*([A-Z0-9\-\/]+)")
    if fields.get("order_no") and isinstance(fields.get("order_no"), str):
        m = re.search(r"([A-Z0-9\-\/]+)", fields["order_no"], flags=re.IGNORECASE)
        fields["order_no"] = (m.group(1) if m else fields["order_no"]).strip()

    fields["date"] = _first(r"\b(date|tanggal|tgl)\b\s*[:\-]?\s*([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})")
    if fields.get("date") and isinstance(fields.get("date"), str):
        m = re.search(r"([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})", fields["date"])
        fields["date"] = (m.group(1) if m else fields["date"]).strip()

    fields["supplier"] = _first(r"\b(supplier|vendor|pemasok)\b\s*[:\-]?\s*(.+)")
    fields["season"] = _first(r"\bseason\b\s*[:\-]?\s*([A-Z0-9\s]+)")
    fields["buyer"] = _first(r"\b(buyer|pembeli)\b\s*[:\-]?\s*([A-Z0-9\s]+)")
    if fields.get("buyer") and isinstance(fields.get("buyer"), str):
        fields["buyer"] = fields["buyer"].strip()

    fields["payment_terms"] = _first(r"\b(payment\s*terms|terms\s*of\s*payment|syarat\s*pembayaran)\b\s*[:\-]?\s*(.+)")
    fields["purchaser"] = _first(r"\b(purchaser|pemesan)\b\s*[:\-]?\s*(.+)")

    fields["send_to"] = _first(r"\b(send\s*to|kirim\s*ke|ship\s*to)\b\s*[:\-]?\s*(.+)")
    fields["tax_office_number"] = _first(r"\btax\s*office\s*number\b\s*[:\-]?\s*([0-9]{8,})")

    # Composition header (garment docs)
    # Example: "COMPOSITION LABEL / PART COLORS 800-BLACK"
    fields["composition_label_part_colors"] = _first(
        r"\bcomposition\s*label\s*/\s*part\s*colou?rs\b\s*[:\-]?\s*([^\n]+)"
    )
    # If OCR splits label and value across lines, try to capture next line
    if not fields.get("composition_label_part_colors") and isinstance(text, str) and text.strip():
        lines = [ln.strip() for ln in text.replace("\r", "\n").split("\n") if ln.strip()]
        for i, ln in enumerate(lines):
            if re.search(r"\bcomposition\s*label\b", ln, flags=re.IGNORECASE) and re.search(
                r"\bpart\s*colou?rs\b", ln, flags=re.IGNORECASE
            ):
                # Try to read value from same line tail first
                m = re.search(
                    r"\bpart\s*colou?rs\b\s*[:\-]?\s*([A-Z0-9\-\s]+)",
                    ln,
                    flags=re.IGNORECASE,
                )
                if m and (m.group(1) or "").strip():
                    fields["composition_label_part_colors"] = (m.group(1) or "").strip()
                    break
                # Otherwise take next line as value (e.g., "800-BLACK")
                if i + 1 < len(lines):
                    nxt = lines[i + 1]
                    # accept formats like "800-BLACK" or "800 - BLACK"
                    m2 = re.search(r"\b(\d{2,5}\s*[-/]\s*[A-Z]{2,})\b", nxt.replace(" ", ""), flags=re.IGNORECASE)
                    if m2:
                        fields["composition_label_part_colors"] = m2.group(1).replace(" ", "").strip()
                    else:
                        fields["composition_label_part_colors"] = nxt.strip()
                    break

    cleaned = {k: v for k, v in fields.items() if v is not None}
    # Normalize composition value to a short token if possible (e.g. 800-BLACK)
    if isinstance(cleaned.get("composition_label_part_colors"), str):
        cc = cleaned.get("composition_label_part_colors") or ""
        mcc = re.search(r"\b\d{2,5}\s*[-/]\s*[A-Z]{2,}\b", cc, flags=re.IGNORECASE)
        if mcc:
            cleaned["composition_label_part_colors"] = mcc.group(0).replace(" ", "").strip()
    # Backward-compatible alias: some clients expect order_nr
    if "order_no" in cleaned and "order_nr" not in cleaned:
        cleaned["order_nr"] = cleaned.get("order_no")
    elif "order_nr" in cleaned and "order_no" not in cleaned:
        cleaned["order_no"] = cleaned.get("order_nr")
    return cleaned


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _extract_fields_from_tables(tables: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not tables:
        return out

    # Look for header->value row mappings in any detected table
    key_aliases = {
        "order_no": {"ordernr", "ordernumber", "order", "order-no", "order_nr"},
        "date": {"date", "orderdate"},
        "supplier": {"supplier", "vendor"},
        "season": {"season"},
        "buyer": {"buyer"},
        "payment_terms": {"paymentterms", "payment"},
        "purchaser": {"purchaser"},
    }

    for tbl in tables:
        rows = tbl.get("rows") or []
        if not isinstance(rows, list):
            continue
        for r in rows:
            if not isinstance(r, dict):
                continue
            for hk, hv in r.items():
                nk = _norm_key(str(hk))
                val = (str(hv) if hv is not None else "").strip()
                if not val:
                    continue
                for field, aliases in key_aliases.items():
                    if field in out:
                        continue
                    if nk in {_norm_key(a) for a in aliases}:
                        out[field] = val
                        break

    return out


def _extract_fields_smart(text: str, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer table-derived values when available (usually cleaner)
    t_fields = _extract_fields_from_text(text)

    def _looks_like_size_grid_table(t: Any) -> bool:
        if not isinstance(t, dict):
            return False
        tk = str(t.get("table_kind") or "").strip().lower()
        if tk in {"total_order_grid", "partial_deliveries_grid"}:
            return True
        headers = t.get("headers") or []
        if isinstance(headers, list):
            ht = " ".join([str(h or "") for h in headers]).upper()
            size_tokens = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}
            hits = sum(1 for tok in size_tokens if re.search(r"\b" + re.escape(tok) + r"\b", ht))
            if hits >= 2 and re.search(r"\bCOLOU?R\b", ht):
                return True
        rm = t.get("rows_matrix")
        if isinstance(rm, list) and rm:
            try:
                joined = " ".join([str(x or "") for row in rm[:3] if isinstance(row, list) for x in row]).upper()
                size_tokens = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}
                hits = sum(1 for tok in size_tokens if re.search(r"\b" + re.escape(tok) + r"\b", joined))
                if hits >= 2 and re.search(r"\bCOLOU?R\b", joined):
                    return True
            except Exception:
                pass
        return False

    tables_for_fields = [t for t in (tables or []) if not _looks_like_size_grid_table(t)]
    tbl_fields = _extract_fields_from_tables(tables_for_fields)

    merged = {**t_fields, **{k: v for k, v in tbl_fields.items() if v is not None}}

    def _field_bad(field: str, val: Any) -> bool:
        s = (str(val) if val is not None else "").strip()
        if not s:
            return True

        if field in {"care_instructions"}:
            # Reject gibberish OCR output (e.g., long runs of O/0/N) when it doesn't contain care keywords.
            s2 = re.sub(r"\s+", " ", s).strip()
            has_kw = re.search(
                r"\b(HAND\s*WASH|WASH(?!ED)|BLEACH|IRON|DRY\s*CLEAN|TUMBLE\s*DRY)\b",
                s2,
                flags=re.IGNORECASE,
            ) is not None
            compact = re.sub(r"[^A-Z0-9]", "", s2.upper())
            if not has_kw and re.search(r"(O|0|N){6,}", compact) is not None:
                return True
            letters = re.findall(r"[A-Z]", s2.upper())
            if letters:
                bad_letters = sum(1 for ch in letters if ch in {"O", "N"})
                bad_ratio = bad_letters / max(1, len(letters))
                if not has_kw and len(s2) >= 25 and bad_ratio >= 0.45:
                    return True
        # Frequently swapped: a label token is used as a value
        if _norm_key(s) in {
            "date",
            "ordernr",
            "order",
            "supplier",
            "season",
            "buyer",
            "paymentterms",
            "purchaser",
            "sendto",
            "shipto",
            "taxofficenumber",
            "description",
            "article",
            "marketoforigin",
            "pvp",
        }:
            return True
        if re.search(r"\b(TOTAL|DRIGIN|MARKETOF)\b", s, flags=re.IGNORECASE):
            return True
        if field in {"supplier_ref"}:
            if re.search(r"\b(OUTER\s+SHELL|LINING|COMPOSITION|COMPOSITIONS|CARE|INSTRUCTIONS|TOTAL)\b", s, flags=re.IGNORECASE):
                return True
            # Prevent capturing price list as supplier ref
            if re.search(r"\b(EUR|USD|GBP)\b", s, flags=re.IGNORECASE) is not None and re.search(r"\b\d+[\.,]\d{2}\b", s) is not None:
                return True
            if re.fullmatch(r"\d{2,4}\-?[A-Z]{2,}", s.replace(" ", "").upper()):
                return True
        if field in {"order_no"}:
            if re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", s):
                return True
            if not re.search(r"\b[A-Z0-9]{2,}[\-/][A-Z0-9]{1,}\b", s.replace(" ", "").upper()):
                return True
        if field in {"date"}:
            if re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", s) is None:
                return True
        if field in {"buyer"}:
            # In garment SO/PO buyer is usually a short code/number
            if not re.fullmatch(r"[A-Z0-9]{1,8}", s.replace(" ", "").upper()):
                return True
        if field in {"season"}:
            if not re.search(r"\b[WS]\b", s, flags=re.IGNORECASE) and not re.search(r"\b\d{4}\b", s):
                return True
            # season should not contain other label words
            if re.search(r"\b(buyer|payment\s*terms|supplier|date|order)\b", s, flags=re.IGNORECASE):
                return True
        if field in {"supplier"}:
            # supplier should not be prefixed with order/date tokens
            if re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", s):
                return True
            if re.search(r"\b[A-Z0-9]{2,}[\-/][A-Z0-9]{1,}\b", s.replace(" ", "").upper()):
                return True
            if re.search(r"\b(order\s*nr|order\s*no|date)\b", s, flags=re.IGNORECASE):
                return True
        if field in {"purchaser"}:
            # purchaser block should not collapse to another label
            if re.search(r"\b(send\s*to|ship\s*to|payment\s*terms|buyer|season|supplier|date|order)\b", s, flags=re.IGNORECASE):
                return True
        if field in {"payment_terms"}:
            # payment terms should not contain season/buyer header tokens
            if re.search(r"\b[WS]\s*\d{4}\b", s, flags=re.IGNORECASE) and re.search(r"\b\d{3,6}\b", s):
                # if it begins with season/buyer-like tokens, treat as contaminated
                if re.match(r"^\s*[WS]\s*\d{4}\s+\d{3,6}\b", s, flags=re.IGNORECASE):
                    return True
            # If payment terms collapses to just a season value (common OCR swap), treat as bad
            if re.fullmatch(r"\s*[WS]\s*\d{4}\s*", s, flags=re.IGNORECASE):
                return True
        if field in {"pvp"}:
            if not (re.search(r"\b(EUR|USD|GBP)\b", s, flags=re.IGNORECASE) and re.search(r"\b\d+[\.,]\d{2}\b", s)):
                return True
        if field in {"market_of_origin"}:
            if not re.search(r"\b[A-Z]{3,}\b", s):
                return True
        if field in {"description"}:
            if re.search(r"\b(EUR|USD|GBP)\b", s, flags=re.IGNORECASE):
                return True
        return False

    # Garment SO/PO: anchor-based parsing from raw OCR text to handle mixed layouts (left->right and top->bottom)
    try:
        def _join_prices(lines: List[str]) -> str:
            out: List[str] = []
            seen = set()
            for ln in (lines or []):
                s = " ".join(str(ln or "").strip().split())
                if not s:
                    continue
                k = s.upper()
                if k in seen:
                    continue
                seen.add(k)
                out.append(s)
            return ", ".join(out).strip()

        def _norm_line(s: str) -> str:
            return " ".join((s or "").strip().split())

        raw_lines = [_norm_line(ln) for ln in (text or "").replace("\r", "\n").split("\n")]
        lines2 = [ln for ln in raw_lines if ln]

        def _find_idx(pred) -> Optional[int]:
            for ii, ln in enumerate(lines2):
                if pred(ln):
                    return ii
            return None

        def _collect_until(start_idx: int, stop_pred, max_lines: int) -> str:
            parts: List[str] = []
            for ii in range(start_idx, min(len(lines2), start_idx + max_lines)):
                ln = lines2[ii]
                if stop_pred(ln):
                    break
                parts.append(ln)
            return " ".join([p for p in parts if p]).strip()

        # PURCHASER block: PURCHASER then company lines until SEND TO
        if _field_bad("purchaser", merged.get("purchaser")):
            i_p = _find_idx(lambda s: _norm_key(s) in {"purchaser", "purchaser:"} or s.upper() == "PURCHASER")
            i_s = _find_idx(lambda s: "send" in _norm_key(s) and ("to" in _norm_key(s) or _norm_key(s) in {"sendto"}))
            if i_p is not None:
                end_idx = i_s if (i_s is not None and i_s > i_p) else None
                parts = []
                for ii in range(i_p + 1, min(len(lines2), (end_idx if end_idx is not None else i_p + 6))):
                    if end_idx is not None and ii >= end_idx:
                        break
                    if _norm_key(lines2[ii]) in {"sendto", "sendto:", "sendto-", "sendto."}:
                        break
                    parts.append(lines2[ii])
                v = " ".join([p for p in parts if p]).strip()
                if v:
                    merged["purchaser"] = v

        # SEND TO address: SEND TO then lines until Tax office number / ORDER-NR / DATE
        if _field_bad("send_to", merged.get("send_to")):
            i_st = _find_idx(lambda s: ("send" in _norm_key(s) and ("to" in _norm_key(s) or _norm_key(s) in {"sendto"})) or ("ship" in _norm_key(s) and "to" in _norm_key(s)))
            if i_st is not None:
                def _stop_sendto(s: str) -> bool:
                    nk = _norm_key(s)
                    return nk.startswith("taxofficenumber") or nk in {"ordernr", "order", "date", "supplier", "season", "buyer", "paymentterms"} or "order" in nk and "nr" in nk

                v = _collect_until(i_st + 1, _stop_sendto, 8)
                if v:
                    merged["send_to"] = v

        # Item block (ARTICLE/DESCRIPTION/MARKET OF ORIGIN/PVP) from text anchors
        # Use header line containing ARTICLE/DESCRIPTION/MARKET and then read subsequent lines until COMPOSITIONS/CARE
        item_header_idx = _find_idx(lambda s: "article" in _norm_key(s) and "description" in _norm_key(s))
        if item_header_idx is not None:
            def _stop_item(s: str) -> bool:
                nk = _norm_key(s)
                return "compositions" in nk or "composition" in nk or nk.startswith("care") or "careinstructions" in nk

            block = []
            for ii in range(item_header_idx + 1, min(len(lines2), item_header_idx + 25)):
                if _stop_item(lines2[ii]):
                    break
                block.append(lines2[ii])

            def _is_price(s: str) -> bool:
                return re.search(r"\b(EUR|USD|GBP)\b", s, flags=re.IGNORECASE) is not None and re.search(r"\b\d+[\.,]\d{2}\b", s) is not None

            prices = [ln for ln in block if _is_price(ln)]
            if prices:
                pv = _join_prices(prices)
                if pv:
                    existing = str(merged.get("pvp") or "").strip()
                    existing_count = len([p for p in re.split(r"\s*,\s*", existing) if p.strip()]) if existing else 0
                    new_count = len([p for p in pv.split(",") if p.strip()])
                    if _field_bad("pvp", merged.get("pvp")) or new_count > existing_count:
                        merged["pvp"] = pv

            if _field_bad("article", merged.get("article")):
                for ln in block:
                    m = re.search(r"\b\d{3,5}\s*/\s*\d{1,4}\b", ln)
                    if m:
                        merged["article"] = m.group(0).replace(" ", "")
                        break

            if _field_bad("market_of_origin", merged.get("market_of_origin")):
                for ln in block:
                    if re.search(r"\bINDONESIA\b", ln, flags=re.IGNORECASE):
                        merged["market_of_origin"] = "INDONESIA"
                        break

            if _field_bad("description", merged.get("description")):
                for ln in block:
                    if _is_price(ln):
                        continue
                    if re.search(r"\bINDONESIA\b", ln, flags=re.IGNORECASE):
                        continue
                    if re.search(r"\b\d{3,5}\s*/\s*\d{1,4}\b", ln):
                        continue
                    # avoid common noise tokens
                    if re.search(r"\b(DRIGIN|ORIGIN|MARKETOF)\b", ln, flags=re.IGNORECASE):
                        continue
                    if len(ln) >= 6:
                        merged["description"] = ln
                        break

        # Header row parsing: "ORDER NR DATE SUPPLIER" then "<order> <date> <supplier>"
        # OCR often flattens these into a single line; parse by known token shapes.
        if _field_bad("order_no", merged.get("order_no")) or _field_bad("date", merged.get("date")) or _field_bad("supplier", merged.get("supplier")):
            for i in range(0, max(0, len(lines2) - 1)):
                h = _norm_key(lines2[i])
                if not ("ordernr" in h and "date" in h and "supplier" in h):
                    continue
                # take next non-empty line as the values row
                j = i + 1
                while j < len(lines2) and not lines2[j].strip():
                    j += 1
                if j >= len(lines2):
                    break
                vline = lines2[j]
                # IMPORTANT: do not remove spaces here; it can glue order + date and overmatch
                m_ord = re.search(r"\b([A-Z0-9]{2,}[\-/][A-Z0-9]{1,})\b", vline, flags=re.IGNORECASE)
                m_dt = re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", vline)
                if m_ord and _field_bad("order_no", merged.get("order_no")):
                    merged["order_no"] = m_ord.group(1).strip()
                if m_dt and _field_bad("date", merged.get("date")):
                    merged["date"] = m_dt.group(1).strip()
                # supplier = remaining text after removing order/date tokens
                if _field_bad("supplier", merged.get("supplier")):
                    supplier_txt = vline
                    if m_ord:
                        supplier_txt = supplier_txt.replace(m_ord.group(1), " ")
                        supplier_txt = supplier_txt.replace(m_ord.group(1).replace("-", " - "), " ")
                    if m_dt:
                        supplier_txt = supplier_txt.replace(m_dt.group(1), " ")
                    supplier_txt = " ".join(supplier_txt.split()).strip(" -\t")
                    if supplier_txt and len(supplier_txt) >= 4:
                        merged["supplier"] = supplier_txt
                break

        # Header row parsing: "SEASON BUYER PAYMENT TERMS" then "W 2025 1516 TRANSF..."
        if _field_bad("season", merged.get("season")) or _field_bad("buyer", merged.get("buyer")) or _field_bad("payment_terms", merged.get("payment_terms")):
            for i in range(0, max(0, len(lines2) - 1)):
                h = _norm_key(lines2[i])
                if not ("season" in h and "buyer" in h and "paymentterms" in h):
                    continue
                j = i + 1
                while j < len(lines2) and not lines2[j].strip():
                    j += 1
                if j >= len(lines2):
                    break
                vline = lines2[j]
                # season like "W 2025" or "S 2024"
                m_season = re.search(r"\b([WS])\s*(\d{4})\b", vline, flags=re.IGNORECASE)
                # buyer is typically a short numeric/alpha code
                m_buyer = re.search(r"\b([A-Z0-9]{1,8})\b", vline.replace(" ", "").upper())
                if m_season and _field_bad("season", merged.get("season")):
                    merged["season"] = f"{m_season.group(1).upper()} {m_season.group(2)}".strip()
                # buyer: prefer the first 3-6 digit token after season
                buyer_val = None
                post = vline
                if m_season:
                    post = vline[m_season.end() :]
                m_buyer2 = re.search(r"\b(\d{3,6}|[A-Z0-9]{1,8})\b", post.strip(), flags=re.IGNORECASE)
                if m_buyer2:
                    buyer_val = m_buyer2.group(1).strip()
                elif m_buyer:
                    buyer_val = m_buyer.group(1).strip()
                if buyer_val and _field_bad("buyer", merged.get("buyer")):
                    merged["buyer"] = buyer_val.replace(" ", "")
                # payment terms: remainder after removing season and buyer token
                if _field_bad("payment_terms", merged.get("payment_terms")):
                    payment_txt = vline
                    if m_season:
                        payment_txt = payment_txt.replace(m_season.group(0), " ")
                    if buyer_val:
                        payment_txt = re.sub(r"\b" + re.escape(buyer_val) + r"\b", " ", payment_txt)
                    payment_txt = " ".join(payment_txt.split()).strip(" -\t")
                    if payment_txt and len(payment_txt) >= 6:
                        merged["payment_terms"] = payment_txt
                break
    except Exception:
        pass

    # Garment SO/PO: state-machine parsing from raw text labels/values (handles stacked labels then stacked values)
    try:
        def _nk(s: str) -> str:
            return _norm_key(s)

        raw_lines = [_norm_line(ln) for ln in (text or "").replace("\r", "\n").split("\n")]
        seq = [ln for ln in raw_lines if ln]

        # Canonical label -> field key
        label_defs: Dict[str, Dict[str, Any]] = {
            "PURCHASER": {"field": "purchaser", "aliases": {"purchaser", "purchaser:"}},
            "SEND TO": {"field": "send_to", "aliases": {"sendto", "sendto:", "sendt0", "sendto-", "send to", "shipto", "ship to", "kirim ke"}},
            "TAX OFFICE NUMBER": {"field": "tax_office_number", "aliases": {"taxofficenumber", "tax office number", "taxofficenumber:"}},
            "ORDER-NR": {"field": "order_no", "aliases": {"ordernr", "order nr", "order-no", "order no", "order number", "sales order", "no so", "nomor so"}},
            "DATE": {"field": "date", "aliases": {"date", "tanggal", "tgl"}},
            "SUPPLIER": {"field": "supplier", "aliases": {"supplier", "vendor", "pemasok"}},
            "SEASON": {"field": "season", "aliases": {"season"}},
            "BUYER": {"field": "buyer", "aliases": {"buyer", "pembeli"}},
            "PAYMENT TERMS": {"field": "payment_terms", "aliases": {"paymentterms", "payment terms", "termsofpayment", "syaratpembayaran"}},
            "SUPPLIER REF": {"field": "supplier_ref", "aliases": {"supplierref", "supplier ref", "supplierref:"}},
            "ARTICLE": {"field": "article", "aliases": {"article", "articie", "artlcle"}},
            "DESCRIPTION": {"field": "description", "aliases": {"description", "descripton", "descriplion"}},
            "MARKET OF ORIGIN": {"field": "market_of_origin", "aliases": {"marketof", "marketoforigin", "market origin", "marketofor", "origin"}},
            "PVP": {"field": "pvp", "aliases": {"pvp"}},
            "COMPOSITIONS INFORMATION": {"field": "_stop", "aliases": {"compositionsineormation", "compositionsinformation", "compositioninformation", "compotitionsinformation", "compositionweightedcolors"}},
            "CARE INSTRUCTIONS": {"field": "_stop", "aliases": {"careinstructions", "careinstruction", "care introtion"}},
        }

        long_fields = {"purchaser", "send_to"}

        def _match_label(line: str) -> Optional[str]:
            n = _nk(line)
            if not n:
                return None
            # Special-case: send to (OCR missing spaces)
            if ("send" in n and ("to" in n or n.endswith("t0"))) or n in {"sendto", "shipto"}:
                return "SEND TO"
            for canon, spec in label_defs.items():
                for a in spec.get("aliases") or set():
                    if n == _nk(str(a)):
                        return canon
            # fuzzy fallback
            if fuzz is not None:
                best = 0.0
                best_key = None
                for canon, spec in label_defs.items():
                    for a in spec.get("aliases") or set():
                        na = _nk(str(a))
                        if not na:
                            continue
                        score = float(fuzz.ratio(n, na))
                        if score > best:
                            best = score
                            best_key = canon
                if best_key is not None and best >= 85.0:
                    return best_key
            return None

        def _is_price_line(s: str) -> bool:
            return re.search(r"\b\d+[\.,]\d{2}\b", s) is not None and re.search(r"\b(EUR|USD|GBP)\b", s, flags=re.IGNORECASE) is not None

        def _is_article_line(s: str) -> bool:
            return re.search(r"\b\d{3,5}\s*/\s*\d{1,4}\b", s) is not None

        # Pass 1: detect blocks for long fields (PURCHASER, SEND TO)
        i = 0
        while i < len(seq):
            canon = _match_label(seq[i])
            if canon is None:
                i += 1
                continue
            field = label_defs.get(canon, {}).get("field")
            if field in long_fields:
                parts: List[str] = []
                j = i + 1
                while j < len(seq) and len(parts) < 8:
                    nxt = seq[j]
                    nxt_label = _match_label(nxt)
                    if nxt_label is not None and nxt_label != canon:
                        break
                    # stop at strong anchors
                    if _nk(nxt).startswith("taxofficenumber"):
                        break
                    parts.append(nxt)
                    j += 1
                v = " ".join([p for p in parts if p]).strip()
                if v and _field_bad(str(field), merged.get(field)):
                    merged[str(field)] = v
                i = j
                continue
            i += 1

        # Pass 2: stacked labels -> stacked values mapping
        pending: List[str] = []
        pending_fields: List[str] = []
        for ln in seq:
            canon = _match_label(ln)
            if canon is not None:
                f = label_defs.get(canon, {}).get("field")
                if f == "_stop":
                    pending.clear()
                    pending_fields.clear()
                    continue
                if f in long_fields:
                    pending.clear()
                    pending_fields.clear()
                    continue
                pending.append(canon)
                pending_fields.append(str(f))
                continue

            # Value line
            if not pending_fields:
                continue
            f = pending_fields.pop(0)
            pending.pop(0)
            v = ln.strip()
            if not v:
                continue
            # Normalize by field
            if f == "order_no":
                m = re.search(r"([A-Z0-9\-\/]+)", v, flags=re.IGNORECASE)
                v = (m.group(1) if m else v).strip()
            elif f == "date":
                m = re.search(r"([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})", v)
                v = (m.group(1) if m else v).strip()
            elif f == "article":
                m = re.search(r"(\d{3,5}\s*/\s*\d{1,4})", v)
                v = (m.group(1) if m else v).replace(" ", "")
            elif f == "market_of_origin":
                if re.search(r"\bINDONESIA\b", v, flags=re.IGNORECASE):
                    v = "INDONESIA"
            elif f == "pvp":
                # pvp may be multiple price lines; keep the first here (additional price lines parsed below)
                if not _is_price_line(v):
                    continue
            if v and (_field_bad(str(f), merged.get(f)) or f not in merged):
                merged[f] = v

        # Pass 3: item block enrichment (description/origin/prices) between ARTICLE/DESCRIPTION headers and COMPOSITIONS
        if any(_match_label(ln) == "ARTICLE" for ln in seq):
            # collect region from first ARTICLE label until compositions/care
            start_idx = None
            for idx, ln in enumerate(seq):
                if _match_label(ln) == "ARTICLE":
                    start_idx = idx
                    break
            if start_idx is not None:
                block: List[str] = []
                for ln in seq[start_idx + 1 : start_idx + 35]:
                    canon = _match_label(ln)
                    if canon in {"COMPOSITIONS INFORMATION", "CARE INSTRUCTIONS"}:
                        break
                    if canon is not None:
                        continue
                    block.append(ln)

                if _field_bad("article", merged.get("article")):
                    for ln in block:
                        if _is_article_line(ln):
                            m = re.search(r"(\d{3,5}\s*/\s*\d{1,4})", ln)
                            if m:
                                merged["article"] = m.group(1).replace(" ", "")
                                break

                if _field_bad("market_of_origin", merged.get("market_of_origin")):
                    for ln in block:
                        if re.search(r"\bINDONESIA\b", ln, flags=re.IGNORECASE):
                            merged["market_of_origin"] = "INDONESIA"
                            break

                if _field_bad("pvp", merged.get("pvp")):
                    prices = [ln for ln in block if _is_price_line(ln)]
                    if prices:
                        pv = _join_prices(prices)
                        if pv:
                            merged["pvp"] = pv

                if _field_bad("description", merged.get("description")):
                    for ln in block:
                        if _is_price_line(ln) or _is_article_line(ln):
                            continue
                        if re.search(r"\bINDONESIA\b", ln, flags=re.IGNORECASE):
                            continue
                        if re.search(r"\b(DRIGIN|MARKETOF|ORIGIN)\b", ln, flags=re.IGNORECASE):
                            continue
                        if len(ln) >= 6:
                            merged["description"] = ln
                            break
    except Exception:
        pass

    # Deterministic label -> value scan (repairs common swaps when OCR line order is unreliable)
    try:
        def _find_value_after(canon_label: str, validator, max_lookahead: int = 6) -> Optional[str]:
            for idx, ln in enumerate(seq):
                if _match_label(ln) != canon_label:
                    continue
                for j in range(idx + 1, min(len(seq), idx + 1 + max_lookahead)):
                    v = (seq[j] or "").strip()
                    if not v:
                        continue
                    if _match_label(v) is not None:
                        continue
                    if validator(v):
                        return v
            return None

        def _find_values_after(canon_label: str, validator, max_lookahead: int = 10) -> List[str]:
            vals: List[str] = []
            for idx, ln in enumerate(seq):
                if _match_label(ln) != canon_label:
                    continue
                try:
                    ln0 = (ln or "").strip()
                    if canon_label == "PVP":
                        m = re.search(r"\b\d+[\.,]\d{2}\b\s*(?:EUR|USD|GBP)\b", ln0, flags=re.IGNORECASE)
                        if m:
                            v0 = m.group(0).strip()
                            if validator(v0):
                                vals.append(v0)
                except Exception:
                    pass
                for j in range(idx + 1, min(len(seq), idx + 1 + max_lookahead)):
                    v = (seq[j] or "").strip()
                    if not v:
                        continue
                    if _match_label(v) is not None:
                        break
                    if validator(v):
                        vals.append(v)
                break
            return vals

        def _is_order_no(v: str) -> bool:
            vv = (v or "").upper()
            return re.search(r"\b[A-Z0-9]{2,}[\-/][A-Z0-9]{1,}\b", vv) is not None and re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", vv) is None

        def _is_date(v: str) -> bool:
            return re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", v) is not None

        def _is_buyer(v: str) -> bool:
            return re.fullmatch(r"[A-Z0-9]{1,8}", v.replace(" ", "").upper()) is not None

        def _is_season(v: str) -> bool:
            return re.search(r"\b[WS]\b", v, flags=re.IGNORECASE) is not None and re.search(r"\b\d{4}\b", v) is not None

        def _is_payment_terms(v: str) -> bool:
            return re.search(r"\bDAY\b", v, flags=re.IGNORECASE) is not None or re.search(r"\bDAYS\b", v, flags=re.IGNORECASE) is not None or re.search(r"\bTRANSF\b", v, flags=re.IGNORECASE) is not None

        def _is_supplier(v: str) -> bool:
            # prefer a supplier-like string (company) that is not just codes/dates
            if _is_date(v) or _is_order_no(v):
                return False
            if len(v) < 6:
                return False
            return re.search(r"\b(CO\.?|LTD\.?|LIMITED|TRADING|COMPANY)\b", v, flags=re.IGNORECASE) is not None or re.search(r"\b[A-Z]{2,}\b", v) is not None

        def _is_pvp(v: str) -> bool:
            return _is_price_line(v)

        if _field_bad("order_no", merged.get("order_no")):
            v = _find_value_after("ORDER-NR", _is_order_no)
            if v:
                m = re.search(r"\b([A-Z0-9]{2,}[\-/][A-Z0-9]{1,})\b", v, flags=re.IGNORECASE)
                merged["order_no"] = (m.group(1) if m else v).strip()

        if _field_bad("date", merged.get("date")):
            v = _find_value_after("DATE", _is_date)
            if v:
                m = re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", v)
                merged["date"] = (m.group(1) if m else v).strip()

        if _field_bad("season", merged.get("season")):
            v = _find_value_after("SEASON", _is_season)
            if v:
                merged["season"] = v.strip()

        if _field_bad("buyer", merged.get("buyer")):
            v = _find_value_after("BUYER", _is_buyer)
            if v:
                merged["buyer"] = v.replace(" ", "").strip()

        if _field_bad("payment_terms", merged.get("payment_terms")):
            v = _find_value_after("PAYMENT TERMS", _is_payment_terms, max_lookahead=10)
            if v:
                merged["payment_terms"] = v.strip()

        if _field_bad("supplier", merged.get("supplier")):
            v = _find_value_after("SUPPLIER", _is_supplier, max_lookahead=10)
            if v:
                merged["supplier"] = v.strip()

        # PVP may be multiple lines; collect all price-like values after the label
        try:
            pvps = _find_values_after("PVP", _is_pvp, max_lookahead=14)
            if pvps:
                pv = _join_prices(pvps)
                if pv:
                    existing = str(merged.get("pvp") or "").strip()
                    existing_count = len([p for p in re.split(r"\s*,\s*", existing) if p.strip()]) if existing else 0
                    new_count = len([p for p in re.split(r"\s*,\s*", pv) if p.strip()])
                    if _field_bad("pvp", merged.get("pvp")) or new_count > existing_count:
                        merged["pvp"] = pv
        except Exception:
            pass

        # Purchaser / send_to: avoid capturing another label as the value
        if _field_bad("purchaser", merged.get("purchaser")):
            i_p = None
            for ii, ln in enumerate(seq):
                if _match_label(ln) == "PURCHASER":
                    i_p = ii
                    break
            if i_p is not None:
                parts: List[str] = []
                for j in range(i_p + 1, min(len(seq), i_p + 10)):
                    if _match_label(seq[j]) is not None:
                        break
                    parts.append(seq[j])
                v = " ".join([p for p in parts if p]).strip()
                if v:
                    merged["purchaser"] = v

            # OCR sometimes mixes columns so PURCHASER label appears without subsequent lines (next label is SEND TO)
            # or its value is captured elsewhere. Heuristic: capture the purchaser company block (EMEA Aspire Trading...)
            # before Tax office number / order header.
            if _field_bad("purchaser", merged.get("purchaser")):
                i_em = None
                for ii, ln in enumerate(seq):
                    if re.search(r"\bEMEA\b", ln, flags=re.IGNORECASE) and re.search(r"\b(ASPIRE|TRADING)\b", ln, flags=re.IGNORECASE):
                        i_em = ii
                        break
                if i_em is not None:
                    parts2: List[str] = []
                    for j in range(i_em, min(len(seq), i_em + 12)):
                        if re.search(r"\btax\s*office\s*number\b", seq[j], flags=re.IGNORECASE):
                            break
                        if re.search(r"\border\s*nr\b", seq[j], flags=re.IGNORECASE) and re.search(r"\bdate\b", seq[j], flags=re.IGNORECASE):
                            break
                        if _match_label(seq[j]) is not None and j != i_em:
                            break
                        parts2.append(seq[j])
                    v2 = " ".join([p for p in parts2 if p]).strip()
                    if v2:
                        merged["purchaser"] = v2

        if _field_bad("send_to", merged.get("send_to")):
            i_s = None
            for ii, ln in enumerate(seq):
                if _match_label(ln) == "SEND TO":
                    i_s = ii
                    break
            if i_s is not None:
                def _stop_sendto_line(s: str) -> bool:
                    if not (s or "").strip():
                        return False
                    if _match_label(s) is not None:
                        return True
                    # Stop at strong anchors / header rows
                    if re.search(r"\btax\s*office\s*number\b", s, flags=re.IGNORECASE):
                        return True
                    if re.search(r"\border\s*nr\b", s, flags=re.IGNORECASE) and re.search(r"\bdate\b", s, flags=re.IGNORECASE):
                        return True
                    if re.search(r"\bseason\b", s, flags=re.IGNORECASE) and re.search(r"\bbuyer\b", s, flags=re.IGNORECASE):
                        return True
                    return False

                # When OCR merges two columns, purchaser text can leak into send_to lines.
                # Trim everything after purchaser-anchors (BCW OFFICE, DUBAI, EMIRATOS, EMEA ASPIRE, TRADING FZE).
                def _trim_purchaser_tail(s: str) -> str:
                    t = (s or "").strip()
                    if not t:
                        return t
                    anchors = [
                        r"\bBCW\b",
                        r"\bOFFICE\b",
                        r"\bEXPOREGISTER\b",
                        r"\bDUBAI\b",
                        r"\bEMIRATOS\b",
                        r"\bEMEA\b",
                        r"\bASPIRE\b",
                        r"\bTRADING\b",
                        r"\bFZE\b",
                    ]
                    mpos = None
                    for a in anchors:
                        m = re.search(a, t, flags=re.IGNORECASE)
                        if m:
                            mpos = m.start() if mpos is None else min(mpos, m.start())
                    if mpos is not None and mpos > 0:
                        t = t[:mpos].rstrip(" ,;:-\t")
                    return t

                parts = []
                for j in range(i_s + 1, min(len(seq), i_s + 20)):
                    ln = (seq[j] or "").strip()
                    if _stop_sendto_line(ln):
                        break
                    ln2 = _trim_purchaser_tail(ln)
                    if not ln2:
                        continue
                    # If the line is clearly purchaser-column after trimming, skip it
                    if re.search(r"\b(DUBAI|EMIRATOS|EXPOREGISTER|BCW|EMEA|ASPIRE|TRADING|FZE)\b", ln2, flags=re.IGNORECASE):
                        continue
                    parts.append(ln2)
                v = " ".join([p for p in parts if p]).strip()
                if v:
                    merged["send_to"] = v
    except Exception:
        pass

    # Heuristic: label on one line, value on next line (common in sales order headers)
    lines = [ln.strip() for ln in (text or "").replace("\r", "\n").split("\n")]
    label_map = {
        "order_no": {"order-nr", "order nr", "order no", "order number", "no order", "no. order", "no so", "no. so", "sales order", "sales order no", "nomor so"},
        "date": {"date", "tanggal", "tgl"},
        "supplier": {"supplier", "vendor", "pemasok"},
        "season": {"season"},
        "buyer": {"buyer", "pembeli"},
        "payment_terms": {"payment terms", "terms of payment", "syarat pembayaran"},
        "purchaser": {"purchaser", "pemesan"},
        "send_to": {"send to", "sendto", "send t0", "send t o", "ship to", "shipto", "kirim ke"},
    }

    def _is_label(s: str, candidates: set) -> bool:
        ns = _norm_key(s)
        if any(ns == _norm_key(c) for c in candidates):
            return True
        if fuzz is None:
            return False

        # OCR often introduces minor typos. Use fuzzy match against normalized candidates.
        # Example: "0rder nr" -> "order nr"
        best = 0.0
        for c in candidates:
            nc = _norm_key(str(c))
            if not nc:
                continue
            score = float(fuzz.ratio(ns, nc))
            if score > best:
                best = score
        # SEND TO is often noisy in OCR; accept a lower fuzzy threshold
        if candidates is label_map.get("send_to"):
            return best >= 80.0
        return best >= 90.0

    def _label_to_field(s: str) -> Optional[str]:
        for field, aliases in label_map.items():
            if _is_label(s, aliases):
                return field
        return None

    for i, ln in enumerate(lines[:-1]):
        nxt = lines[i + 1]
        if not ln or not nxt:
            continue

        field = _label_to_field(ln)
        if not field or field in merged:
            continue

        # Don't treat another label as the value (handles stacked label blocks)
        if _label_to_field(nxt) is not None:
            continue

        if field == "date":
            m = re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", nxt)
            if m:
                merged[field] = m.group(1)
        elif field == "order_no":
            m = re.search(r"\b([A-Z0-9\-\/]+)\b", nxt, flags=re.IGNORECASE)
            if m:
                merged[field] = m.group(1)
        elif field in ("purchaser", "send_to"):
            # allow a short multi-line value until the next label
            parts = [nxt.strip()]
            for j in range(i + 2, min(len(lines), i + 5)):
                if not lines[j].strip():
                    break
                if _label_to_field(lines[j]) is not None:
                    break
                parts.append(lines[j].strip())
            merged[field] = " ".join([p for p in parts if p]).strip()
        else:
            merged[field] = nxt.strip()

    # Handle stacked label blocks: LABEL1\nLABEL2\n... then VALUE1\nVALUE2\n...
    i = 0
    while i < len(lines) - 2:
        if not lines[i].strip():
            i += 1
            continue

        first_field = _label_to_field(lines[i])
        second_field = _label_to_field(lines[i + 1])
        if first_field is None or second_field is None:
            i += 1
            continue

        labels: List[str] = []
        fields: List[str] = []
        j = i
        while j < len(lines):
            f = _label_to_field(lines[j])
            if f is None:
                break
            if f in merged:
                labels.append(lines[j])
                fields.append(f)
            else:
                labels.append(lines[j])
                fields.append(f)
            j += 1

        if len(fields) < 2:
            i += 1
            continue

        # Read same number of value lines
        k = j
        values: List[str] = []
        while k < len(lines) and len(values) < len(fields):
            if not lines[k].strip():
                k += 1
                continue
            if _label_to_field(lines[k]) is not None:
                break
            values.append(lines[k].strip())
            k += 1

        if len(values) == len(fields):
            for f, v in zip(fields, values):
                if f in merged:
                    continue
                if f == "date":
                    m = re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", v)
                    merged[f] = m.group(1) if m else v
                elif f == "order_no":
                    m = re.search(r"\b([A-Z0-9\-\/]+)\b", v, flags=re.IGNORECASE)
                    merged[f] = m.group(1) if m else v
                else:
                    merged[f] = v
            i = k
            continue

        i += 1

    # Fallback: if DATE label missed, still try to pick a date candidate
    if "date" not in merged:
        m = re.search(r"\b([0-3]?\d[\./\-][01]?\d[\./\-]\d{2,4})\b", text or "")
        if m:
            merged["date"] = (m.group(1) or "").strip()

    # Fallback: if ORDER-NR label missed, try to grab a nearby order-like token
    if "order_no" not in merged:
        m = re.search(r"\b(\d{3,}[A-Z]?\-\w+)\b", text or "")
        if m:
            merged["order_no"] = (m.group(1) or "").strip()

    # Text-window fallback for multi-line PVP (when OCR keeps prices as free text, not seq/table)
    try:
        pvp_existing = str(merged.get("pvp") or "").strip()
        pvp_existing_count = len([p for p in re.split(r"\s*,\s*", pvp_existing) if p.strip()]) if pvp_existing else 0
        if pvp_existing_count < 2 and isinstance(text, str) and text.strip():
            price_re = re.compile(r"\b\d+[\.,]\d{2}\b\s*(?:EUR|USD|GBP)\b", flags=re.IGNORECASE)
            candidates: List[str] = []
            t0 = text
            for m in re.finditer(r"\bPVP\b", t0, flags=re.IGNORECASE):
                start = m.start()
                # Scan a bounded window after the PVP label for price tokens
                window = t0[start : min(len(t0), start + 900)]
                for mm in price_re.finditer(window):
                    candidates.append(mm.group(0).strip())
                if candidates:
                    break
            pv = _join_prices(candidates) if candidates else ""
            if pv:
                new_count = len([p for p in re.split(r"\s*,\s*", pv) if p.strip()])
                if new_count > pvp_existing_count:
                    merged["pvp"] = pv
    except Exception:
        pass

    # Table-based fallback for multi-line PVP (often appears as multiple rows in a PVP column)
    try:
        pvp_existing = str(merged.get("pvp") or "").strip()
        pvp_existing_count = len([p for p in re.split(r"\s*,\s*", pvp_existing) if p.strip()]) if pvp_existing else 0
        if pvp_existing_count < 2 and isinstance(tables, list) and tables:
            price_re = re.compile(r"\b\d+[\.,]\d{2}\b\s*(?:EUR|USD|GBP)\b", flags=re.IGNORECASE)

            def _collect_prices_from_cell(v: Any) -> List[str]:
                s = str(v or "").strip()
                if not s:
                    return []
                parts = re.split(r"[\n\r]+", s)
                out: List[str] = []
                for part in parts:
                    for m in price_re.finditer(part):
                        out.append(m.group(0).strip())
                return out

            candidates: List[str] = []
            for t in tables:
                if not isinstance(t, dict):
                    continue
                tk = str(t.get("table_kind") or "").strip().lower()
                if tk in {"total_order_grid", "partial_deliveries_grid"}:
                    continue

                headers = t.get("headers") or []
                rows = t.get("rows") or []
                if not isinstance(headers, list) or not headers:
                    continue

                # Find a PVP column in headers (robust to OCR variants)
                pvp_key: Optional[str] = None
                for h in headers:
                    hh = str(h or "")
                    if re.search(r"\bPVP\b", hh, flags=re.IGNORECASE) or _norm_key(hh) == "pvp":
                        pvp_key = str(h)
                        break
                if pvp_key is None:
                    continue

                if isinstance(rows, list):
                    for r in rows:
                        if isinstance(r, dict) and pvp_key in r:
                            candidates.extend(_collect_prices_from_cell(r.get(pvp_key)))
                        elif isinstance(r, dict):
                            # Sometimes keys are normalized differently; try any key that looks like PVP
                            for kk, vv in r.items():
                                if re.search(r"\bPVP\b", str(kk or ""), flags=re.IGNORECASE) or _norm_key(str(kk or "")) == "pvp":
                                    candidates.extend(_collect_prices_from_cell(vv))
                                    break

                rm = t.get("rows_matrix")
                if isinstance(rm, list) and rm:
                    try:
                        header_row = rm[0] if isinstance(rm[0], list) else None
                        if isinstance(header_row, list):
                            idx = None
                            for i, h in enumerate(header_row):
                                if re.search(r"\bPVP\b", str(h or ""), flags=re.IGNORECASE) or _norm_key(str(h or "")) == "pvp":
                                    idx = i
                                    break
                            if idx is not None:
                                for row in rm[1:]:
                                    if isinstance(row, list) and idx < len(row):
                                        candidates.extend(_collect_prices_from_cell(row[idx]))
                    except Exception:
                        pass

            pv = _join_prices(candidates)
            if pv:
                new_count = len([p for p in re.split(r"\s*,\s*", pv) if p.strip()])
                if new_count > pvp_existing_count:
                    merged["pvp"] = pv
    except Exception:
        pass

    # Normalize payment_terms: remove leading season/buyer if OCR prepends it
    if isinstance(merged.get("payment_terms"), str):
        pt = merged.get("payment_terms") or ""
        # Example: "W 2025 1516 TRANSF. 90 DAYS ..." -> "TRANSF. 90 DAYS ..."
        pt2 = re.sub(r"^\s*[WS]\s*\d{4}\s+\d{3,6}\s+", "", pt, flags=re.IGNORECASE).strip()
        # Sometimes label words also leak: "BUYER PAYMENT TERMS W 2025 1516 TRANSF..."
        pt2 = re.sub(r"^\s*(?:buyer\s+)?payment\s*terms\s+", "", pt2, flags=re.IGNORECASE).strip()
        if pt2 and pt2 != pt:
            merged["payment_terms"] = pt2

    # Ensure composition label is present when detected in OCR text
    if "composition_label_part_colors" not in merged and isinstance(text, str) and text.strip():
        m = re.search(
            r"\bcomposition\s*label\s*/\s*part\s*colou?rs\b\s*[:\-]?\s*([A-Z0-9\-\s]+)",
            text,
            flags=re.IGNORECASE,
        )
        v = (m.group(1) if m else "")
        v = (v or "").strip()
        if v:
            merged["composition_label_part_colors"] = v
        else:
            # Multi-line fallback: find line with label then take next non-empty line
            lines = [ln.strip() for ln in text.replace("\r", "\n").split("\n") if ln.strip()]
            for i, ln in enumerate(lines):
                if re.search(r"\bcomposition\s*label\b", ln, flags=re.IGNORECASE) and re.search(
                    r"\bpart\s*colou?rs\b", ln, flags=re.IGNORECASE
                ):
                    if i + 1 < len(lines):
                        merged["composition_label_part_colors"] = lines[i + 1].strip()
                    break

    # COMPOSITIONS INFORMATION: often appears as a material table (OUTER SHELL / MAIN FABRIC / ...) without the label.
    # Build a compact multi-line string from either text or table rows.
    if _field_bad("compositions_information", merged.get("compositions_information")):
        try:
            def _format_compositions_info(s: str) -> str:
                out = " ".join((s or "").replace("\r", "\n").replace("\n", " ").split()).strip()
                if not out:
                    return out
                out = re.sub(r"\s*,\s*", ", ", out).strip(" ,")
                out = re.sub(r"(,\s*){2,}", ", ", out)
                return out.strip(" ,")

            def _parse_composition_from_tables(tables_any: Any) -> str:
                if not isinstance(tables_any, list):
                    return ""

                section_order = ["EMBELLISHMENT", "MAIN FABRIC", "SECONDARY FABRIC"]
                section_set = set(section_order)
                outer_shell_seen = False
                except_trimmings_seen = False
                current_section: Optional[str] = None

                materials: Dict[str, List[str]] = {k: [] for k in section_order}

                def _norm_cell(x: Any) -> str:
                    return " ".join(str(x or "").replace("\r", " ").replace("\n", " ").split()).strip()

                def _cell_is_stop(s: str) -> bool:
                    return (
                        re.search(
                            r"\b(CARE\s+INSTRUCTIONS|HANGTAG\s+LABEL|MAIN\s+LABEL|EXTERNAL\s+FABRIC|HANGING|TOTAL\s+ORDER|ORDER\-?NR|DATE|SUPPLIER|ARTICLE|DESCRIPTION|MARKET\s+OF\s+ORIGIN|PVP)\b",
                            s,
                            flags=re.IGNORECASE,
                        )
                        is not None
                    )

                def _cell_is_outer_shell(s: str) -> bool:
                    return re.search(r"\bOUTER\s+SHELL\b", s, flags=re.IGNORECASE) is not None

                def _cell_is_except_trimmings(s: str) -> bool:
                    return re.search(r"\bEXCEPT\s+FOR\s+TRIMMINGS\b", s, flags=re.IGNORECASE) is not None

                def _cell_section(s: str) -> Optional[str]:
                    for sec in section_order:
                        if re.search(r"\b" + re.escape(sec) + r"\b", s, flags=re.IGNORECASE):
                            return sec
                    return None

                def _cell_is_material(s: str) -> bool:
                    if not s:
                        return False
                    if re.search(r"\b\d{1,3}\s*%\b", s) is not None:
                        return True
                    if re.search(
                        r"\b(POLYESTER|VISCOSE|RECYCLED|FILAMENT|ELASTANE|COTTON|NYLON|ACRYLIC|WOOL)\b",
                        s,
                        flags=re.IGNORECASE,
                    ) is not None:
                        return True
                    return False

                def _push(sec: Optional[str], s: str) -> None:
                    if not sec or sec not in section_set:
                        return
                    if not s:
                        return
                    # Avoid capturing the section header itself as a value
                    if _cell_section(s) is not None and s.upper() == sec:
                        return
                    if _cell_is_outer_shell(s) or _cell_is_except_trimmings(s) or _cell_is_stop(s):
                        return
                    materials[sec].append(s)

                for t in tables_any:
                    rm = (t or {}).get("rows_matrix") if isinstance(t, dict) else None
                    if not isinstance(rm, list):
                        continue
                    for r in rm:
                        if not isinstance(r, list) or not r:
                            continue
                        cells = [_norm_cell(c) for c in r]
                        cells = [c for c in cells if c]
                        if not cells:
                            continue
                        row_text = " ".join(cells)
                        if _cell_is_stop(row_text):
                            continue
                        if _cell_is_outer_shell(row_text):
                            outer_shell_seen = True
                        if _cell_is_except_trimmings(row_text):
                            except_trimmings_seen = True
                            current_section = None
                            continue

                        sec = None
                        for c in cells:
                            sec = _cell_section(c) or sec
                        if sec is not None:
                            current_section = sec
                        # Collect material lines from other cells in the same row
                        for c in cells:
                            if _cell_is_stop(c) or _cell_is_outer_shell(c) or _cell_is_except_trimmings(c):
                                continue
                            if _cell_section(c) is not None:
                                continue
                            if _cell_is_material(c):
                                _push(current_section, c)

                # Build a stable comma-separated output without empty tokens
                out_parts: List[str] = []
                if outer_shell_seen:
                    out_parts.append("OUTER SHELL")
                for sec in section_order:
                    vals = materials.get(sec) or []
                    # Dedup while preserving order
                    seen = set()
                    vals2: List[str] = []
                    for v in vals:
                        vv = _norm_cell(v)
                        if not vv:
                            continue
                        key = vv.lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        vals2.append(vv)
                    if vals2:
                        out_parts.append(sec)
                        out_parts.extend(vals2)
                if except_trimmings_seen:
                    out_parts.append("EXCEPT FOR TRIMMINGS")
                return _format_compositions_info(", ".join(out_parts))

            def _is_composition_anchor(ln: str) -> bool:
                return re.search(
                    r"\b(OUTER\s+SHELL|EXCEPT\s+FOR\s+TRIMMINGS|EMBELLISHMENT|MAIN\s+FABRIC|SECONDARY\s+FABRIC|LINING)\b",
                    ln or "",
                    flags=re.IGNORECASE,
                ) is not None

            def _is_composition_line(ln: str) -> bool:
                s = (ln or "").strip()
                if not s:
                    return False
                if re.search(r"\b\d{1,3}\s*%\b", s) is not None:
                    return True
                if re.search(r"\b(POLYESTER|VISCOSE|RECYCLED|FILAMENT|ELASTANE|COTTON|NYLON|ACRYLIC|WOOL)\b", s, flags=re.IGNORECASE) is not None:
                    return True
                return False

            def _is_composition_stop(ln: str) -> bool:
                return re.search(
                    r"\b(CARE\s+INSTRUCTIONS|HANGTAG\s+LABEL|MAIN\s+LABEL|EXTERNAL\s+FABRIC|HANGING|TOTAL\s+ORDER|ARTICLE|DESCRIPTION|MARKET\s+OF\s+ORIGIN|PVP)\b",
                    ln or "",
                    flags=re.IGNORECASE,
                ) is not None

            # 1) Text-based capture
            if isinstance(text, str) and text.strip():
                lines3 = [ln.strip() for ln in text.replace("\r", "\n").split("\n")]
                start = None
                for idx, ln in enumerate(lines3):
                    if _is_composition_anchor(ln):
                        start = idx
                        break
                if start is not None:
                    parts: List[str] = []
                    for ln in lines3[start : start + 60]:
                        if _is_composition_stop(ln):
                            break
                        if _is_composition_anchor(ln) or _is_composition_line(ln):
                            cleaned_ln = " ".join((ln or "").split()).strip()
                            if cleaned_ln:
                                parts.append(cleaned_ln)
                    # Keep order but remove duplicates
                    seen = set()
                    parts2: List[str] = []
                    for p in parts:
                        k = p.lower()
                        if k in seen:
                            continue
                        seen.add(k)
                        parts2.append(p)
                    if parts2:
                        merged["compositions_information"] = _format_compositions_info(", ".join(parts2))

            # 2) Table-based capture (when text is messy but rows_matrix is available)
            if _field_bad("compositions_information", merged.get("compositions_information")) and isinstance(tables, list):
                parsed = _parse_composition_from_tables(tables)
                if parsed:
                    merged["compositions_information"] = parsed
        except Exception:
            pass

    # TOTAL ORDER: garment size grid usually has header COLOUR + sizes (XS/S/M/L/XL) + Total
    if _field_bad("total_order", merged.get("total_order")):
        try:
            def _parse_int_like(s: str) -> Optional[int]:
                v = (s or "").strip()
                if not v:
                    return None
                v = v.replace(" ", "")
                # Accept thousands separators
                v = v.replace(".", "")
                v = v.replace(",", "")
                if not re.fullmatch(r"\d{1,12}", v):
                    return None
                try:
                    return int(v)
                except Exception:
                    return None

            def _format_int(n: int) -> str:
                try:
                    return f"{int(n):,}"
                except Exception:
                    return str(n)

            def _is_total_order_grid(rm: Any) -> bool:
                if not isinstance(rm, list) or not rm:
                    return False
                size_tokens = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}

                # Table may include the title "TOTAL ORDER" as its own row/cell
                for r in rm[:8]:
                    if not isinstance(r, list):
                        continue
                    row_text = " ".join([str(c or "").strip() for c in r]).upper()
                    if "TOTAL ORDER" in row_text:
                        return True

                # Find a header row that contains size tokens; COLOUR/TOTAL may be OCR'd poorly or missing
                for r in rm[:8]:
                    if not isinstance(r, list):
                        continue
                    cells = [str(c or "").strip() for c in r]
                    row_text = " ".join(cells).upper()
                    hits = sum(1 for t in size_tokens if re.search(r"\b" + re.escape(t) + r"\b", row_text))
                    has_colour = ("COLOUR" in row_text) or ("COLOR" in row_text) or ("COLO" in row_text)
                    has_total = re.search(r"\bT\s*O?\s*T\s*A\s*L\b", row_text) is not None
                    if hits >= 2 and (has_colour or has_total):
                        return True
                    # If very strong size header, accept even if colour/total not found
                    if hits >= 3:
                        return True
                return False

            def _extract_total_from_grid(rm: Any) -> Optional[int]:
                if not isinstance(rm, list):
                    return None
                def _looks_total_word(s: str) -> bool:
                    nk = _norm_key(s)
                    return nk in {"total", "totai", "t0tal", "tota1", "totaiorder", "totalorder"} or nk.endswith("total")

                # Prefer explicit TOTAL row last cell
                best_last_col: List[int] = []
                best_any_num: List[int] = []

                for r in rm:
                    if not isinstance(r, list) or not r:
                        continue
                    cells = [" ".join(str(c or "").split()).strip() for c in r]
                    cells2 = [c for c in cells if c]
                    row_text = " ".join(cells2)
                    if not cells2:
                        continue

                    if any(_looks_total_word(c) for c in cells2[:2]) or ("TOTAL" in row_text.upper() and _looks_total_word(row_text)):
                        n = _parse_int_like(cells2[-1])
                        if n is not None:
                            return n

                    # collect numeric candidates from last column (robust)
                    n2 = _parse_int_like(cells2[-1])
                    if n2 is not None:
                        best_last_col.append(n2)
                    for c in cells2:
                        n3 = _parse_int_like(c)
                        if n3 is not None:
                            best_any_num.append(n3)

                if best_last_col:
                    return max(best_last_col)
                if best_any_num:
                    return max(best_any_num)
                return None

            # Table-based
            if isinstance(tables, list):
                for t in tables:
                    rm = (t or {}).get("rows_matrix") if isinstance(t, dict) else None
                    if not _is_total_order_grid(rm):
                        continue
                    n = _extract_total_from_grid(rm)
                    if n is not None:
                        merged["total_order"] = _format_int(n)
                        break

            # Text-based fallback
            if _field_bad("total_order", merged.get("total_order")) and isinstance(text, str) and text.strip():
                # 1) Inline pattern: TOTAL ORDER 27,800
                m = re.search(r"\btotal\s*order\b\s*[:\-]?\s*([0-9][0-9\.,]{2,})", text, flags=re.IGNORECASE)
                if m:
                    n = _parse_int_like(m.group(1) or "")
                    if n is not None:
                        merged["total_order"] = _format_int(n)

                # 2) Window scan: if PP-Structure didn't return the grid as table, the numbers may still appear in OCR text
                if _field_bad("total_order", merged.get("total_order")):
                    m2 = re.search(r"\btotal\s*order\b", text, flags=re.IGNORECASE)
                    if m2:
                        win = text[m2.end() : m2.end() + 800]
                        # Prefer window after a TOTAL row marker if present
                        m_tot = re.search(r"\btotal\b", win, flags=re.IGNORECASE)
                        if m_tot:
                            win2 = win[m_tot.end() : m_tot.end() + 400]
                        else:
                            win2 = win

                        nums: List[int] = []
                        for tok in re.findall(r"\b\d{1,3}(?:[\.,]\d{3})+\b|\b\d{4,12}\b", win2):
                            n3 = _parse_int_like(tok)
                            if n3 is not None:
                                nums.append(n3)
                        if nums:
                            merged["total_order"] = _format_int(max(nums))
        except Exception:
            pass

    # CARE INSTRUCTIONS: often appears on the same line as the label, or as a 2-column row (label | value)
    if _field_bad("care_instructions", merged.get("care_instructions")):
        try:
            def _format_care_info(s: str) -> str:
                out = " ".join((s or "").replace("\r", "\n").replace("\n", " ").split()).strip()
                out = re.sub(r"\s*\|\s*", " | ", out)
                out = re.sub(r"\s*,\s*", ", ", out)
                return out.strip(" ,")

            def _format_care_join(parts: List[str]) -> str:
                cleaned = []
                seen = set()
                for p in parts:
                    pp = _format_care_info(p)
                    if not pp:
                        continue
                    k = pp.upper()
                    if k in seen:
                        continue
                    seen.add(k)
                    cleaned.append(pp)
                return " || ".join(cleaned).strip(" ,|")

            def _care_keywords_hit(s: str) -> bool:
                return (
                    re.search(
                        r"\b(HAND\s*WASH|WASH(?!ED)|DO\s*NOT\s*BLEACH|BLEACH|DO\s*NOT\s*IRON|IRON|DO\s*NOT\s*DRY\s*CLEAN|DRY\s*CLEAN|DO\s*NOT\s*TUMBLE\s*DRY|TUMBLE\s*DRY)\b",
                        s or "",
                        flags=re.IGNORECASE,
                    )
                    is not None
                )

            def _care_structure_hit(s: str) -> bool:
                # Some OCR outputs keep the care format separators but lose keywords.
                # Accept if it looks like a care list: multiple '|' separators and/or temperature patterns.
                ss = (s or "").strip()
                if not ss:
                    return False
                pipe_count = ss.count("|")
                has_temp = re.search(r"\b\d{1,3}\s*(?:°\s*)?(?:C|F)\b", ss, flags=re.IGNORECASE) is not None
                has_fraction = re.search(r"\b\d{1,3}\s*/\s*\d{1,3}\b", ss) is not None
                # Require some alphabetic content to avoid taking pure noise
                alpha = len(re.findall(r"[A-Z]", ss.upper()))
                return (pipe_count >= 4) or (has_temp and (pipe_count >= 2 or has_fraction) and alpha >= 8)

            def _care_accept(s: str) -> bool:
                return _care_keywords_hit(s) or _care_structure_hit(s)

            def _care_to_double_pipe(s: str) -> str:
                # Convert single-pipe separated lists to the requested ' || ' format
                raw = " ".join((s or "").replace("\r", "\n").replace("\n", " ").split()).strip()
                raw = re.sub(r"\s*,\s*", ", ", raw)
                raw = raw.strip(" ,")
                if not raw:
                    return raw

                # Split on one-or-more pipes (handles both '|' and '||' from PDFs)
                parts = [p.strip() for p in re.split(r"\s*\|+\s*", raw) if p.strip()]
                if len(parts) >= 2:
                    return _format_care_join(parts)

                # No pipe list; keep as normalized text (but don't introduce pipe noise)
                return raw

            # 1) Text: inline value after label
            if isinstance(text, str) and text.strip():
                m = re.search(
                    r"\bcare\s*instructions\b\s*[:\-]?\s*(.+)$",
                    text,
                    flags=re.IGNORECASE | re.MULTILINE,
                )
                if m:
                    v = _format_care_info(m.group(1) or "")
                    if v and len(v) >= 6 and _care_accept(v) and _field_bad("care_instructions", merged.get("care_instructions")):
                        merged["care_instructions"] = _care_to_double_pipe(v)

                # Sometimes OCR puts the label on its own line then the value on the next line
                if _field_bad("care_instructions", merged.get("care_instructions")):
                    lines_ci = [ln.strip() for ln in text.replace("\r", "\n").split("\n")]
                    for i, ln in enumerate(lines_ci):
                        if re.search(r"\bcare\s*instructions\b", ln, flags=re.IGNORECASE):
                            tail = re.sub(r"(?i).*\bcare\s*instructions\b\s*[:\-]?", "", ln).strip()
                            parts: List[str] = []
                            if tail:
                                parts.append(tail)
                            for j in range(i + 1, min(len(lines_ci), i + 8)):
                                nxt = (lines_ci[j] or "").strip()
                                if not nxt:
                                    if parts:
                                        break
                                    continue
                                # stop if next label starts
                                if re.search(
                                    r"\b(COMPOSITIONS\s+INFORMATION|HANGTAG\s+LABEL|MAIN\s+LABEL|EXTERNAL\s+FABRIC|HANGING|TOTAL\s+ORDER|ORDER\-?NR|DATE|SUPPLIER|ARTICLE|DESCRIPTION|MARKET\s+OF\s+ORIGIN|PVP)\b",
                                    nxt,
                                    flags=re.IGNORECASE,
                                ) is not None:
                                    break
                                parts.append(nxt)
                            vv = _format_care_info(" ".join(parts))
                            if vv and len(vv) >= 6 and _care_accept(vv):
                                merged["care_instructions"] = _care_to_double_pipe(vv)
                            break

            # 2) Tables: detect CARE INSTRUCTIONS cell and read adjacent / following rows
            if _field_bad("care_instructions", merged.get("care_instructions")) and isinstance(tables, list):
                def _is_stop_cell(s: str) -> bool:
                    return re.search(
                        r"\b(COMPOSITIONS\s+INFORMATION|HANGTAG\s+LABEL|MAIN\s+LABEL|EXTERNAL\s+FABRIC|HANGING|TOTAL\s+ORDER|ORDER\-?NR|DATE|SUPPLIER|ARTICLE|DESCRIPTION|MARKET\s+OF\s+ORIGIN|PVP)\b",
                        s or "",
                        flags=re.IGNORECASE,
                    ) is not None

                for t in tables:
                    rm = (t or {}).get("rows_matrix") if isinstance(t, dict) else None
                    if not isinstance(rm, list):
                        continue
                    for ridx, r in enumerate(rm):
                        if not isinstance(r, list) or not r:
                            continue
                        cells = [" ".join(str(c or "").split()).strip() for c in r]
                        # find label cell index
                        cidx = None
                        for ii, c in enumerate(cells):
                            if re.search(r"\bcare\s*instructions\b", c or "", flags=re.IGNORECASE):
                                cidx = ii
                                break
                        if cidx is None:
                            continue

                        parts: List[str] = []
                        # same-row tail after label
                        tail = re.sub(r"(?i).*\bcare\s*instructions\b\s*[:\-]?", "", cells[cidx] or "").strip()
                        if tail:
                            parts.append(tail)

                        # same row: collect other non-empty cells (likely value column)
                        for ii, c in enumerate(cells):
                            if ii == cidx:
                                continue
                            if not c:
                                continue
                            if _is_stop_cell(c):
                                continue
                            parts.append(c)

                        # following rows: often the value continues beneath the value column
                        for rr in range(ridx + 1, min(len(rm), ridx + 6)):
                            nr = rm[rr]
                            if not isinstance(nr, list):
                                break
                            ncells = [" ".join(str(c or "").split()).strip() for c in nr]
                            row_text = " ".join([c for c in ncells if c])
                            if not row_text:
                                if parts:
                                    break
                                continue
                            if _is_stop_cell(row_text):
                                break
                            if re.search(r"\bcare\s*instructions\b", row_text, flags=re.IGNORECASE):
                                continue
                            # take value-column cell if available else whole row
                            vv = ""
                            if cidx < len(ncells):
                                vv = (ncells[cidx] or "").strip()
                            if not vv:
                                vv = row_text
                            if vv and not _is_stop_cell(vv):
                                parts.append(vv)

                        vv2 = _format_care_info(" ".join([p for p in parts if p]).strip())
                        if vv2 and len(vv2) >= 6 and _care_accept(vv2):
                            merged["care_instructions"] = _care_to_double_pipe(vv2)
                            break
                    if not _field_bad("care_instructions", merged.get("care_instructions")):
                        break

            # 3) Fallback: keyword-based capture near CARE INSTRUCTIONS anchor (handles noisy inline OCR)
            if _field_bad("care_instructions", merged.get("care_instructions")) and isinstance(text, str) and text.strip():
                lines_ci2 = [ln.strip() for ln in text.replace("\r", "\n").split("\n")]
                anchor_idx: Optional[int] = None
                for i, ln in enumerate(lines_ci2):
                    if re.search(r"\bcare\s*instructions\b", ln, flags=re.IGNORECASE):
                        anchor_idx = i
                        break

                parts2: List[str] = []
                if anchor_idx is not None:
                    tail = re.sub(r"(?i).*\bcare\s*instructions\b\s*[:\-]?", "", lines_ci2[anchor_idx]).strip()
                    if tail and _care_accept(tail):
                        parts2.append(tail)
                    for j in range(anchor_idx + 1, min(len(lines_ci2), anchor_idx + 25)):
                        ln = (lines_ci2[j] or "").strip()
                        if not ln:
                            if parts2:
                                break
                            continue
                        if re.search(
                            r"\b(COMPOSITIONS\s+INFORMATION|HANGTAG\s+LABEL|MAIN\s+LABEL|EXTERNAL\s+FABRIC|HANGING|TOTAL\s+ORDER|ORDER\-?NR|DATE|SUPPLIER|ARTICLE|DESCRIPTION|MARKET\s+OF\s+ORIGIN|PVP)\b",
                            ln,
                            flags=re.IGNORECASE,
                        ) is not None:
                            break
                        if _care_accept(ln) or re.search(r"\bDO\s*NOT\b", ln, flags=re.IGNORECASE) is not None:
                            parts2.append(ln)
                else:
                    for ln in lines_ci2:
                        if _care_accept(ln) or re.search(r"\bDO\s*NOT\b", ln, flags=re.IGNORECASE) is not None:
                            parts2.append(ln)

                vv3 = _format_care_join(parts2)
                if vv3 and len(vv3) >= 6:
                    merged["care_instructions"] = _care_to_double_pipe(vv3)
        except Exception:
            pass

    # Final normalization: ensure compositions_information is comma-separated for downstream parsing
    if isinstance(merged.get("compositions_information"), str):
        try:
            merged["compositions_information"] = _format_compositions_info(merged.get("compositions_information") or "")
        except Exception:
            pass

    # Backward-compatible alias: clients may use order_nr
    if "order_no" in merged and "order_nr" not in merged:
        merged["order_nr"] = merged.get("order_no")
    elif "order_nr" in merged and "order_no" not in merged:
        merged["order_no"] = merged.get("order_nr")

    return merged


def _postprocess_ocr_text(text: str) -> str:
    t = (text or "").replace("\r", "\n")
    if not t.strip():
        return (text or "").strip()

    def _fix_line(line: str) -> str:
        s = line
        s = re.sub(r",(?=\S)", ", ", s)
        s = re.sub(r"(?<=\S)\((?=\S)", " (", s)
        s = re.sub(r"(?<=\S)\)(?=\S)", ") ", s)

        # Temperature degree symbol reconstruction
        # Examples: "30 C" -> "30°C", "230 F" -> "230°F"
        s = re.sub(r"\b(\d{1,3})\s*(?:°\s*)?C\b", r"\1°C", s)
        s = re.sub(r"\b(\d{1,3})\s*(?:°\s*)?F\b", r"\1°F", s)

        # Separate common ALLCAPS glued tokens (keep codes with hyphen/slash intact)
        s = re.sub(r"\bTOTAL\s*ORDER\b", "TOTAL ORDER", s, flags=re.IGNORECASE)
        s = re.sub(r"\bTOTALORDER\b", "TOTAL ORDER", s, flags=re.IGNORECASE)
        s = re.sub(r"\bPAYMENT\s*TERMS\b", "PAYMENT TERMS", s, flags=re.IGNORECASE)
        s = re.sub(r"\bPAYMENTTERMS\b", "PAYMENT TERMS", s, flags=re.IGNORECASE)
        s = re.sub(r"\bMARKETOF\b", "MARKET OF", s, flags=re.IGNORECASE)
        s = re.sub(r"\bTAX\s*OFFICE\s*NUMBER\b", "Tax office number", s, flags=re.IGNORECASE)

        # EMEAAspire -> EMEA Aspire
        s = re.sub(r"\b([A-Z]{2,})([A-Z][a-z])", r"\1 \2", s)

        # Insert spaces between letters and digits
        s = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", s)
        s = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", s)

        # Normalize spaces but preserve newlines by operating per line
        s = re.sub(r"[\t ]+", " ", s)
        return s.strip()

    lines = t.split("\n")
    fixed_lines = [_fix_line(ln) for ln in lines]
    # Keep blank lines, but cap very long blank runs
    out = "\n".join(fixed_lines)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


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


def _extract_care_instructions_from_crop(image_bgr: np.ndarray, lines: Any) -> Dict[str, Any]:
    debug: Dict[str, Any] = {
        "label_found": False,
        "crop_bbox": None,
        "crop_ocr_text": None,
        "value": None,
    }
    try:
        if not isinstance(lines, list) or not lines:
            return debug

        label_bbox: Optional[Dict[str, float]] = None
        for l in lines:
            if not isinstance(l, dict):
                continue
            txt = str(l.get("text") or "").strip()
            if not txt:
                continue
            if re.search(r"\bcare\s*instructions\b", txt, flags=re.IGNORECASE) is None:
                continue
            poly = l.get("polygon")
            if not poly:
                continue
            label_bbox = _polygon_to_bbox(poly)
            break

        if label_bbox is None:
            return debug
        debug["label_found"] = True

        h, w = _ensure_bgr(image_bgr).shape[:2]
        x0 = max(0, int(label_bbox["x"] - 12))
        x1 = min(w, int(label_bbox.get("x2", label_bbox["x"] + label_bbox["w"]) + 900))
        y0 = max(0, int(label_bbox["y"] - 8))
        # extend more vertically; care sentence can be lower than the label
        y1 = min(h, int(label_bbox.get("y2", label_bbox["y"] + label_bbox["h"]) + 260))
        if x1 <= x0 or y1 <= y0:
            return debug

        debug["crop_bbox"] = {"x0": x0, "y0": y0, "x1": x1, "y1": y1}
        crop = _ensure_bgr(image_bgr)[y0:y1, x0:x1]
        if crop.size == 0:
            return debug

        scale = 2.5
        crop_up = cv2.resize(crop, dsize=None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

        ocr = _get_paddle_ocr()
        res = ocr.ocr(crop_up, cls=True)
        texts: List[str] = []
        for item in (res or []):
            for line in item:
                _box, (t, _conf) = line
                if t:
                    texts.append(str(t))
        if not texts:
            return debug

        joined = "\n".join(texts)
        joined = _postprocess_ocr_text(joined)
        debug["crop_ocr_text"] = joined[:800]

        # Try to extract a care list from the crop OCR output
        parts: List[str] = []
        for ln in [x.strip() for x in joined.replace("\r", "\n").split("\n")]:
            if not ln:
                continue
            if re.search(r"\b(HAND\s*WASH|WASH(?!ED)|DO\s*NOT|BLEACH|IRON|DRY\s*CLEAN|TUMBLE\s*DRY)\b", ln, flags=re.IGNORECASE) is not None:
                parts.append(ln)

        if not parts:
            # Sometimes crop OCR yields a single line with pipes
            for ln in [x.strip() for x in joined.replace("\r", "\n").split("\n")]:
                if ln.count("|") >= 2:
                    parts.append(ln)
                    break

        if not parts:
            return debug

        raw = " ".join(parts)
        raw = " ".join(raw.split()).strip()
        segs = [p.strip() for p in re.split(r"\s*\|+\s*", raw) if p.strip()]
        if len(segs) >= 2:
            # Dedup while preserving order
            out: List[str] = []
            seen = set()
            for s in segs:
                k = s.upper()
                if k in seen:
                    continue
                seen.add(k)
                out.append(s)
            debug["value"] = " || ".join(out).strip(" ,|")
            return debug

        debug["value"] = raw
        return debug
    except Exception as e:
        debug["error"] = str(e)
        return debug


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


def ocr_extract_sync(payload: Dict[str, Any]) -> Dict[str, Any]:
    t0 = time.perf_counter()
    request_id = str(payload.get("request_id") or "").strip() or str(uuid.uuid4())
    warnings: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    filename = str(payload.get("filename") or "uploaded")
    engine = payload.get("engine") or "tesseract"
    preprocess = bool(payload.get("preprocess", True))
    preprocess_mode = payload.get("preprocess_mode") or "basic"
    file_b64 = payload.get("file_b64")

    if not isinstance(file_b64, str) or not file_b64:
        raise ValueError("Missing file_b64")

    try:
        file_bytes = base64.b64decode(file_b64.encode("utf-8"), validate=False)
    except Exception as e:
        raise ValueError(f"Invalid file_b64: {e}")

    if not file_bytes:
        raise ValueError("Empty file")

    pages_text: Optional[List[str]] = None

    so_dbg_tables = str(os.environ.get("SO_DEBUG_PDF_TABLES") or "").strip().lower() in {"1", "true", "yes", "on"}

    is_pdf = filename.lower().endswith(".pdf") or _looks_like_pdf_bytes(file_bytes)

    if is_pdf:
        pages_text = _pdf_text_pages(file_bytes)
        joined = "\n\n".join([t for t in (pages_text or []) if (t or "").strip()]).strip()
        page_count = len(pages_text or [])
        pdf_tables_pages = _pdf_tables_pages_tabula(file_bytes, page_count) if page_count else None
        has_pdf_tables = any((isinstance(p, list) and len(p) > 0) for p in (pdf_tables_pages or []))

        if so_dbg_tables:
            try:
                per_page_counts = [len(p or []) if isinstance(p, list) else 0 for p in (pdf_tables_pages or [])]
                logger.info(
                    "so_pdf_fastpath_input %s",
                    json.dumps(
                        {
                            "event": "so_pdf_fastpath_input",
                            "request_id": request_id,
                            "doc_filename": filename,
                            "page_count": page_count,
                            "joined_len": len(joined or ""),
                            "has_pdf_tables": bool(has_pdf_tables),
                            "tabula_tables_per_page": per_page_counts,
                        },
                        ensure_ascii=False,
                    ),
                )
            except Exception:
                pass

        if so_dbg_tables:
            try:
                per_page_counts = [len(p or []) if isinstance(p, list) else 0 for p in (pdf_tables_pages or [])]
                _payload = {
                    "event": "so_pdf_fastpath_input",
                    "request_id": request_id,
                    "doc_filename": filename,
                    "page_count": page_count,
                    "joined_len": len(joined or ""),
                    "has_pdf_tables": bool(has_pdf_tables),
                    "tabula_tables_per_page": per_page_counts,
                }
                logger.info("so_pdf_fastpath_input %s", json.dumps(_payload, ensure_ascii=False))
            except Exception:
                pass

        # Digital-PDF fast path:
        # - If embedded text exists, we can extract header fields from text.
        # - If Tabula can extract tables, we can also parse TOTAL ORDER / partial deliveries without OCR.
        if ((joined and len(joined) >= 50) or has_pdf_tables):
            t_pdf = time.perf_counter()
            all_pages: List[Dict[str, Any]] = []
            combined_texts: List[str] = []
            for i, t in enumerate(pages_text or [], start=1):
                pp_text = _postprocess_ocr_text(t or "")
                page_tables: List[Dict[str, Any]] = []
                if isinstance(pdf_tables_pages, list) and i - 1 < len(pdf_tables_pages):
                    for tt in (pdf_tables_pages[i - 1] or []):
                        if isinstance(tt, dict) and tt.get("headers") and tt.get("rows"):
                            page_tables.append(tt)
                pp_fields = _extract_fields_smart(pp_text, page_tables)
                page_res: Dict[str, Any] = {
                    "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
                    "page": i,
                    "text": pp_text,
                    "tables": page_tables,
                    "fields": pp_fields,
                    "field_pairs": _fields_to_pairs(pp_fields),
                    "preprocess": {"enabled": False, "target": "pdf_text"},
                }
                try:
                    page_res["tables"] = [_build_ai_kv_table_from_fields(pp_fields)] + (page_res.get("tables") or [])
                except Exception:
                    warnings.append({"page": i, "code": "AI_TABLE_BUILD_FAILED", "message": "Failed to build AI key/value table"})
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
                        tt = _table_add_rows_matrix(tt)
                        if str(tt.get("table_kind") or "").strip().lower() in {"total_order_grid", "partial_deliveries_grid"}:
                            try:
                                _normalize_size_grid_columns(tt)
                            except Exception as e:
                                if so_dbg_tables:
                                    try:
                                        rows0 = tt.get("rows") or []
                                        row_keys = list((rows0[0] or {}).keys())[:12] if isinstance(rows0, list) and rows0 and isinstance(rows0[0], dict) else None
                                        logger.info(
                                            "so_pdf_fastpath_normalize_failed",
                                            extra={
                                                "request_id": request_id,
                                                "page": tt.get("page"),
                                                "table_kind": str(tt.get("table_kind") or ""),
                                                "headers": [str(h or "") for h in (tt.get("headers") or [])[:12]],
                                                "row0_keys": row_keys,
                                                "error": str(e),
                                            },
                                        )
                                    except Exception:
                                        pass
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

            combined_tables = _filter_tables_for_sales_order(combined_tables)
            combined_tables = [_table_add_rows_matrix(t) for t in combined_tables]
            combined_tables = _filter_tables_for_sales_order(combined_tables)
            try:
                for t in combined_tables:
                    if not isinstance(t, dict):
                        continue
                    if str(t.get("table_kind") or "").strip().lower() in {"total_order_grid", "partial_deliveries_grid"}:
                        try:
                            _normalize_size_grid_columns(t)
                        except Exception as e:
                            if so_dbg_tables:
                                try:
                                    rows0 = t.get("rows") or []
                                    row_keys = list((rows0[0] or {}).keys())[:12] if isinstance(rows0, list) and rows0 and isinstance(rows0[0], dict) else None
                                    logger.info(
                                        "so_pdf_fastpath_normalize_failed %s",
                                        json.dumps(
                                            {
                                                "event": "so_pdf_fastpath_normalize_failed",
                                                "request_id": request_id,
                                                "page": t.get("page"),
                                                "table_kind": str(t.get("table_kind") or ""),
                                                "headers": [str(h or "") for h in (t.get("headers") or [])[:12]],
                                                "row0_keys": row_keys,
                                                "error": str(e),
                                            },
                                            ensure_ascii=False,
                                        ),
                                    )
                                except Exception:
                                    pass
            except Exception:
                pass
            combined_tables = _dedup_top_level_ai_kv_tables(combined_tables)

            # Fallback: parse TOTAL ORDER from embedded text when Tabula tables are missing/misclassified.
            try:
                payload0 = _build_sales_order_payload(combined_tables)
                grid0 = ((payload0.get("total_order") or {}).get("grid")) if isinstance(payload0, dict) else None
                if not (isinstance(grid0, list) and len(grid0) > 0):
                    parsed = _parse_total_order_from_text("\n".join(combined_texts))
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
                        combined_tables = [_table_add_rows_matrix(t) for t in combined_tables]
                        try:
                            for t in combined_tables:
                                if isinstance(t, dict) and str(t.get("table_kind") or "").strip().lower() == "total_order_grid":
                                    _normalize_size_grid_columns(t)
                        except Exception:
                            pass
                        if so_dbg_tables:
                            try:
                                _payload = {
                                    "event": "so_pdf_fastpath_total_order_text_fallback_used",
                                    "request_id": request_id,
                                    "row_count": len(parsed.get("rows") or []),
                                }
                                logger.info("so_pdf_fastpath_total_order_text_fallback_used %s", json.dumps(_payload, ensure_ascii=False))
                            except Exception:
                                pass
                    elif so_dbg_tables:
                        try:
                            snippet = "\n".join((combined_texts or [])[:1])
                            logger.info(
                                "so_pdf_fastpath_total_order_text_fallback_none %s",
                                json.dumps(
                                    {
                                        "event": "so_pdf_fastpath_total_order_text_fallback_none",
                                        "request_id": request_id,
                                        "note": "_parse_total_order_from_text returned None",
                                        "text_head": (snippet or "")[:800],
                                    },
                                    ensure_ascii=False,
                                ),
                            )
                        except Exception:
                            pass
            except Exception:
                pass

            if so_dbg_tables:
                try:
                    kinds: Dict[str, int] = {}
                    for t in combined_tables:
                        if not isinstance(t, dict):
                            continue
                        k = str(t.get("table_kind") or "").strip() or "(none)"
                        kinds[k] = int(kinds.get(k, 0)) + 1
                    payload_dbg = _build_sales_order_payload(combined_tables)
                    grid_len = len(((payload_dbg.get("total_order") or {}).get("grid")) or []) if isinstance(payload_dbg, dict) else None
                    _payload = {
                        "event": "so_pdf_fastpath_summary",
                        "request_id": request_id,
                        "table_kinds": kinds,
                        "total_order_grid_len": grid_len,
                    }
                    logger.info("so_pdf_fastpath_summary %s", json.dumps(_payload, ensure_ascii=False))
                except Exception:
                    pass

            dt = time.perf_counter() - t0
            logger.info(
                "ocr_extract_sync_done",
                extra={
                    "request_id": request_id,
                    "doc_filename": filename,
                    "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
                    "page_count": len(all_pages),
                    "duration_sec": round(dt, 4),
                    "pdf_text_sec": round(time.perf_counter() - t_pdf, 4),
                },
            )
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
                "sales_order_payload": _build_sales_order_payload(combined_tables),
            }

    images_bgr = _images_from_upload(filename, file_bytes)

    all_pages: List[Dict[str, Any]] = []
    combined_texts: List[str] = []
    for page_idx, img_bgr in enumerate(images_bgr, start=1):
        t_page0 = time.perf_counter()
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

        if engine == "tesseract":
            page_res = _run_tesseract(input_for_ocr)
        elif engine == "paddle_structure":
            page_res = _run_paddle_structure(_ensure_bgr(input_for_ocr))
            page_res["avg_confidence"] = None
            page_res["text"] = _postprocess_ocr_text(page_res.get("text") or "")
            page_res["fields"] = _extract_fields_smart(page_res.get("text") or "", page_res.get("tables") or [])
        elif engine == "paddle_ensemble":
            bgr = _ensure_bgr(input_for_ocr)
            struct_res = _run_paddle_structure(bgr)
            paddle_res = _run_paddle(bgr)
            merged_text = _merge_text(struct_res.get("text") or "", paddle_res.get("text") or "", paddle_res.get("avg_confidence"))
            merged_text = _postprocess_ocr_text(merged_text)
            page_res = {
                "engine": "paddle_ensemble",
                "layout": struct_res.get("layout") or [],
                "tables": struct_res.get("tables") or [],
                "text": merged_text,
                "avg_confidence": paddle_res.get("avg_confidence"),
                "lines": paddle_res.get("lines") or [],
            }
            if not page_res.get("tables"):
                page_res["tables"] = _extract_tables_from_paddle_page(image_for_tables, {"lines": page_res.get("lines")})
            page_res["fields"] = _extract_fields_smart(merged_text, page_res.get("tables") or [])
        else:
            page_res = _run_paddle(_ensure_bgr(input_for_ocr))

        page_res["page"] = page_idx
        page_res["preprocess"] = prep_meta

        if engine == "paddle":
            page_res["tables"] = _extract_tables_from_paddle_page(image_for_tables, page_res)
            page_res["text"] = _postprocess_ocr_text(page_res.get("text") or "")
            page_res["fields"] = _extract_fields_smart(page_res.get("text") or "", page_res.get("tables") or [])

        # CARE INSTRUCTIONS crop re-OCR fallback for paddle-based engines
        try:
            if engine in ("paddle", "paddle_ensemble"):
                fields0 = page_res.get("fields") or {}
                care0 = fields0.get("care_instructions") if isinstance(fields0, dict) else None

                def _care_needs_crop(v: Any) -> bool:
                    if not isinstance(v, str) or not v.strip():
                        return True
                    s2 = re.sub(r"\s+", " ", v).strip()
                    has_kw = re.search(
                        r"\b(HAND\s*WASH|WASH(?!ED)|DO\s*NOT|BLEACH|IRON|DRY\s*CLEAN|TUMBLE\s*DRY)\b",
                        s2,
                        flags=re.IGNORECASE,
                    ) is not None
                    compact = re.sub(r"[^A-Z0-9]", "", s2.upper())
                    if not has_kw and re.search(r"(O|0|N){6,}", compact) is not None:
                        return True
                    return False

                if _care_needs_crop(care0):
                    # IMPORTANT: crop from the same image used for Paddle OCR so polygon coordinates align.
                    care_dbg = _extract_care_instructions_from_crop(_ensure_bgr(input_for_ocr), page_res.get("lines"))
                    page_res["care_crop_debug"] = {k: v for k, v in (care_dbg or {}).items() if k in {"label_found", "crop_bbox", "crop_ocr_text", "error"}}
                    care_crop = (care_dbg or {}).get("value") if isinstance(care_dbg, dict) else None
                    if isinstance(care_crop, str) and care_crop.strip() and isinstance(fields0, dict):
                        fields0["care_instructions"] = care_crop.strip()
                        page_res["fields"] = fields0
        except Exception:
            pass

        try:
            if filename.lower().endswith(".pdf") and pages_text and engine in ("paddle", "paddle_ensemble"):
                fields0 = page_res.get("fields") or {}
                care0 = fields0.get("care_instructions") if isinstance(fields0, dict) else None
                pdf_dbg: Dict[str, Any] = {
                    "has_pages_text": bool(pages_text),
                    "page_idx": page_idx,
                    "matched": False,
                    "snippet": None,
                }

                def _care_needs_pdf(v: Any) -> bool:
                    if not isinstance(v, str) or not v.strip():
                        return True
                    s2 = re.sub(r"\s+", " ", v).strip()
                    has_kw = re.search(
                        r"\b(HAND\s*WASH|WASH(?!ED)|DO\s*NOT|BLEACH|IRON|DRY\s*CLEAN|TUMBLE\s*DRY)\b",
                        s2,
                        flags=re.IGNORECASE,
                    ) is not None
                    compact = re.sub(r"[^A-Z0-9]", "", s2.upper())
                    if not has_kw and re.search(r"(O|0|N){6,}", compact) is not None:
                        return True
                    return False

                if _care_needs_pdf(care0) and page_idx - 1 < len(pages_text):
                    t_pdf_page = _postprocess_ocr_text(pages_text[page_idx - 1] or "")
                    care_kw = re.compile(r"\b(HAND\s*WASH|WASH(?!ED)|DO\s*NOT|BLEACH|IRON|DRY\s*CLEAN|TUMBLE\s*DRY)\b", flags=re.IGNORECASE)

                    def _care_structure_hit_pdf(s: str) -> bool:
                        ss = (s or "").strip()
                        if not ss:
                            return False
                        pipe_count = ss.count("|")
                        has_temp = re.search(r"\b\d{1,3}\s*(?:°\s*)?(?:C|F)\b", ss, flags=re.IGNORECASE) is not None
                        has_fraction = re.search(r"\b\d{1,3}\s*/\s*\d{1,3}\b", ss) is not None
                        alpha = len(re.findall(r"[A-Z]", ss.upper()))
                        return (pipe_count >= 4) or (has_temp and (pipe_count >= 2 or has_fraction) and alpha >= 8)

                    def _to_double_pipe(raw: str) -> str:
                        r0 = " ".join((raw or "").replace("\r", "\n").replace("\n", " ").split()).strip().strip(" ,")
                        if not r0:
                            return r0
                        segs0 = [p.strip() for p in re.split(r"\s*\|+\s*", r0) if p.strip()]
                        if len(segs0) >= 2:
                            out0: List[str] = []
                            seen0 = set()
                            for s0 in segs0:
                                k0 = s0.upper()
                                if k0 in seen0:
                                    continue
                                seen0.add(k0)
                                out0.append(s0)
                            return " || ".join(out0).strip(" ,|")
                        return r0

                    # A) Inline capture on same line as label
                    m = re.search(
                        r"\bcare\s*instructions\b\s*[:\-]?\s*(.+)$",
                        t_pdf_page,
                        flags=re.IGNORECASE | re.MULTILINE,
                    )

                    raw_candidate: Optional[str] = None
                    if m and (m.group(1) or "").strip():
                        raw_candidate = _to_double_pipe(m.group(1) or "")

                    # B) Multi-line capture after the label (common in embedded PDF text)
                    if not raw_candidate:
                        lines_pdf = [ln.strip() for ln in t_pdf_page.replace("\r", "\n").split("\n")]
                        start_i: Optional[int] = None
                        for ii, ln in enumerate(lines_pdf):
                            if re.search(r"\bcare\s*instructions\b", ln, flags=re.IGNORECASE):
                                start_i = ii
                                tail = re.sub(r"(?i).*\bcare\s*instructions\b\s*[:\-]?", "", ln).strip()
                                parts_pdf: List[str] = []
                                if tail:
                                    parts_pdf.append(tail)
                                stop_pat = re.compile(
                                    r"\b(COMPOSITIONS\s+INFORMATION|HANGTAG\s+LABEL|MAIN\s+LABEL|EXTERNAL\s+FABRIC|HANGING|TOTAL\s+ORDER|ORDER\-?NR|DATE|SUPPLIER|ARTICLE|DESCRIPTION|MARKET\s+OF\s+ORIGIN|PVP)\b",
                                    flags=re.IGNORECASE,
                                )
                                for jj in range(ii + 1, min(len(lines_pdf), ii + 12)):
                                    nxt = (lines_pdf[jj] or "").strip()
                                    if not nxt:
                                        if parts_pdf:
                                            break
                                        continue
                                    if stop_pat.search(nxt) is not None:
                                        break
                                    parts_pdf.append(nxt)
                                if parts_pdf:
                                    raw_candidate = _to_double_pipe(" ".join(parts_pdf))
                                break

                    if raw_candidate:
                        pdf_dbg["matched"] = True
                        pdf_dbg["snippet"] = raw_candidate[:300]
                        if care_kw.search(raw_candidate) is not None or _care_structure_hit_pdf(raw_candidate):
                            fields0["care_instructions"] = raw_candidate
                            page_res["fields"] = fields0

                page_res["care_pdf_debug"] = pdf_dbg
        except Exception:
            pass

        if isinstance(page_res.get("tables"), list):
            page_res["tables"] = _filter_tables_for_sales_order(page_res.get("tables") or [])
            page_res["tables"] = [_table_add_rows_matrix(t) for t in (page_res.get("tables") or [])]
            page_res["tables"] = _filter_tables_for_sales_order(page_res.get("tables") or [])

        try:
            if isinstance(page_res.get("fields"), dict):
                ai_tbl = _build_ai_kv_table_from_fields(page_res.get("fields"))
                page_res["tables"] = [ai_tbl] + (page_res.get("tables") or [])
        except Exception:
            warnings.append({"page": page_idx, "code": "AI_TABLE_BUILD_FAILED", "message": "Failed to build AI key/value table"})
            pass

        # Inject text-derived fields into AI key/value tables when table-based extraction missed them
        try:
            fields = page_res.get("fields") or {}
            if isinstance(fields, dict) and isinstance(page_res.get("tables"), list):
                field_to_key = {
                    "send_to": "SEND TO",
                    "purchaser": "PURCHASER",
                    "supplier": "SUPPLIER",
                    "payment_terms": "PAYMENT TERMS",
                    "article": "ARTICLE",
                    "description": "DESCRIPTION",
                    "market_of_origin": "MARKET OF ORIGIN",
                    "pvp": "PVP",
                }

                def _is_contaminated_value(key: str, cur: str) -> bool:
                    k = (key or "").strip().upper()
                    c = (cur or "").strip()
                    if not c:
                        return True
                    # Generic contamination
                    if re.fullmatch(r"COL_?\d+", c.strip(), flags=re.IGNORECASE):
                        return True
                    if re.search(r"\b(TOTAL|DRIGIN|MARKETOF)\b", c, flags=re.IGNORECASE):
                        return True
                    # If it still contains a label token (e.g. "1517 BUYER") it's likely wrong
                    try:
                        if "_split_value_label" in locals():
                            sp = _split_value_label(c)
                            if sp is not None and sp[0] and sp[1]:
                                return True
                    except Exception:
                        pass
                    if k == "SUPPLIER REF":
                        if re.search(r"\b(OUTER\s+SHELL|LINING|COMPOSITION|COMPOSITIONS|CARE|INSTRUCTIONS|TOTAL)\b", c, flags=re.IGNORECASE):
                            return True
                        if re.fullmatch(r"\d{2,4}\-?[A-Z]{2,}", c.replace(" ", "").upper()):
                            return True
                    if k == "MARKET OF ORIGIN":
                        if not re.search(r"\b[A-Z]{3,}\b", c):
                            return True
                    if k == "PVP":
                        if not (re.search(r"\b(EUR|USD|GBP)\b", c, flags=re.IGNORECASE) or re.search(r"\b\d+[\.,]\d{2}\b", c)):
                            return True
                    if k == "DESCRIPTION":
                        if re.search(r"\b(EUR|USD|GBP)\b", c, flags=re.IGNORECASE):
                            return True
                    return False

                def _should_override(key: str, cur: str, new_val: str) -> bool:
                    if not new_val or not str(new_val).strip():
                        return False
                    if not cur or not str(cur).strip():
                        return True
                    return _is_contaminated_value(key, str(cur))
                for tbl in page_res.get("tables") or []:
                    if not isinstance(tbl, dict):
                        continue
                    if tbl.get("headers") != ["key", "value"]:
                        continue

                    # Prefer kv_pairs_all if present
                    kv_all = tbl.get("kv_pairs_all")
                    if isinstance(kv_all, list):
                        changed = False
                        for p in kv_all:
                            if not isinstance(p, dict):
                                continue
                            k = str(p.get("key") or "").strip().upper()
                            if not k:
                                continue
                            for f_name, target_key in field_to_key.items():
                                if k == target_key:
                                    v = fields.get(f_name)
                                    if isinstance(v, str) and _should_override(k, str(p.get("value") or ""), v):
                                        p["value"] = v.strip()
                                        changed = True
                            # Force SUPPLIER REF blank if it looks contaminated
                            if k == "SUPPLIER REF" and _is_contaminated_value(k, str(p.get("value") or "")):
                                if str(p.get("value") or "").strip() != "":
                                    p["value"] = ""
                                    changed = True
                        if changed:
                            tbl["rows"] = [{"key": p.get("key"), "value": p.get("value")} for p in kv_all if isinstance(p, dict)]
                            tbl["rows_matrix"] = [[str(p.get("key") or ""), str(p.get("value") or "")] for p in kv_all if isinstance(p, dict)]
                        continue

                    # Fallback: mutate rows directly
                    rows = tbl.get("rows")
                    if isinstance(rows, list):
                        for r in rows:
                            if not isinstance(r, dict):
                                continue
                            k = str(r.get("key") or "").strip().upper()
                            if not k:
                                continue
                            for f_name, target_key in field_to_key.items():
                                if k == target_key and (r.get("value") is None or str(r.get("value") or "").strip() == ""):
                                    v = fields.get(f_name)
                                    if isinstance(v, str) and v.strip():
                                        r["value"] = v.strip()
                        tbl["rows_matrix"] = [[str(r.get("key") or ""), str(r.get("value") or "")] for r in rows if isinstance(r, dict)]
        except Exception:
            pass

        page_res["field_pairs"] = _fields_to_pairs(page_res.get("fields"))

        all_pages.append(page_res)
        combined_texts.append(_postprocess_ocr_text(page_res.get("text") or ""))

    combined_tables: List[Dict[str, Any]] = []
    for p in all_pages:
        for t in (p.get("tables") or []):
            tt = {"page": p.get("page"), **t}
            try:
                tt = _table_add_rows_matrix(tt)
            except Exception:
                pass
            combined_tables.append(tt)

    combined_tables = _dedup_top_level_ai_kv_tables(combined_tables)

    try:
        if str(os.getenv("DEBUG_TABLE_KIND_DUMP") or "").strip() in {"1", "true", "True", "YES", "yes"}:
            try:
                dump: List[Dict[str, Any]] = []
                for t in combined_tables:
                    if not isinstance(t, dict):
                        continue
                    rm = t.get("rows_matrix")
                    rm_head = rm[:6] if isinstance(rm, list) else None
                    dump.append(
                        {
                            "page": t.get("page"),
                            "table_index": t.get("table_index"),
                            "table_kind": t.get("table_kind"),
                            "headers": t.get("headers"),
                            "rows_matrix_head": rm_head,
                        }
                    )
                msg = json.dumps(dump, ensure_ascii=False)
                logger.info("DEBUG_TABLE_KIND_DUMP request_id=%s %s", request_id, msg)
                try:
                    chunk = 8000
                    total_parts = (len(msg) + chunk - 1) // chunk
                    print(f"DEBUG_TABLE_KIND_DUMP_PRINT request_id={request_id} BEGIN parts={total_parts}")
                    for i in range(0, len(msg), chunk):
                        part_no = i // chunk + 1
                        part = msg[i : i + chunk]
                        print(
                            f"DEBUG_TABLE_KIND_DUMP_PRINT request_id={request_id} part={part_no}/{total_parts} {part}"
                        )
                    print(f"DEBUG_TABLE_KIND_DUMP_PRINT request_id={request_id} END")
                    sys.stdout.flush()
                except Exception:
                    pass
            except Exception:
                pass
    except Exception:
        pass

    try:
        if str(os.getenv("DEBUG_PARTIAL_DELIVERIES") or "").strip() in {"1", "true", "True", "YES", "yes"}:
            try:
                dbg_sales = str(os.getenv("DEBUG_SALES_ORDER_PAYLOAD") or "").strip()
                print(
                    f"DEBUG_ENV_SNAPSHOT request_id={request_id} DEBUG_PARTIAL_DELIVERIES=1 DEBUG_SALES_ORDER_PAYLOAD={dbg_sales!r}"
                )
                sys.stdout.flush()
            except Exception:
                pass
            pd = []
            for t in combined_tables:
                if not isinstance(t, dict):
                    continue
                if str(t.get("table_kind") or "").strip().lower() != "partial_deliveries_grid":
                    continue
                pd.append(
                    {
                        "page": t.get("page"),
                        "table_index": t.get("table_index"),
                        "headers": t.get("headers"),
                        "pre_rows_matrix": t.get("pre_rows_matrix"),
                        "rows": t.get("rows"),
                    }
                )
            if pd:
                msg = json.dumps(pd, ensure_ascii=False)
                # Avoid extremely large logs
                if len(msg) > 20000:
                    msg = msg[:20000] + "...<truncated>"
                logger.info("DEBUG_PARTIAL_DELIVERIES request_id=%s %s", request_id, msg)
                try:
                    print(f"DEBUG_PARTIAL_DELIVERIES_PRINT request_id={request_id} {msg}")
                    sys.stdout.flush()
                except Exception:
                    pass
            else:
                logger.info("DEBUG_PARTIAL_DELIVERIES request_id=%s no partial_deliveries_grid tables", request_id)
                try:
                    print(f"DEBUG_PARTIAL_DELIVERIES_PRINT request_id={request_id} no partial_deliveries_grid tables")
                    sys.stdout.flush()
                except Exception:
                    pass
    except Exception:
        pass

    combined_fields: List[Dict[str, Any]] = []
    for p in all_pages:
        if p.get("fields"):
            combined_fields.append({"page": p.get("page"), **(p.get("fields") or {})})

    combined_field_pairs: List[Dict[str, Any]] = []
    for p in all_pages:
        for pair in (p.get("field_pairs") or []):
            if isinstance(pair, dict) and pair.get("key") is not None and pair.get("value") is not None:
                combined_field_pairs.append({"page": p.get("page"), **pair})

    dt = time.perf_counter() - t0
    logger.info(
        "ocr_extract_sync_done",
        extra={
            "request_id": request_id,
            "doc_filename": filename,
            "engine": engine,
            "page_count": len(all_pages),
            "duration_sec": round(dt, 4),
            "warnings": len(warnings),
            "errors": len(errors),
        },
    )
    return {
        "schema_version": "1.0",
        "document_meta": {
            "request_id": request_id,
            "filename": filename,
            "engine": engine,
            "preprocess": {"enabled": preprocess, "mode": preprocess_mode},
            "page_count": len(all_pages),
            "timings": {"total_sec": round(dt, 4)},
        },
        "warnings": warnings,
        "errors": errors,
        "filename": filename,
        "engine": engine,
        "pages": all_pages,
        "text": "\n\n".join(combined_texts).strip(),
        "tables": combined_tables,
        "fields": combined_fields,
        "field_pairs": combined_field_pairs,
    }


@app.post("/ocr/extract/async")
async def ocr_extract_async(
    request: Request,
    file: Optional[UploadFile] = File(None),
    engine: _ENGINE = Query("tesseract"),
    preprocess: bool = Query(True),
    preprocess_mode: _PREPROCESS_MODE = Query("basic"),
) -> JSONResponse:
    if ocr_extract_task is None:
        raise HTTPException(status_code=503, detail="Celery not available")

    if file is not None:
        filename = file.filename or "uploaded"
        file_bytes = await file.read()
    else:
        filename = request.headers.get("x-filename") or "uploaded"
        file_bytes = await request.body()

    if not file_bytes:
        raise HTTPException(status_code=400, detail="Empty file")

    payload = {
        "filename": filename,
        "engine": engine,
        "preprocess": preprocess,
        "preprocess_mode": preprocess_mode,
        "file_b64": base64.b64encode(file_bytes).decode("utf-8"),
    }

    job = ocr_extract_task.delay(payload)
    return JSONResponse({"task_id": job.id, "status": "PENDING"})


@app.get("/ocr/jobs/{task_id}")
def ocr_job_status(task_id: str) -> JSONResponse:
    if AsyncResult is None or celery_app is None:
        raise HTTPException(status_code=503, detail="Celery not available")

    res = AsyncResult(task_id, app=celery_app)
    status = str(res.status)
    if res.successful():
        return JSONResponse({"task_id": task_id, "status": status, "result": res.result})
    if res.failed():
        return JSONResponse({"task_id": task_id, "status": status, "error": str(res.result)})
    return JSONResponse({"task_id": task_id, "status": status})


@app.post("/ocr/extract")
async def ocr_extract(
    request: Request,
    file: Optional[UploadFile] = File(None),
    engine: _ENGINE = Query("tesseract"),
    preprocess: bool = Query(True),
    preprocess_mode: _PREPROCESS_MODE = Query("basic"),
) -> JSONResponse:
    t0 = time.perf_counter()
    request_id = request.headers.get("x-request-id") or request.headers.get("x-correlation-id") or str(uuid.uuid4())
    warnings: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
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

    so_dbg_tables = str(os.environ.get("SO_DEBUG_PDF_TABLES") or "").strip().lower() in {"1", "true", "yes", "on"}

    # If PDF has embedded text (selectable text), prefer extracting it directly.
    is_pdf = filename.lower().endswith(".pdf") or (isinstance(content_type, str) and "pdf" in content_type.lower()) or _looks_like_pdf_bytes(file_bytes)
    if is_pdf:
        # Normalize filename so downstream logic and logs clearly indicate PDF.
        if not filename.lower().endswith(".pdf"):
            filename = f"{filename}.pdf"
        pages_text = _pdf_text_pages(file_bytes)
        joined = "\n\n".join([t for t in (pages_text or []) if (t or "").strip()]).strip()
        page_count = len(pages_text or [])
        pdf_tables_pages = _pdf_tables_pages_tabula(file_bytes, page_count) if page_count else None
        has_pdf_tables = any((isinstance(p, list) and len(p) > 0) for p in (pdf_tables_pages or []))

        # Digital-PDF fast path (pdfplumber text + Tabula tables) for any engine.
        # Fall back to OCR if there is no usable embedded text and no tables.
        if ((joined and len(joined) >= 50) or has_pdf_tables):
            all_pages: List[Dict[str, Any]] = []
            combined_texts: List[str] = []
            for i, t in enumerate(pages_text or [], start=1):
                pp_text = _postprocess_ocr_text(t or "")
                page_tables: List[Dict[str, Any]] = []
                if isinstance(pdf_tables_pages, list) and i - 1 < len(pdf_tables_pages):
                    for tt in (pdf_tables_pages[i - 1] or []):
                        if isinstance(tt, dict) and tt.get("headers") and tt.get("rows"):
                            page_tables.append(tt)
                pp_fields = _extract_fields_smart(pp_text, page_tables)
                page_res: Dict[str, Any] = {
                    "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
                    "page": i,
                    "text": pp_text,
                    "tables": page_tables,
                    "fields": pp_fields,
                    "field_pairs": _fields_to_pairs(pp_fields),
                    "preprocess": {"enabled": False, "target": "pdf_text"},
                }
                try:
                    page_res["tables"] = [_build_ai_kv_table_from_fields(pp_fields)] + (page_res.get("tables") or [])
                except Exception:
                    warnings.append({"page": i, "code": "AI_TABLE_BUILD_FAILED", "message": "Failed to build AI key/value table"})
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
                        tt = _table_add_rows_matrix(tt)
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

            combined_tables = _filter_tables_for_sales_order(combined_tables)
            combined_tables = [_table_add_rows_matrix(t) for t in combined_tables]
            combined_tables = _filter_tables_for_sales_order(combined_tables)
            try:
                for t in combined_tables:
                    if not isinstance(t, dict):
                        continue
                    if str(t.get("table_kind") or "").strip().lower() in {"total_order_grid", "partial_deliveries_grid"}:
                        try:
                            _normalize_size_grid_columns(t)
                        except Exception as e:
                            if so_dbg_tables:
                                try:
                                    rows0 = t.get("rows") or []
                                    row_keys = list((rows0[0] or {}).keys())[:12] if isinstance(rows0, list) and rows0 and isinstance(rows0[0], dict) else None
                                    logger.info(
                                        "so_pdf_fastpath_normalize_failed %s",
                                        json.dumps(
                                            {
                                                "event": "so_pdf_fastpath_normalize_failed",
                                                "request_id": request_id,
                                                "page": t.get("page"),
                                                "table_kind": str(t.get("table_kind") or ""),
                                                "headers": [str(h or "") for h in (t.get("headers") or [])[:12]],
                                                "row0_keys": row_keys,
                                                "error": str(e),
                                            },
                                            ensure_ascii=False,
                                        ),
                                    )
                                except Exception:
                                    pass
            except Exception:
                pass
            combined_tables = _dedup_top_level_ai_kv_tables(combined_tables)

            # Fallback: parse TOTAL ORDER from embedded text when Tabula tables are missing/misclassified.
            try:
                payload0 = _build_sales_order_payload(combined_tables)
                grid0 = ((payload0.get("total_order") or {}).get("grid")) if isinstance(payload0, dict) else None
                if not (isinstance(grid0, list) and len(grid0) > 0):
                    parsed = _parse_total_order_from_text("\n".join(combined_texts))
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
                        combined_tables = [_table_add_rows_matrix(t) for t in combined_tables]
                        try:
                            for t in combined_tables:
                                if isinstance(t, dict) and str(t.get("table_kind") or "").strip().lower() == "total_order_grid":
                                    _normalize_size_grid_columns(t)
                        except Exception:
                            pass
                        if so_dbg_tables:
                            try:
                                _payload = {
                                    "event": "so_pdf_fastpath_total_order_text_fallback_used",
                                    "request_id": request_id,
                                    "row_count": len(parsed.get("rows") or []),
                                }
                                logger.info("so_pdf_fastpath_total_order_text_fallback_used %s", json.dumps(_payload, ensure_ascii=False))
                            except Exception:
                                pass
                    elif so_dbg_tables:
                        try:
                            # Provide a small snippet around TOTAL ORDER for debugging
                            full = "\n".join(combined_texts)
                            m = re.search(r"(?i)TOTAL\s+ORDER", full)
                            if m:
                                a = max(0, m.start() - 300)
                                b = min(len(full), m.start() + 900)
                                snippet = full[a:b]
                            else:
                                snippet = full[:900]
                            logger.info(
                                "so_pdf_fastpath_total_order_text_fallback_none %s",
                                json.dumps(
                                    {
                                        "event": "so_pdf_fastpath_total_order_text_fallback_none",
                                        "request_id": request_id,
                                        "note": "_parse_total_order_from_text returned None",
                                        "snippet": snippet,
                                    },
                                    ensure_ascii=False,
                                ),
                            )
                        except Exception:
                            pass
            except Exception:
                pass

            if so_dbg_tables:
                try:
                    kinds: Dict[str, int] = {}
                    for t in combined_tables:
                        if not isinstance(t, dict):
                            continue
                        k = str(t.get("table_kind") or "").strip() or "(none)"
                        kinds[k] = int(kinds.get(k, 0)) + 1
                    payload_dbg = _build_sales_order_payload(combined_tables)
                    grid_len = len(((payload_dbg.get("total_order") or {}).get("grid")) or []) if isinstance(payload_dbg, dict) else None
                    _payload = {
                        "event": "so_pdf_fastpath_summary",
                        "request_id": request_id,
                        "table_kinds": kinds,
                        "total_order_grid_len": grid_len,
                    }
                    logger.info("so_pdf_fastpath_summary %s", json.dumps(_payload, ensure_ascii=False))
                except Exception:
                    pass

            dt = time.perf_counter() - t0
            logger.info(
                "ocr_extract_done",
                extra={
                    "request_id": request_id,
                    "doc_filename": filename,
                    "engine": "pdfplumber_tabula" if has_pdf_tables else "pdfplumber",
                    "page_count": len(all_pages),
                    "duration_sec": round(dt, 4),
                    "warnings": len(warnings),
                    "errors": len(errors),
                },
            )

            return JSONResponse(
                {
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
                    "sales_order_payload": _build_sales_order_payload(combined_tables),
                }
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
        t_page0 = time.perf_counter()
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
                page_res["text"] = _postprocess_ocr_text(page_res.get("text") or "")
                page_res["fields"] = _extract_fields_smart(page_res.get("text") or "", page_res.get("tables") or [])
            elif engine == "paddle_ensemble":
                bgr = _ensure_bgr(input_for_ocr)
                struct_res = _run_paddle_structure(bgr)
                paddle_res = _run_paddle(bgr)

                merged_text = _merge_text(struct_res.get("text") or "", paddle_res.get("text") or "", paddle_res.get("avg_confidence"))
                merged_text = _postprocess_ocr_text(merged_text)

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

                page_res["fields"] = _extract_fields_smart(merged_text, page_res.get("tables") or [])
            else:
                # paddle expects bgr
                page_res = _run_paddle(_ensure_bgr(input_for_ocr))

            page_res["page"] = page_idx
            page_res["preprocess"] = prep_meta

            if engine == "paddle":
                page_res["tables"] = _extract_tables_from_paddle_page(image_for_tables, page_res)
                page_res["text"] = _postprocess_ocr_text(page_res.get("text") or "")
                page_res["fields"] = _extract_fields_smart(page_res.get("text") or "", page_res.get("tables") or [])

            if isinstance(page_res.get("tables"), list):
                page_res["tables"] = _filter_tables_for_sales_order(page_res.get("tables") or [])
                page_res["tables"] = [_table_add_rows_matrix(t) for t in (page_res.get("tables") or [])]
                page_res["tables"] = _filter_tables_for_sales_order(page_res.get("tables") or [])

            try:
                if isinstance(page_res.get("fields"), dict):
                    ai_tbl = _build_ai_kv_table_from_fields(page_res.get("fields"))
                    page_res["tables"] = [ai_tbl] + (page_res.get("tables") or [])
            except Exception:
                warnings.append({"page": page_idx, "code": "AI_TABLE_BUILD_FAILED", "message": "Failed to build AI key/value table"})

            page_res["timings"] = {"total_sec": round(time.perf_counter() - t_page0, 4)}

            page_res["field_pairs"] = _fields_to_pairs(page_res.get("fields"))

            all_pages.append(page_res)
            combined_texts.append(_postprocess_ocr_text(page_res.get("text") or ""))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"OCR failed on page {page_idx}: {e}")

    combined_tables: List[Dict[str, Any]] = []
    for p in all_pages:
        for t in (p.get("tables") or []):
            tt = {"page": p.get("page"), **t}
            try:
                tt = _table_add_rows_matrix(tt)
            except Exception:
                pass
            combined_tables.append(tt)

    combined_tables = _dedup_top_level_ai_kv_tables(combined_tables)

    try:
        if str(os.getenv("DEBUG_PARTIAL_DELIVERIES") or "").strip() in {"1", "true", "True", "YES", "yes"}:
            pd = []
            for t in combined_tables:
                if not isinstance(t, dict):
                    continue
                if str(t.get("table_kind") or "").strip().lower() != "partial_deliveries_grid":
                    continue
                pd.append(
                    {
                        "page": t.get("page"),
                        "table_index": t.get("table_index"),
                        "headers": t.get("headers"),
                        "pre_rows_matrix": t.get("pre_rows_matrix"),
                        "rows": t.get("rows"),
                    }
                )
            if pd:
                msg = json.dumps(pd, ensure_ascii=False)
                if len(msg) > 20000:
                    msg = msg[:20000] + "...<truncated>"
                logger.info("DEBUG_PARTIAL_DELIVERIES request_id=%s %s", request_id, msg)
                try:
                    print(f"DEBUG_PARTIAL_DELIVERIES_PRINT request_id={request_id} {msg}")
                    sys.stdout.flush()
                except Exception:
                    pass
            else:
                logger.info("DEBUG_PARTIAL_DELIVERIES request_id=%s no partial_deliveries_grid tables", request_id)
                try:
                    print(f"DEBUG_PARTIAL_DELIVERIES_PRINT request_id={request_id} no partial_deliveries_grid tables")
                    sys.stdout.flush()
                except Exception:
                    pass
    except Exception:
        pass

    combined_fields: List[Dict[str, Any]] = []
    for p in all_pages:
        if p.get("fields"):
            combined_fields.append({"page": p.get("page"), **(p.get("fields") or {})})

    combined_field_pairs: List[Dict[str, Any]] = []
    for p in all_pages:
        for pair in (p.get("field_pairs") or []):
            if isinstance(pair, dict) and pair.get("key") is not None and pair.get("value") is not None:
                combined_field_pairs.append({"page": p.get("page"), **pair})

    dt = time.perf_counter() - t0
    logger.info(
        "ocr_extract_done",
        extra={
            "request_id": request_id,
            "doc_filename": filename,
            "engine": engine,
            "page_count": len(all_pages),
            "duration_sec": round(dt, 4),
            "warnings": len(warnings),
            "errors": len(errors),
        },
    )

    sales_order_payload = _build_sales_order_payload(combined_tables)

    try:
        if str(os.getenv("DEBUG_SALES_ORDER_TRACE") or "").strip() in {"1", "true", "True", "YES", "yes"}:
            try:
                def _safe_norm_key(s: Any) -> str:
                    try:
                        return _norm_key(str(s or ""))
                    except Exception:
                        return str(s or "").strip().lower()

                def _levenshtein(a: str, b: str, max_dist: int = 4) -> int:
                    aa = str(a or "")
                    bb = str(b or "")
                    if aa == bb:
                        return 0
                    if not aa:
                        return len(bb)
                    if not bb:
                        return len(aa)
                    if abs(len(aa) - len(bb)) > max_dist:
                        return max_dist + 1
                    prev = list(range(len(bb) + 1))
                    for i, ca in enumerate(aa, start=1):
                        cur = [i]
                        row_best = max_dist + 1
                        for j, cb in enumerate(bb, start=1):
                            ins = cur[j - 1] + 1
                            dele = prev[j] + 1
                            sub = prev[j - 1] + (0 if ca == cb else 1)
                            v = ins if ins < dele else dele
                            if sub < v:
                                v = sub
                            cur.append(v)
                            if v < row_best:
                                row_best = v
                        if row_best > max_dist:
                            return max_dist + 1
                        prev = cur
                    return prev[-1]

                _HEADER_LABEL_TO_PAYLOAD = {
                    "ordernr": "ordernr",
                    "order_nr": "ordernr",
                    "orderno": "ordernr",
                    "order": "ordernr",
                    "date": "date",
                    "orderdate": "date",
                    "dateoforder": "date",
                    "season": "season",
                    "buyer": "buyer",
                    "purchaser": "purchaser",
                    "supplier": "supplier",
                    "sendto": "sendto",
                    "send_to": "sendto",
                    "send": "sendto",
                    "paymentterms": "paymentterms",
                    "payment_terms": "paymentterms",
                    "termsofpayment": "paymentterms",
                    "terms": "paymentterms",
                    "supplierref": "supplierref",
                    "supplier_ref": "supplierref",
                    "article": "article",
                    "description": "description",
                    "marketoforigin": "marketoforigin",
                    "market_origin": "marketoforigin",
                    "marketof": "marketoforigin",
                    "pvp": "pvp",
                    "compositionsinformation": "compositionsinformation",
                    "compositioninformation": "compositionsinformation",
                    "compositions": "compositionsinformation",
                    "composition": "compositionsinformation",
                    "careinstructions": "careinstructions",
                    "care": "careinstructions",
                    "hangtaglabel": "hangtaglabel",
                    "mainlabel": "mainlabel",
                    "externalfabric": "externalfabric",
                    "hanging": "hanging",
                    "totalorder": "totalorder",
                    "total": "totalorder",
                }

                def _canon_header_payload_key(label: str) -> Dict[str, str]:
                    lk0 = _safe_norm_key(label)
                    if not lk0:
                        return {"payload_key": "", "match_mode": "unknown"}
                    direct = _HEADER_LABEL_TO_PAYLOAD.get(lk0)
                    if direct:
                        return {"payload_key": direct, "match_mode": "direct"}
                    best_key = ""
                    best_d = 5
                    for cand in _HEADER_LABEL_TO_PAYLOAD.keys():
                        d = _levenshtein(lk0, cand, max_dist=4)
                        if d < best_d:
                            best_d = d
                            best_key = cand
                            if best_d <= 1:
                                break
                    if best_key and best_d <= 2:
                        return {"payload_key": _HEADER_LABEL_TO_PAYLOAD.get(best_key, ""), "match_mode": "fuzzy"}
                    return {"payload_key": lk0, "match_mode": "unknown"}

                _PARTIAL_LABEL_TO_PAYLOAD = {
                    "logisticorder": "logistic_order",
                    "logisticsorder": "logistic_order",
                    "handoverdate": "handover_date",
                    "transportmode": "transport_mode",
                    "transportationmode": "transport_mode",
                    "presentationtype": "presentation_type",
                    "costprice": "cost_price",
                    "delivery": "delivery",
                    "incoterm": "incoterm",
                    "from": "from",
                }

                def _canon_partial_payload_key(label: str) -> Dict[str, str]:
                    lk0 = _safe_norm_key(label)
                    if not lk0:
                        return {"payload_key": "", "match_mode": "unknown"}
                    direct = _PARTIAL_LABEL_TO_PAYLOAD.get(lk0)
                    if direct:
                        return {"payload_key": direct, "match_mode": "direct"}
                    best_key = ""
                    best_d = 5
                    for cand in _PARTIAL_LABEL_TO_PAYLOAD.keys():
                        d = _levenshtein(lk0, cand, max_dist=4)
                        if d < best_d:
                            best_d = d
                            best_key = cand
                            if best_d <= 1:
                                break
                    if best_key and best_d <= 2:
                        return {"payload_key": _PARTIAL_LABEL_TO_PAYLOAD.get(best_key, ""), "match_mode": "fuzzy"}
                    return {"payload_key": lk0, "match_mode": "unknown"}

                candidates_header: List[Dict[str, Any]] = []
                candidates_partial_meta: List[Dict[str, Any]] = []
                candidates_partial_rows: List[Dict[str, Any]] = []

                for t in combined_tables:
                    if not isinstance(t, dict):
                        continue
                    page = t.get("page")
                    table_index = t.get("table_index")
                    tk = str(t.get("table_kind") or "").strip().lower()

                    if t.get("headers") == ["key", "value"]:
                        kv_all = t.get("kv_pairs_all")
                        if isinstance(kv_all, list):
                            for p in kv_all:
                                if not isinstance(p, dict):
                                    continue
                                k = str(p.get("key") or "").strip()
                                v = str(p.get("value") or "").strip()
                                if k:
                                    canon = _canon_header_payload_key(k)
                                    payload_key = str(canon.get("payload_key") or "")
                                    match_mode = str(canon.get("match_mode") or "unknown")
                                    candidates_header.append(
                                        {
                                            "page": page,
                                            "table_index": table_index,
                                            "source": "ai_kv",
                                            "key": k,
                                            "key_norm": _safe_norm_key(k),
                                            "payload_key": payload_key,
                                            "match_mode": match_mode,
                                            "value": v,
                                        }
                                    )
                        else:
                            rm = t.get("rows_matrix")
                            if isinstance(rm, list):
                                for r in rm:
                                    if not (isinstance(r, list) and len(r) >= 2):
                                        continue
                                    k = str(r[0] or "").strip()
                                    v = str(r[1] or "").strip()
                                    if k and k.lower() != "key":
                                        canon = _canon_header_payload_key(k)
                                        payload_key = str(canon.get("payload_key") or "")
                                        match_mode = str(canon.get("match_mode") or "unknown")
                                        candidates_header.append(
                                            {
                                                "page": page,
                                                "table_index": table_index,
                                                "source": "ai_kv_rows_matrix",
                                                "key": k,
                                                "key_norm": _safe_norm_key(k),
                                                "payload_key": payload_key,
                                                "match_mode": match_mode,
                                                "value": v,
                                            }
                                        )

                    # Also collect generic rows_matrix candidates from non-kv tables (helps find missed header kv)
                    if t.get("headers") != ["key", "value"]:
                        rm = t.get("rows_matrix")
                        if isinstance(rm, list):
                            for rr in rm[:30]:
                                if not (isinstance(rr, list) and len(rr) >= 2):
                                    continue
                                k = str(rr[0] or "").strip()
                                v = str(rr[1] or "").strip()
                                if not k or k.lower() in {"key", "variable"}:
                                    continue
                                # Skip obvious table headers
                                if re.fullmatch(r"(?i)COLOU?R|XS|S|M|L|XL|TOTAL", k.strip()):
                                    continue
                                canon = _canon_header_payload_key(k)
                                payload_key = str(canon.get("payload_key") or "")
                                match_mode = str(canon.get("match_mode") or "unknown")
                                candidates_header.append(
                                    {
                                        "page": page,
                                        "table_index": table_index,
                                        "source": "rows_matrix",
                                        "key": k,
                                        "key_norm": _safe_norm_key(k),
                                        "payload_key": payload_key,
                                        "match_mode": match_mode,
                                        "value": v,
                                    }
                                )

                    if tk == "partial_deliveries_grid":
                        pre = t.get("pre_rows_matrix")
                        if isinstance(pre, list):
                            for r in pre:
                                if not (isinstance(r, list) and len(r) >= 2):
                                    continue
                                k = str(r[0] or "").strip()
                                v = str(r[1] or "").strip()
                                if k:
                                    canon = _canon_partial_payload_key(k)
                                    payload_key = str(canon.get("payload_key") or "")
                                    match_mode = str(canon.get("match_mode") or "unknown")
                                    candidates_partial_meta.append(
                                        {
                                            "page": page,
                                            "table_index": table_index,
                                            "source": "partial_pre_rows_matrix",
                                            "key": k,
                                            "key_norm": _safe_norm_key(k),
                                            "payload_key": payload_key,
                                            "match_mode": match_mode,
                                            "value": v,
                                        }
                                    )

                        # Include rows that were filtered out (TOTAL, COST PRICE) for trace visibility
                        rows = t.get("rows")
                        if isinstance(rows, list):
                            for r in rows:
                                if not isinstance(r, dict):
                                    continue
                                c0 = str(r.get("COLOUR") or "").strip()
                                xs0 = str(r.get("XS") or "").strip()
                                if not c0:
                                    continue
                                kind = "line"
                                payload_key = ""
                                reason = ""
                                if re.fullmatch(r"TOTAL", c0, flags=re.IGNORECASE):
                                    kind = "summary"
                                    reason = "filtered_total_row"
                                if re.search(r"\bCOST\s*PRICE\b", c0, flags=re.IGNORECASE):
                                    kind = "meta"
                                    payload_key = "cost_price"
                                    reason = "meta_row_cost_price"
                                candidates_partial_rows.append(
                                    {
                                        "page": page,
                                        "table_index": table_index,
                                        "source": "partial_rows",
                                        "key": c0,
                                        "key_norm": _safe_norm_key(c0),
                                        "payload_key": payload_key,
                                        "value": xs0,
                                        "kind": kind,
                                        "reason": reason,
                                    }
                                )

                payload_header = (sales_order_payload or {}).get("header") or {}
                used_header_keys = {_safe_norm_key(k) for k in payload_header.keys()}

                used_header_payload_keys = {str(_canon_header_payload_key(k).get("payload_key") or "") for k in payload_header.keys()}

                used_partial_meta_keys: set[str] = set()
                for mh in (sales_order_payload or {}).get("partial_delivery_headers") or []:
                    if not isinstance(mh, dict):
                        continue
                    for k, v in mh.items():
                        if k == "delivery_seq":
                            continue
                        if v not in (None, ""):
                            used_partial_meta_keys.add(_safe_norm_key(k))

                used_partial_payload_keys = set()
                for mh in (sales_order_payload or {}).get("partial_delivery_headers") or []:
                    if not isinstance(mh, dict):
                        continue
                    for k, v in mh.items():
                        if k == "delivery_seq":
                            continue
                        if v not in (None, ""):
                            used_partial_payload_keys.add(_safe_norm_key(k))

                def _classify_header_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
                    out = dict(c)
                    v = str(out.get("value") or "").strip()
                    pk = str(out.get("payload_key") or "").strip()
                    if not v:
                        out["status"] = "skipped"
                        out["reason"] = "empty_value"
                        return out
                    if pk and pk in used_header_payload_keys:
                        out["status"] = "used"
                        out["reason"] = "mapped_to_payload"
                        return out
                    if out.get("key_norm") in used_header_keys:
                        out["status"] = "used"
                        out["reason"] = "exact_key_used"
                        return out
                    out["status"] = "unused"
                    out["reason"] = "not_mapped_or_not_used"
                    return out

                header_candidates_classified = [_classify_header_candidate(c) for c in candidates_header]
                unused_header = [c for c in header_candidates_classified if c.get("status") == "unused"]

                def _classify_partial_meta_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
                    out = dict(c)
                    v = str(out.get("value") or "").strip()
                    pk = str(out.get("payload_key") or "").strip()
                    if not v:
                        out["status"] = "skipped"
                        out["reason"] = "empty_value"
                        return out
                    if pk and _safe_norm_key(pk) in used_partial_payload_keys:
                        out["status"] = "used"
                        out["reason"] = "mapped_to_payload"
                        return out
                    out["status"] = "unused"
                    out["reason"] = "not_mapped_or_not_used"
                    return out

                partial_meta_candidates_classified = [_classify_partial_meta_candidate(c) for c in candidates_partial_meta]
                unused_partial_meta = [c for c in partial_meta_candidates_classified if c.get("status") == "unused"]

                def _classify_partial_row_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
                    out = dict(c)
                    v = str(out.get("value") or "").strip()
                    pk = str(out.get("payload_key") or "").strip()
                    if out.get("reason") == "filtered_total_row":
                        out["status"] = "skipped"
                        return out
                    if pk and _safe_norm_key(pk) in used_partial_payload_keys and v:
                        out["status"] = "used"
                        out["reason"] = out.get("reason") or "mapped_to_payload"
                        return out
                    if not v:
                        out["status"] = "skipped"
                        out["reason"] = out.get("reason") or "empty_value"
                        return out
                    out["status"] = "unused"
                    out["reason"] = out.get("reason") or "not_used"
                    return out

                partial_row_candidates_classified = [_classify_partial_row_candidate(c) for c in candidates_partial_rows]

                trace = {
                    "header": {
                        "used_keys": sorted(list(used_header_keys)),
                        "used_payload_keys": sorted(list(used_header_payload_keys)),
                        "candidates": header_candidates_classified,
                        "unused_candidates": unused_header,
                    },
                    "partial_delivery_meta": {
                        "used_keys": sorted(list(used_partial_meta_keys)),
                        "used_payload_keys": sorted(list(used_partial_payload_keys)),
                        "candidates": partial_meta_candidates_classified,
                        "unused_candidates": unused_partial_meta,
                    },
                    "partial_delivery_rows": {
                        "candidates": partial_row_candidates_classified,
                        "unused_candidates": [c for c in partial_row_candidates_classified if c.get("status") == "unused"],
                        "skipped_candidates": [c for c in partial_row_candidates_classified if c.get("status") == "skipped"],
                    },
                }

                trace_msg = json.dumps(trace, ensure_ascii=False)
                logger.info("DEBUG_SALES_ORDER_TRACE request_id=%s %s", request_id, trace_msg)
                try:
                    chunk = 8000
                    total_parts = (len(trace_msg) + chunk - 1) // chunk
                    print(f"DEBUG_SALES_ORDER_TRACE_PRINT request_id={request_id} BEGIN parts={total_parts}")
                    for i in range(0, len(trace_msg), chunk):
                        part_no = i // chunk + 1
                        part = trace_msg[i : i + chunk]
                        print(
                            f"DEBUG_SALES_ORDER_TRACE_PRINT request_id={request_id} part={part_no}/{total_parts} {part}"
                        )
                    print(f"DEBUG_SALES_ORDER_TRACE_PRINT request_id={request_id} END")
                    sys.stdout.flush()
                except Exception:
                    pass
            except Exception:
                pass
    except Exception:
        pass

    try:
        if str(os.getenv("DEBUG_SALES_ORDER_PAYLOAD") or "").strip() in {"1", "true", "True", "YES", "yes"}:
            msg = json.dumps(sales_order_payload, ensure_ascii=False)
            logger.info("DEBUG_SALES_ORDER_PAYLOAD request_id=%s %s", request_id, msg)
            try:
                try:
                    header_only = json.dumps({"header": (sales_order_payload or {}).get("header")}, ensure_ascii=False)
                    print(f"DEBUG_SALES_ORDER_HEADER_PRINT request_id={request_id} {header_only}")
                except Exception:
                    pass

                # Print in chunks to avoid console line limits while still printing full JSON.
                chunk = 8000
                total_parts = (len(msg) + chunk - 1) // chunk
                try:
                    head_snip = msg[:300]
                    print(f"DEBUG_SALES_ORDER_PAYLOAD_PRINT request_id={request_id} HEAD {head_snip}")
                except Exception:
                    pass
                print(f"DEBUG_SALES_ORDER_PAYLOAD_PRINT request_id={request_id} BEGIN parts={total_parts}")
                for i in range(0, len(msg), chunk):
                    part_no = i // chunk + 1
                    part = msg[i : i + chunk]
                    print(
                        f"DEBUG_SALES_ORDER_PAYLOAD_PRINT request_id={request_id} part={part_no}/{total_parts} {part}"
                    )
                print(f"DEBUG_SALES_ORDER_PAYLOAD_PRINT request_id={request_id} END")
                sys.stdout.flush()
            except Exception:
                pass
    except Exception:
        pass

    return JSONResponse(
        {
            "schema_version": "1.0",
            "document_meta": {
                "request_id": request_id,
                "filename": filename,
                "engine": engine,
                "preprocess": {"enabled": preprocess, "mode": preprocess_mode},
                "page_count": len(all_pages),
                "timings": {"total_sec": round(dt, 4)},
            },
            "warnings": warnings,
            "errors": errors,
            "filename": filename,
            "engine": engine,
            "pages": all_pages,
            "text": "\n\n".join(combined_texts).strip(),
            "tables": combined_tables,
            "fields": combined_fields,
            "field_pairs": combined_field_pairs,
            "sales_order_payload": sales_order_payload,
        }
    )
