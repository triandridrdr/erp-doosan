from typing import Any, Dict, Tuple

import cv2
import numpy as np


def _ensure_bgr(image: np.ndarray) -> np.ndarray:
    if image.ndim == 2:
        return cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
    if image.shape[2] == 4:
        return cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)
    return image


def preprocess_opencv(image_bgr: np.ndarray) -> Tuple[np.ndarray, Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    bgr = _ensure_bgr(image_bgr)

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

    return thr, meta
