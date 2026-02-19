# service-python (Python OCR Service)

HTTP OCR microservice intended to be called from Spring Boot.

Pipeline:

1. Spring Boot -> HTTP request
2. Python OCR Service
3. Preprocessing (OpenCV)
4. OCR engine (Tesseract / PaddleOCR)
5. Return JSON

## Endpoints

- `GET /health`
- `POST /ocr/extract`
  - multipart form field: `file`
  - query params:
    - `engine`: `tesseract` (default) | `paddle`
    - `preprocess`: `true` (default) | `false`

## Local setup (Windows)

1. Create venv and install deps:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Tesseract (required if using `engine=tesseract`)

- Install Tesseract OCR for Windows.
- Ensure `tesseract.exe` is in PATH, or set env var:

```powershell
$env:TESSERACT_CMD="C:\Program Files\Tesseract-OCR\tesseract.exe"
```

3. PDF support (optional)

If you upload PDF, `pdf2image` needs Poppler installed and available on PATH.

4. Run server:

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

## Example request

```powershell
curl -X POST "http://localhost:8001/ocr/extract?engine=tesseract&preprocess=true" `
  -F "file=@sample.png"
```
