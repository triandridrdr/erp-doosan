import re
from typing import Any, Callable, Dict, List, Optional, Tuple


def _norm_space(s: str) -> str:
    return " ".join((s or "").replace("\r", "\n").replace("\t", " ").split()).strip()


def _cell_str(x: Any) -> str:
    return _norm_space(str(x or ""))


def _looks_like_value(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    if len(t) <= 1:
        return False
    # Avoid treating repeated column labels as values
    if re.fullmatch(r"KEY|VALUE", t.strip(), flags=re.IGNORECASE):
        return False
    return True


def _split_kv_inline(s: str) -> Optional[Tuple[str, str]]:
    # Handles: "ORDER NO: 123", "DATE - 01/01/2025"
    t = _norm_space(s)
    if not t:
        return None
    m = re.search(r"^(.{2,60}?)(?:\s*[:\-]\s+)(.{1,200})$", t)
    if not m:
        return None
    k = _norm_space(m.group(1))
    v = _norm_space(m.group(2))
    if not (k and v):
        return None
    return (k, v)


def extract_header_kv_from_tables(
    *,
    tables: Any,
    canon_key: Callable[[str], str],
    norm_key: Callable[[str], str],
    max_scan_rows: int = 30,
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not isinstance(tables, list):
        return out

    def push(k_raw: str, v_raw: str) -> None:
        k0 = (k_raw or "").strip()
        v0 = (v_raw or "").strip()
        if not (k0 and v0):
            return
        ck = canon_key(k0) or norm_key(k0)
        if not ck:
            return
        out.append((ck, v0))

    for t in tables:
        if not isinstance(t, dict):
            continue
        rm = t.get("rows_matrix")
        if not isinstance(rm, list) or not rm:
            continue

        rows = [r for r in rm if isinstance(r, list)]
        if not rows:
            continue
        rows = rows[:max_scan_rows]

        # A) Vertical 2-col: [Label, Value]
        for r in rows:
            if len(r) < 2:
                continue
            c0 = _cell_str(r[0])
            c1 = _cell_str(r[1])
            if not (c0 and c1):
                continue
            if canon_key(c0):
                if _looks_like_value(c1):
                    push(c0, c1)
            else:
                sp = _split_kv_inline(c0)
                if sp and canon_key(sp[0]):
                    push(sp[0], sp[1])

        # B) Horizontal header row then values row
        for i in range(0, max(0, len(rows) - 1)):
            r0 = rows[i]
            r1 = rows[i + 1]
            # require at least 2 columns
            if len(r0) < 2 or len(r1) < 2:
                continue
            # map column indexes where header cell is recognized as a key
            col_map: Dict[int, str] = {}
            for ci, cell in enumerate(r0[:20]):
                s = _cell_str(cell)
                if not s:
                    continue
                ck = canon_key(s)
                if ck:
                    col_map[int(ci)] = ck
                else:
                    sp = _split_kv_inline(s)
                    if sp and canon_key(sp[0]):
                        # inline key:value in header row
                        push(sp[0], sp[1])
            if len(col_map) < 2:
                continue
            for ci, ck in col_map.items():
                if ci >= len(r1):
                    continue
                v = _cell_str(r1[ci])
                if _looks_like_value(v) and not canon_key(v):
                    out.append((ck, v))

        # C) Stacked labels in one row, stacked values in next row but shifted
        for i in range(0, max(0, len(rows) - 1)):
            r0 = [ _cell_str(x) for x in rows[i] ]
            r1 = [ _cell_str(x) for x in rows[i + 1] ]
            r0 = [x for x in r0 if x]
            r1 = [x for x in r1 if x]
            if len(r0) < 2 or len(r1) < 2:
                continue
            keys = []
            for x in r0[:8]:
                ck = canon_key(x)
                if ck:
                    keys.append(ck)
            if len(keys) < 2:
                continue
            # take first N values from next row that are not keys
            vals = [v for v in r1 if _looks_like_value(v) and not canon_key(v)]
            if len(vals) < 2:
                continue
            n = min(len(keys), len(vals))
            for kk, vv in zip(keys[:n], vals[:n]):
                out.append((kk, vv))

    return out
