import re
from typing import Dict

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


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

_HEADER_LABEL_TO_CANON: Dict[str, str] = {
    "ordernr": "ordernr",
    "order_nr": "ordernr",
    "orderno": "ordernr",
    "order": "ordernr",
    "orderid": "ordernr",
    "ordernumber": "ordernr",
    "ordernumberpo": "ordernr",
    "orderreference": "ordernr",
    "orderref": "ordernr",
    "orderrefno": "ordernr",
    "orderrefnumber": "ordernr",
    "purchaseorder": "ordernr",
    "purchaseordernumber": "ordernr",
    "purchaseorderid": "ordernr",
    "purchaseorderno": "ordernr",
    "purchaseorderref": "ordernr",
    "purchaseorderreference": "ordernr",
    "purchaseorderidno": "ordernr",
    "ponumber": "ordernr",
    "poref": "ordernr",
    "poreference": "ordernr",
    "poreferenceno": "ordernr",
    "poreferencenumber": "ordernr",
    "porefno": "ordernr",
    "porefnumber": "ordernr",
    "purchaseordernr": "ordernr",
    "purchaseorderno": "ordernr",
    "purchaseordernumber": "ordernr",
    "no": "ordernr",
    "noso": "ordernr",
    "nosalesorder": "ordernr",
    "salesorder": "ordernr",
    "salesorderno": "ordernr",
    "salesordernumber": "ordernr",
    "pono": "ordernr",
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
    "shiptoaddr": "sendto",
    "shiptoaddress1": "sendto",
    "shiptoaddress2": "sendto",
    "shiptoaddress3": "sendto",
    "shippingaddress": "sendto",
    "deliveryaddress": "sendto",
    "deliveryto": "sendto",
    "delivery_to": "sendto",
    "deliverystation": "sendto",
    "deliver_to": "sendto",
    "deliverto": "sendto",
    "paymentterms": "paymentterms",
    "payment_terms": "paymentterms",
    "termsofpayment": "paymentterms",
    "terms": "paymentterms",
    "paymentterm": "paymentterms",
    "payment_term": "paymentterms",
    "payterms": "paymentterms",
    "termspayment": "paymentterms",
    "paymentcondition": "paymentterms",
    "paymentconditions": "paymentterms",
    "paymenttermss": "paymentterms",
    "supplierref": "supplierref",
    "supplier_ref": "supplierref",
    "vendorref": "supplierref",
    "vendor_ref": "supplierref",
    "reference": "supplierref",
    "ref": "supplierref",
    "vendorreference": "supplierref",
    "vendorreferenceno": "supplierref",
    "vendorreferencenumber": "supplierref",
    "factoryref": "supplierref",
    "factoryreference": "supplierref",
    "supplierreference": "supplierref",
    "supplierreferenceno": "supplierref",
    "article": "article",
    "style": "article",
    "styleno": "article",
    "style_no": "article",
    "item": "article",
    "itemno": "article",
    "item_no": "article",
    "articleno": "article",
    "articlecode": "article",
    "stylenumber": "article",
    "model": "article",
    "description": "description",
    "desc": "description",
    "descripton": "description",
    "descriplion": "description",
    "itemdescription": "description",
    "productdescription": "description",
    "marketoforigin": "marketoforigin",
    "market_origin": "marketoforigin",
    "origin": "marketoforigin",
    "countryoforigin": "marketoforigin",
    "countryorigin": "marketoforigin",
    "countryofmanufacture": "marketoforigin",
    "manufacturingcountry": "marketoforigin",
    "pvp": "pvp",
    "rrp": "pvp",
    "recommendedretailprice": "pvp",
    "retailprice": "pvp",
    "unitprice": "pvp",
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


def canon_header_key_fuzzy(label: str) -> str:
    nk = _norm_key(label)
    if not nk:
        return ""
    direct = _HEADER_LABEL_TO_CANON.get(nk)
    if direct:
        return direct
    if nk in _HEADER_CANON_KEYS:
        return nk

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

    if fuzz is not None:
        best_score = 0.0
        best_cand = None
        for cand in _HEADER_LABEL_TO_CANON.keys():
            score = float(fuzz.ratio(nk, _norm_key(cand)))
            if score > best_score:
                best_score = score
                best_cand = cand
        if best_cand is not None and best_score >= 85.0:
            return _HEADER_LABEL_TO_CANON.get(str(best_cand), "")
    return ""
