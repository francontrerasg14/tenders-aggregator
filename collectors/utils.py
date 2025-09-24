# -*- coding: utf-8 -*-
import re
from datetime import datetime
from dateutil import parser as dateparser

def parse_date_any(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return dateparser.parse(s)
    except Exception:
        return None

def in_date_range(dt_val: datetime | None, start: datetime, end: datetime) -> bool:
    if dt_val is None:
        return False
    return start <= dt_val <= end

def normalize_cpv_list(codes: list[str], mode: str = "exact") -> set[str]:
    out = set()
    for c in (codes or []):
        c = (c or "").strip()
        if not c:
            continue
        if mode == "exact":
            if c.isdigit():
                out.add(c.zfill(8))
        else:  # prefix
            if c.isdigit() and 2 <= len(c) <= 8:
                out.add(c)
    return out

def cpv_match(values: list[str] | set[str], targets: set[str], mode: str = "exact") -> bool:
    if not targets:
        return True
    vals = set(values or [])
    if not vals:
        return False
    if mode == "exact":
        return bool(vals & targets)
    for v in vals:
        for t in targets:
            if v.startswith(t):
                return True
    return False

# 8 dígitos con check digit opcional (NNNNNNNN-#) → devolvemos solo los 8
CPV_RE = re.compile(r"\b(\d{8})(?:-\d)?\b")

def extract_cpvs_from_text(text: str) -> list[str]:
    if not text:
        return []
    return list({m.group(1) for m in CPV_RE.finditer(text)})

# Normaliza espacios “raros” (NBSP, etc.)
NBSP_RE = re.compile(r"[\u00A0\u2007\u202F]")
def norm_spaces(s: str) -> str:
    if not s:
        return s
    s = NBSP_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()
