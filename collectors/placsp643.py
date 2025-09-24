# -*- coding: utf-8 -*-
"""
Collector PLACSP sindicación 643 (ZIP mensual actualizado a diario).
Filtra por fecha published/updated/either y por CPV (exact/prefix), con scope de CPV.
"""
import io, time, zipfile, requests
from typing import List, Set
from lxml import etree
from collectors.utils import cpv_match

ZIP_URL_TEMPLATE = ("https://contrataciondelestado.es/sindicacion/sindicacion_643/"
                    "licitacionesPerfilesContratanteCompleto3_{yyyymm}.zip")
_PARSER = etree.XMLParser(recover=True, resolve_entities=False, huge_tree=True)

def _http_get_bytes(url: str, timeout: int = 120, retries: int = 3, backoff: float = 1.5) -> bytes:
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last = e
            if i < retries-1: time.sleep(backoff**i)
    raise RuntimeError(f"GET {url} failed: {last}")

def _iter_entries(atom_bytes: bytes):
    root = etree.fromstring(atom_bytes, parser=_PARSER)
    return root.xpath("//*[local-name()='entry']")

def _t1(n, xp: str) -> str:
    res = n.xpath(xp)
    if not res: return ""
    v = res[0]
    return v if isinstance(v, str) else (v.text or "")

def _texts(n, xp: str) -> List[str]:
    res = n.xpath(xp); out=[]
    for v in res:
        out.append(v if isinstance(v, str) else (v.text or ""))
    return [x for x in out if x is not None]

def _date_starts(e, iso_date: str, field: str) -> bool:
    v = _t1(e, f"string(./*[local-name()='{field}'])")
    return bool(v) and v.startswith(iso_date)

def _best_link(e):
    href = _t1(e, "string(./*[local-name()='link' and @rel='alternate']/@href)")
    return href or _t1(e, "string(./*[local-name()='link']/@href)")

def _cpv_scoped(e, scope: str) -> List[str]:
    if scope == "folder":
        return _texts(e, ".//*[local-name()='ProcurementProject']//*[local-name()='ItemClassificationCode']/text()")
    if scope == "lots":
        return _texts(e, ".//*[local-name()='ProcurementProjectLot']//*[local-name()='ItemClassificationCode']/text()")
    return _texts(e, ".//*[local-name()='ItemClassificationCode']/text()")

def collect(date_iso: str,
            when: str = "either",
            cpv: List[str] | None = None,
            cpv_mode: str = "exact",
            cpv_scope: str = "folder") -> List[dict]:
    cpv = cpv or []
    targets: Set[str] = set()
    for c in cpv:
        c = (c or "").strip()
        if not c: continue
        if cpv_mode == "exact":
            if c.isdigit(): targets.add(c.zfill(8))
        else:
            if c.isdigit() and 2 <= len(c) <= 8: targets.add(c)

    yyyymm = date_iso[:7].replace("-", "")
    url = ZIP_URL_TEMPLATE.replace("{yyyymm}", yyyymm)
    zip_bytes = _http_get_bytes(url)

    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".atom"): continue
            try: atom = zf.read(name)
            except KeyError: continue
            for e in _iter_entries(atom):
                ok_date = (
                    (when=="updated"   and _date_starts(e, date_iso, "updated")) or
                    (when=="published" and _date_starts(e, date_iso, "published")) or
                    (when=="either"    and (_date_starts(e, date_iso, "updated") or _date_starts(e, date_iso, "published")))
                )
                if not ok_date: continue
                cpvs = set(_cpv_scoped(e, cpv_scope))
                if targets and not cpv_match(cpvs, targets, cpv_mode):
                    continue
                expediente = _t1(e, "string(.//*[local-name()='ContractFolderID'])")
                title      = _t1(e, "string(./*[local-name()='title'])")
                organo     = _t1(e, "string(.//*[local-name()='ContractingPartyName'])")
                importe    = _t1(e, "string(.//*[local-name()='TotalAmount'])")
                estado     = _t1(e, "string(.//*[local-name()='ContractFolderStatus'])")
                published  = _t1(e, "string(./*[local-name()='published'])")
                updated    = _t1(e, "string(./*[local-name()='updated'])")
                link       = _best_link(e)
                rows.append({
                    "fuente":"PLACSP",
                    "expediente":expediente, "objeto":title, "organo":organo, "estado":estado,
                    "importe":importe, "cpv":";".join(sorted(set(cpvs))),
                    "fecha_published":published, "fecha_updated":updated, "enlace":link
                })
    rows.sort(key=lambda r:(r.get("expediente") or "", r.get("enlace") or ""))
    return rows
