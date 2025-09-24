# -*- coding: utf-8 -*-
"""
Collector genérico de RSS con enriquecimiento opcional por detalle.
- Lee RSS/Atom, filtra por fecha y CPV.
- Si follow_detail=True: hace GET a cada enlace y extrae expediente, órgano, CPV, importes.
  Parsers incluidos:
    * Comunidad de Madrid (contratos-publicos.comunidad.madrid)
    * Galicia PcPG (contratosdegalicia.gal)
    * Fallback genérico si el dominio no está mapeado.
"""
import re, time, feedparser, requests
from urllib.parse import urlparse
from lxml import html
from datetime import datetime
from collectors.utils import parse_date_any, in_date_range, extract_cpvs_from_text, cpv_match

UA = {"User-Agent": "TendersAggregator/1.0 (+bot educado)"}

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _get(url: str, timeout: int = 60) -> html.HtmlElement | None:
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return html.fromstring(r.content)

# -------- Parsers de detalle --------
def parse_detail_madrid(doc: html.HtmlElement) -> dict:
    out = {}
    title = doc.xpath("string(//h1)") or doc.xpath("string(//h2)")
    out["objeto"] = _norm(title)
    txt = doc.text_content() or ""
    m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", txt)
    if m: out["expediente"] = m.group(0)
    for label in ["Órgano de contratación", "Entidad adjudicadora", "Organismo"]:
        xp = f"//*[contains(translate(text(),'{label.upper()}','{label.upper()}'),'{label.upper()}')]/following::*[1]"
        val = doc.xpath(f"string({xp})")
        if val:
            out["organo"] = _norm(val); break
    m = re.search(r"[\d\.\s]+,\d{2}\s*€", txt)
    if m: out["importe"] = _norm(m.group(0))
    cpvs = extract_cpvs_from_text(txt)
    if cpvs: out["cpv"] = ";".join(sorted(set(cpvs)))
    return out

def parse_detail_galicia(doc: html.HtmlElement) -> dict:
    out = {}
    title = doc.xpath("string(//h1)") or doc.xpath("string(//h2)")
    out["objeto"] = _norm(title)
    txt = doc.text_content() or ""
    m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", txt)
    if m: out["expediente"] = m.group(0)
    for label in ["Órgano de contratación", "Organismo", "Órgano"]:
        xp = f"//*[contains(translate(text(),'{label.upper()}','{label.upper()}'),'{label.upper()}')]/following::*[1]"
        val = doc.xpath(f"string({xp})")
        if val:
            out["organo"] = _norm(val); break
    m = re.search(r"[\d\.\s]+,\d{2}\s*€", txt)
    if m: out["importe"] = _norm(m.group(0))
    cpvs = extract_cpvs_from_text(txt)
    if cpvs: out["cpv"] = ";".join(sorted(set(cpvs)))
    return out

PARSERS_BY_DOMAIN = {
    "contratos-publicos.comunidad.madrid": parse_detail_madrid,
    "www.contratosdegalicia.gal": parse_detail_galicia,
    "contratosdegalicia.gal": parse_detail_galicia,
}

def enrich_by_detail(url: str) -> dict:
    try:
        doc = _get(url)
        if doc is None:
            return {}
        host = urlparse(url).hostname or ""
        for dom, fn in PARSERS_BY_DOMAIN.items():
            if dom in host:
                return fn(doc)
        # Fallback genérico
        out = {}
        title = doc.xpath("string(//h1)") or doc.xpath("string(//h2)")
        out["objeto"] = _norm(title)
        txt = doc.text_content() or ""
        m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", txt)
        if m: out["expediente"] = m.group(0)
        m = re.search(r"[\d\.\s]+,\d{2}\s*€", txt)
        if m: out["importe"] = _norm(m.group(0))
        cpvs = extract_cpvs_from_text(txt)
        if cpvs: out["cpv"] = ";".join(sorted(set(cpvs)))
        return out
    except Exception:
        return {}

# -------- Colector principal --------
def collect(source_name: str,
            feed_url: str,
            date_start: datetime,
            date_end: datetime,
            cpv_targets: set[str],
            cpv_mode: str = "exact",
            follow_detail: bool = False,
            polite_delay: float = 0.8) -> list[dict]:
    d = feedparser.parse(feed_url)
    rows = []
    for e in d.entries:
        # Fecha publicada
        pub = None
        for cand in [getattr(e, "published", None), getattr(e, "updated", None), getattr(e, "created", None)]:
            pub = parse_date_any(cand)
            if pub: break
        if not in_date_range(pub, date_start, date_end):
            continue

        title = getattr(e, "title", "") or ""
        link  = getattr(e, "link", "") or ""
        summary = getattr(e, "summary", "") or ""
        content = f"{title}\n{summary}"

        # Heurística desde feed
        expediente = ""
        m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", content)
        if m: expediente = m.group(0)

        organo = ""
        for tag in ["Órgano", "Organismo", "Entidad"]:
            m = re.search(tag+r".{0,3}:\s*(.+?)(?:<|$|\n)", summary, flags=re.IGNORECASE)
            if m:
                organo = _norm(m.group(1)); break

        importe = ""
        m = re.search(r"[\d\.\s]+,\d{2}\s*€", content)
        if m: importe = _norm(m.group(0))

        cpvs_base = extract_cpvs_from_text(content)

        # Enriquecer con detalle
        cpvs = cpvs_base[:]
        if follow_detail and link:
            time.sleep(polite_delay)
            extra = enrich_by_detail(link)
            if extra.get("expediente"): expediente = extra["expediente"]
            if extra.get("organo"):     organo     = extra["organo"]
            if extra.get("importe"):    importe    = extra["importe"]
            if extra.get("objeto"):     title      = extra["objeto"]
            if extra.get("cpv"):
                cpvs = sorted(set(cpvs + extra["cpv"].split(";")))

        # Filtro CPV
        if cpv_targets and not cpv_match(cpvs, cpv_targets, cpv_mode):
            continue

        rows.append({
            "fuente": source_name,
            "expediente": expediente,
            "objeto": title,
            "organo": organo,
            "estado": "",
            "importe": importe,
            "cpv": ";".join(cpvs),
            "fecha_published": pub.isoformat() if pub else "",
            "fecha_updated": "",
            "enlace": link
        })
    return rows
