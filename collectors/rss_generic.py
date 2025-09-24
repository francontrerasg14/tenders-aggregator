# -*- coding: utf-8 -*-
"""
RSS + enriquecimiento por detalle.
Mejoras:
- Galicia: lee la TABLA "CPV – ..." (todos los códigos) + fallback.
- Importes: busca etiquetas en gallego/castellano y, si falla, primer "€" de la página.
- Diagnóstico: cpv_source (feed/detail), importe_source, detail_status.
"""
import re, time, feedparser, requests
from urllib.parse import urlparse
from lxml import html
from datetime import datetime
from collectors.utils import (
    parse_date_any, in_date_range, extract_cpvs_from_text, cpv_match, norm_spaces
)

UA = {"User-Agent": "TendersAggregator/1.0 (+bot educado)"}

def _norm(s: str) -> str:
    return norm_spaces(s or "")

def _get(url: str, timeout: int = 60) -> html.HtmlElement | None:
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return html.fromstring(r.content)

# ---------- Parsers de detalle ----------
def _extract_amount_by_labels(doc: html.HtmlElement) -> tuple[str, str]:
    """
    Busca importes por etiquetas frecuentes (gallego/castellano).
    Devuelve (importe, source).
    """
    labels = [
        # gallego
        "Orzamento base de licitación", "Orzamento", "Importe de licitación",
        # castellano
        "Presupuesto base de licitación", "Importe de licitación", "Presupuesto",
    ]
    for label in labels:
        xp = f"//*[contains(translate(normalize-space(text()),'{label.upper()}','{label.upper()}'))]/following::*[1]"
        val = doc.xpath(f"string({xp})")
        val = _norm(val)
        if val and "€" in val:
            return val, f"label:{label}"
    # fallback: primer “€” de la página
    txt = _norm(doc.text_content() or "")
    m = re.search(r"[\d\.\s]+,\d{2}\s*€", txt)
    if m:
        return _norm(m.group(0)), "fallback:first-euro"
    return "", ""

def parse_detail_madrid(doc: html.HtmlElement) -> dict:
    out = {}
    out["objeto"] = _norm(doc.xpath("string(//h1|//h2)"))
    txt = _norm(doc.text_content() or "")

    m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", txt)
    if m: out["expediente"] = m.group(0)

    for label in ["Órgano de contratación", "Entidad adjudicadora", "Organismo"]:
        xp = f"//*[contains(translate(text(),'{label.upper()}','{label.upper()}'),'{label.upper()}')]/following::*[1]"
        val = _norm(doc.xpath(f"string({xp})"))
        if val:
            out["organo"] = val; break

    imp, src = _extract_amount_by_labels(doc)
    out["importe"], out["importe_source"] = imp, src

    cpvs = extract_cpvs_from_text(txt)
    if cpvs: out["cpv"] = ";".join(sorted(set(cpvs))); out["cpv_source"] = "detail"
    return out

def parse_detail_galicia(doc: html.HtmlElement) -> dict:
    out = {}
    out["objeto"] = _norm(doc.xpath("string(//h1|//h2)"))
    txt_all = _norm(doc.text_content() or "")

    m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", txt_all)
    if m: out["expediente"] = m.group(0)

    for label in ["Órgano de contratación", "Organismo", "Órgano"]:
        xp = f"//*[contains(translate(text(),'{label.upper()}','{label.upper()}'),'{label.upper()}')]/following::*[1]"
        val = _norm(doc.xpath(f"string({xp})"))
        if val:
            out["organo"] = val; break

    imp, src = _extract_amount_by_labels(doc)
    out["importe"], out["importe_source"] = imp, src

    # --- CPV: tabla específica + fallback ---
    cpv_set = set()
    cells = doc.xpath(
        "//*[contains(translate(normalize-space(.),'CPV','cpv'),'cpv')]/"
        "ancestor::*[self::div or self::section or self::table][1]"
        "//table//tr/td[1] | "
        "//*[contains(translate(normalize-space(.),'VOCABULARIO COMÚN','VOCABULARIO COMÚN') )]/"
        "ancestor::*[self::div or self::section or self::table][1]"
        "//table//tr/td[1]"
    )
    for td in cells:
        code_text = _norm(td.text_content())
        m = re.search(r"(\d{8})(?:-\d)?", code_text)
        if m: cpv_set.add(m.group(1))

    if not cpv_set:
        cpv_set.update(extract_cpvs_from_text(txt_all))

    if cpv_set:
        out["cpv"] = ";".join(sorted(cpv_set)); out["cpv_source"] = "detail"
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
            return {"detail_status":"fetch-failed"}
        host = (urlparse(url).hostname or "").lower()
        fn = None
        for dom, f in PARSERS_BY_DOMAIN.items():
            if dom in host: fn = f; break
        if fn:
            out = fn(doc); out["detail_status"] = "parsed"
            return out
        # Fallback genérico
        out = {}
        out["objeto"] = _norm(doc.xpath("string(//h1|//h2)"))
        txt = _norm(doc.text_content() or "")
        m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", txt)
        if m: out["expediente"] = m.group(0)
        imp, src = _extract_amount_by_labels(doc)
        out["importe"], out["importe_source"] = imp, src
        cpvs = extract_cpvs_from_text(txt)
        if cpvs: out["cpv"] = ";".join(sorted(set(cpvs))); out["cpv_source"]="detail"
        out["detail_status"] = "fallback"
        return out
    except Exception:
        return {"detail_status":"exception"}

def collect(source_name: str,
            feed_url: str,
            date_start: datetime,
            date_end: datetime,
            cpv_targets: set[str],
            cpv_mode: str = "exact",
            follow_detail: bool = False,
            polite_delay: float = 1.2) -> list[dict]:
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

        title = _norm(getattr(e, "title", "") or "")
        link  = getattr(e, "link", "") or ""
        summary = _norm(getattr(e, "summary", "") or "")
        content = f"{title}\n{summary}"

        expediente = ""
        m = re.search(r"\b[\w\-]+/\d{4}/\d+\b", content)
        if m: expediente = m.group(0)

        organo = ""
        for tag in ["Órgano", "Organismo", "Entidad", "Órgano de contratación"]:
            m = re.search(tag+r".{0,3}:\s*(.+?)(?:<|$|\n)", summary, flags=re.IGNORECASE)
            if m:
                organo = _norm(m.group(1)); break

        importe = ""
        m = re.search(r"[\d\.\s]+,\d{2}\s*€", content)
        if m: importe = _norm(m.group(0)); importe_source = "feed"
        else: importe_source = ""

        cpvs = extract_cpvs_from_text(content); cpv_source = "feed" if cpvs else ""

        if follow_detail and link:
            time.sleep(polite_delay)
            extra = enrich_by_detail(link)
            expediente = extra.get("expediente", expediente)
            organo     = extra.get("organo", organo)
            importe    = extra.get("importe", importe)
            if extra.get("importe_source"): importe_source = extra["importe_source"]
            title      = extra.get("objeto", title)
            if extra.get("cpv"):
                cpvs = sorted(set(cpvs + extra["cpv"].split(";"))); cpv_source = "detail"
            detail_status = extra.get("detail_status","parsed")
        else:
            detail_status = "skipped" if not follow_detail else "no-link"

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
            "enlace": link,
            # diagnóstico
            "cpv_source": cpv_source,
            "importe_source": importe_source,
            "detail_status": detail_status,
        })
    return rows
