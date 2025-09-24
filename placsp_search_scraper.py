#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper UI PLACSP (incluye federadas)
-------------------------------------
- Replica la búsqueda de la web por CPV y fecha de publicación.
- Extrae resultados listados (expediente, objeto, órgano, importe si visible, CPV si visible) y el enlace.
- Opción --deep: entra en cada resultado para extraer CPV/importe del detalle (más lento, más completo).

Uso:
  python placsp_search_scraper.py --date 2025-09-23 --cpv 09330000 45261215 45315300 --out placsp_ui_2025-09-23.csv --headless
  python placsp_search_scraper.py --date 2025-09-23,2025-09-23 --cpv-file cpv.txt --deep --headless

Requisitos:
  pip install playwright pandas lxml
  playwright install chromium
"""

import argparse
import time
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

START_URL = "https://contrataciondelestado.es/wps/portal/plataforma"

def parse_args():
    ap = argparse.ArgumentParser("PLACSP UI scraper")
    ap.add_argument("--date", required=True,
                    help="Fecha publicación YYYY-MM-DD o rango YYYY-MM-DD,YYYY-MM-DD")
    ap.add_argument("--cpv", nargs="+", default=[],
                    help="Lista de CPV (8 dígitos). Se ignora si usas --cpv-file")
    ap.add_argument("--cpv-file", default=None,
                    help="Fichero de CPV (uno por línea)")
    ap.add_argument("--out", default=None, help="CSV de salida")
    ap.add_argument("--headless", action="store_true", help="Navegador sin ventana")
    ap.add_argument("--slow", type=float, default=0.0, help="Retardo entre acciones (seg.)")
    ap.add_argument("--deep", action="store_true",
                    help="Abrir cada resultado para extraer CPV/importe del detalle (más lento)")
    ap.add_argument("--max-pages", type=int, default=0,
                    help="Límite de páginas a recorrer (0 = sin límite)")
    return ap.parse_args()

def _load_cpv_list(args):
    if args.cpv_file:
        with open(args.cpv_file, "r", encoding="utf-8") as fh:
            return [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]
    return args.cpv

def _accept_cookies_if_any(page, slow):
    # Muchos sitios usan “Aceptar”, “Aceptar todas”…
    selectors = [
        "button:has-text('Aceptar')",
        "button:has-text('Aceptar todas')",
        "text=Aceptar >> xpath=ancestor::button",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if btn and btn.count() > 0 and btn.is_visible():
                btn.click()
                time.sleep(0.2 + slow)
                return
        except Exception:
            pass

def _fill_cpv(page, cpv_list, slow):
    # Campo “Código CPV” + botón Añadir
    for cpv in cpv_list:
        try:
            box = page.get_by_label("Código CPV", exact=False)
            if not box or box.count() == 0:
                # fallback por title/aria-label
                box = page.locator("input[title*='CPV'], input[aria-label*='CPV']").first
            box.fill("")
            box.type(cpv)
            time.sleep(0.2 + slow)
            page.get_by_role("button", name=lambda n: n and "Añadir" in n).click()
            time.sleep(0.2 + slow)
        except Exception:
            # si falla uno, sigue con el resto
            continue

def _fill_dates(page, d1, d2, slow):
    # “Fecha publicación entre” → dos inputs
    try:
        label = page.locator("text=Fecha publicación entre").first
        block = label.locator("..")
        inputs = block.locator("input").all()
    except Exception:
        inputs = page.locator("input[type='text'], input[type='date']").all()[:2]
    if len(inputs) >= 2:
        inputs[0].fill(d1); time.sleep(0.1 + slow)
        inputs[1].fill(d2); time.sleep(0.1 + slow)

def _go_to_licitaciones(page, slow):
    # La portada tiene enlaces hacia "Licitaciones" o “Búsqueda avanzada”.
    try:
        page.get_by_role("link", name=lambda n: n and "Licitaciones" in n).first.click()
    except Exception:
        try:
            page.locator("a:has-text('Licitaciones')").first.click()
        except Exception:
            page.wait_for_load_state("networkidle")
    time.sleep(0.3 + slow)

def _click_buscar(page, slow):
    page.get_by_role("button", name=lambda n: n and "Buscar" in n).first.click()
    time.sleep(0.6 + slow)

def _scrape_list_rows(page):
    rows = []
    # capturamos enlaces de cada resultado (detalle)
    links = page.locator("a[target='_blank'], a:has-text('Expediente')")
    n = links.count()
    seen = set()
    for i in range(n):
        a = links.nth(i)
        try:
            href = a.get_attribute("href") or ""
            text = (a.text_content() or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            # subimos al contenedor para extraer metadatos cercanos
            parent = a.locator("xpath=ancestor::*[self::tr or self::div][1]")
            snippet = (parent.text_content() or "").strip()
            expediente, organo, importe, cpv = "", "", "", ""
            # heurísticas sencillas por línea
            for line in snippet.splitlines():
                s = line.strip()
                if not expediente and ("Expediente" in s or "/" in s):
                    # se queda con lo que va tras “Expediente:” o el propio texto con barras
                    expediente = s.split(":",1)[-1].strip()
                if not organo and ("Órgano" in s or "Organismo" in s):
                    organo = s.split(":",1)[-1].strip()
                if not importe and "€" in s:
                    importe = s
                if "CPV" in s and not cpv:
                    cpv = s.split(":",1)[-1].strip()
            rows.append({
                "expediente": expediente,
                "objeto": text,
                "organo": organo,
                "importe": importe,
                "cpv": cpv,
                "enlace": href
            })
        except Exception:
            continue
    return rows

def _scrape_detail(page, url, slow):
    # Abre la URL y rasca CPV/importe/órgano con más precisión
    data = {}
    try:
        page.goto(url)
        time.sleep(0.3 + slow)
        # título
        data["objeto"] = (page.locator("h1, h2").first.text_content() or "").strip()
        # expediente (casi siempre visible)
        # buscamos etiqueta “Expediente”
        try:
            exp_label = page.locator(":text-matches('(?i)expediente')").first
            exp_val = exp_label.locator("xpath=following::*[1]").first.text_content()
            data["expediente"] = (exp_val or "").strip()
        except Exception:
            pass
        # órgano / organismo
        try:
            org_label = page.locator(":text-matches('(?i)(órgano|organismo)')").first
            org_val = org_label.locator("xpath=following::*[1]").first.text_content()
            data["organo"] = (org_val or "").strip()
        except Exception:
            pass
        # importe (primer texto con €)
        try:
            euro = page.locator(":text-matches('€')").first.text_content()
            data["importe"] = (euro or "").strip()
        except Exception:
            pass
        # CPV (cadenas de 8 dígitos cerca de “CPV” o en toda la página)
        try:
            cpv_block = page.locator(":text-matches('(?i)cpv')").first.text_content() or ""
        except Exception:
            cpv_block = page.content()
        import re
        codes = re.findall(r"\b\d{8}\b", cpv_block or "")
        if not codes:
            codes = re.findall(r"\b\d{8}\b", page.content() or "")
        data["cpv"] = ";".join(sorted(set(codes)))
    except Exception:
        pass
    data["enlace"] = url
    return data

def main():
    args = parse_args()
    cpv_list = _load_cpv_list(args)
    out = args.out or f"placsp_ui_{args.date.replace(',','_')}.csv"

    # fechas
    if "," in args.date:
        d1, d2 = [x.strip() for x in args.date.split(",", 1)]
    else:
        d1 = d2 = args.date

    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        ctx = browser.new_context(locale="es-ES")
        page = ctx.new_page()
        page.set_default_timeout(20000)

        # 1) Ir a portal y navegar a Licitaciones
        page.goto(START_URL)
        _accept_cookies_if_any(page, args.slow)
        _go_to_licitaciones(page, args.slow)

        # 2) Rellenar CPV y fechas
        _fill_cpv(page, cpv_list, args.slow)
        _fill_dates(page, d1, d2, args.slow)

        # 3) Buscar
        _click_buscar(page, args.slow)

        # 4) Scrape + paginación
        pages_done = 0
        while True:
            all_rows += _scrape_list_rows(page)
            pages_done += 1
            if args.max-pages := args.max_pages:  # pragma: no cover
                pass
            if args.max_pages and pages_done >= args.max_pages:
                break
            # intenta localizar “Siguiente”
            try:
                next_btn = page.get_by_role("link", name=lambda n: n and "Siguiente" in n).first
                if next_btn and next_btn.is_enabled():
                    next_btn.click()
                    time.sleep(0.6 + args.slow)
                    continue
            except Exception:
                pass
            # alternativa: botón o icono con >
            try:
                nxt = page.locator("a:has-text('Siguiente'), button:has-text('Siguiente'), a[aria-label*='Siguiente']").first
                if nxt and nxt.is_enabled():
                    nxt.click()
                    time.sleep(0.6 + args.slow)
                    continue
            except Exception:
                pass
            break  # no hay más

        # 5) Deep (opcional)
        if args.deep and all_rows:
            detail = ctx.new_page()
            for i, r in enumerate(all_rows):
                try:
                    d = _scrape_detail(detail, r["enlace"], args.slow)
                    # merge conservador: prioriza lo que venga del detalle si existe
                    for k, v in d.items():
                        if v and (k != "enlace"):
                            r[k] = v
                except Exception:
                    continue
            detail.close()

        browser.close()

    # Export CSV
    df = pd.DataFrame(all_rows).fillna("")
    if not df.empty:
        df = df.drop_duplicates(subset=["enlace"])
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"[ok] Resultados: {len(df)} → {out}")

if __name__ == "__main__":
    main()
