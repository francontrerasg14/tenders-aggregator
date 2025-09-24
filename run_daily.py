# -*- coding: utf-8 -*-
"""
Agregador multi-fuente: PLACSP 643 + RSS de CCAA (config.yml)
Uso típico:
  python run_daily.py --date 2025-09-23 --when published --cpv 09330000 45261215 45315300 --cpv-scope folder
  python run_daily.py --date 2025-09-22,2025-09-23 --when either --cpv 0933 --cpv-mode prefix
"""
import argparse, csv, os, yaml
from datetime import datetime, timedelta
from dateutil import tz
from collectors.placsp643 import collect as placsp_collect
from collectors.rss_generic import collect as rss_collect
from collectors.utils import normalize_cpv_list, parse_date_any

def parse_args():
    ap = argparse.ArgumentParser("Tenders aggregator (PLACSP + RSS CCAA)")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD o rango YYYY-MM-DD,YYYY-MM-DD (zona Europe/Madrid)")
    ap.add_argument("--when", choices=["updated","published","either"], default="either", help="Solo PLACSP")
    ap.add_argument("--cpv", nargs="+", default=["09330000","45261215","45315300"])
    ap.add_argument("--cpv-mode", choices=["exact","prefix"], default="exact")
    ap.add_argument("--cpv-scope", choices=["folder","lots","both"], default="folder", help="Solo PLACSP")
    ap.add_argument("--config", default="config.yml", help="YAML con rss_sources")
    ap.add_argument("--out", default=None, help="CSV de salida (por defecto: tenders_<fecha>.csv)")
    ap.add_argument("--no-placsp", action="store_true", help="No incluir PLACSP (solo RSS)")
    ap.add_argument("--no-rss", action="store_true", help="No incluir RSS (solo PLACSP)")
    return ap.parse_args()

def madrid_day_bounds(day_str: str):
    tz_mad = tz.gettz("Europe/Madrid")
    d = parse_date_any(day_str)
    if not d: raise SystemExit("Fecha inválida")
    d = d.replace(tzinfo=tz_mad).replace(hour=0, minute=0, second=0, microsecond=0)
    d_end = d + timedelta(days=1) - timedelta(seconds=1)
    return d, d_end

def parse_date_range(arg_date: str):
    if "," in arg_date:
        a, b = [x.strip() for x in arg_date.split(",", 1)]
        start, _ = madrid_day_bounds(a)
        end, _ = madrid_day_bounds(b)
        end = end.replace(hour=23, minute=59, second=59)
        return start, end
    else:
        return madrid_day_bounds(arg_date)

def main():
    args = parse_args()
    start_dt, end_dt = parse_date_range(args.date)
    cpv_targets = normalize_cpv_list(args.cpv, args.cpv_mode)

    rows = []

    # 1) PLACSP por días (si procede)
    if not args.no_placsp:
        cur = start_dt
        while cur.date() <= end_dt.date():
            date_iso = cur.date().isoformat()
            rows += placsp_collect(
                date_iso=date_iso,
                when=args.when,
                cpv=list(cpv_targets),
                cpv_mode=args.cpv_mode,
                cpv_scope=args.cpv_scope
            )
            cur += timedelta(days=1)

    # 2) RSS desde config.yml
    if not args.no_rss and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as fh:
            conf = yaml.safe_load(fh) or {}
        for src in (conf.get("rss_sources") or []):
            name = src.get("name")
            url  = src.get("url")
            follow_detail = bool(src.get("follow_detail", False))
            if not (name and url):
                continue
            rss_rows = rss_collect(
                source_name=name,
                feed_url=url,
                date_start=start_dt,
                date_end=end_dt,
                cpv_targets=cpv_targets,
                cpv_mode=args.cpv_mode,
                follow_detail=follow_detail,
                polite_delay=0.8
            )
            rows += rss_rows

    # 3) De-dupe y salida
    seen = set(); uniq = []
    for r in rows:
        key = (r.get("fuente"), r.get("expediente"), r.get("enlace"))
        if key in seen: continue
        seen.add(key); uniq.append(r)

    out = args.out or f"tenders_{args.date.replace(',','_')}.csv"
    cols = ["fuente","expediente","objeto","organo","estado","importe","cpv","fecha_published","fecha_updated","enlace"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in uniq:
            w.writerow({c: r.get(c, "") for c in cols})

    print(f"[ok] CSV: {out} | filas={len(uniq)} | fuentes: "
          f"{'PLACSP ' if not args.no_placsp else ''}{'RSS' if not args.no_rss else ''}")

if __name__ == "__main__":
    main()
