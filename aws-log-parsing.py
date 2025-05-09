#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
aws-log-parsing.py
Uso:
    python aws-log-parsing.py a.json secuencia.txt secuencia.csv
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from typing import List

try:
    import pytz          # pip install pytz
except ImportError:
    print("ERROR: necesitas instalar pytz ->  pip install pytz")
    sys.exit(1)


ANSI_ESCAPE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub('', text)


def load_patterns(path: str) -> List[str]:
    with open(path, encoding="utf-8") as f:
        return [l.rstrip('\n') for l in f if l.strip()]


def read_events(path: str) -> List[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    events = []
    for value in data.values():           # {logGroupName: [eventos]}
        if isinstance(value, list):
            events.extend(value)
    return events


def parse_ts(ts: str) -> datetime:
    # Formato AWS exportado: 2025-05-08 04:11:39.127   (UTC)
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)


def to_local_iso(dt_utc: datetime, tz_name: str = "America/Bogota") -> str:
    tz_local = pytz.timezone(tz_name)
    return dt_utc.astimezone(tz_local).isoformat(timespec="milliseconds")


def main(json_in: str, txt_in: str, csv_out: str):
    patterns = load_patterns(txt_in)
    events = read_events(json_in)

    matches = []

    for ev in events:
        raw = ev.get("@message", "")
        clean = strip_ansi(raw)

        for pat in patterns:
            if pat in clean:
                ts_utc = parse_ts(ev["@timestamp"])
                matches.append(
                    {
                        "name": ev.get("@entity.KeyAttributes.Name", ""),
                        "ts_utc": ts_utc,
                        "pattern": pat,
                    }
                )
                # si quieres solo la primera coincidencia por evento ->  break

    if not matches:
        print("No se encontraron coincidencias")
        return

    # -----------------------------------------------------------------
    # 1) Orden cronológico y TIMESTAMP de referencia global (el menor)
    # -----------------------------------------------------------------
    matches.sort(key=lambda m: m["ts_utc"])
    first_ts = matches[0]["ts_utc"]            #  <---  # FIX : referencia global

    # -----------------------------------------------------------------
    # 2) Calculamos la columna de milisegundos relativa para *cada* fila
    # -----------------------------------------------------------------
    for m in matches:                          #  # FIX 
        m["rel_ms"] = int((m["ts_utc"] - first_ts).total_seconds() * 1000)
        m["ts_local"] = to_local_iso(m["ts_utc"])

    # -----------------------------------------------------------------
    # 3) Generamos el CSV
    # -----------------------------------------------------------------
    with open(csv_out, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["@entity.KeyAttributes.Name", "@timestamp", "texto_encontrado", "ms_desde_inicio"]
        )
        for m in matches:
            writer.writerow([m["name"], m["ts_local"], m["pattern"], m["rel_ms"]])

    print(f"CSV generado: {csv_out}  ({len(matches)} filas)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Parsea un dump de AWS CWLogs y busca patrones")
    ap.add_argument("json_in")
    ap.add_argument("txt_in")
    ap.add_argument("csv_out")
    args = ap.parse_args()

    main(args.json_in, args.txt_in, args.csv_out)