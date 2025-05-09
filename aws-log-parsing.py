#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
aws-log-parsing.py
Ejemplo de uso:
    python aws-log-parsing.py a.json secuencia.txt secuencia.csv
Requiere:
    pip install pytz
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

try:
    import pytz        # Para convertir la hora UTC a -05
except ImportError:
    print("ERROR: Este script necesita la librería 'pytz'. Ejecute: pip install pytz")
    sys.exit(1)


ANSI_ESCAPE_RE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


def strip_ansi(text: str) -> str:
    """Elimina secuencias ANSI (colores) del texto."""
    return ANSI_ESCAPE_RE.sub('', text)


def load_patterns(txt_path: str) -> list[str]:
    """Carga cada línea del .txt como patrón (ignora vacíos)."""
    with open(txt_path, encoding="utf-8") as fh:
        return [line.rstrip("\n") for line in fh if line.strip()]


def read_events(json_path: str) -> list[dict]:
    """Devuelve una lista plana con todos los objetos de evento del archivo json."""
    with open(json_path, encoding="utf-8") as fh:
        data = json.load(fh)

    events: list[dict] = []
    # El JSON exportado por CW Logs suele ser {logGroup: [eventos, ...], ...}
    for group_value in data.values():
        if isinstance(group_value, list):
            events.extend(group_value)

    return events


def parse_timestamp(ts: str) -> datetime:
    """
    Convierte una cadena 'YYYY-MM-DD HH:MM:SS.sss' (UTC) a datetime con zona UTC.
    """
    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return dt


def to_local_iso(dt_utc: datetime, tz_name: str = "America/Bogota") -> str:
    """Pasa un datetime UTC a la zona indicada y lo devuelve en ISO-8601 con milisegundos."""
    tz_local = pytz.timezone(tz_name)
    dt_local = dt_utc.astimezone(tz_local)
    # timespec="milliseconds" -> 2025-05-07T23:11:39.127-05:00
    return dt_local.isoformat(timespec="milliseconds")


def main(json_in: str, txt_in: str, csv_out: str) -> None:
    patterns = load_patterns(txt_in)
    events = read_events(json_in)

    # Coincidencias encontradas
    matches: list[dict] = []

    # Guardaremos el primer timestamp por logStream para calcular tiempos relativos
    first_ts_by_stream: dict[str, datetime] = {}

    for ev in events:
        message_raw: str = ev.get("@message", "")
        message_clean = strip_ansi(message_raw)

        for pat in patterns:
            if pat in message_clean:
                ts = parse_timestamp(ev["@timestamp"])
                stream_id = ev.get("@logStream", "unknown-stream")

                # Guardar primer TS por stream si aún no existe
                first_ts_by_stream.setdefault(stream_id, ts)

                relative_ms = int((ts - first_ts_by_stream[stream_id]).total_seconds() * 1000)

                matches.append(
                    {
                        "name": ev.get("@entity.KeyAttributes.Name", ""),
                        "ts_local": to_local_iso(ts),
                        "pattern": pat,
                        "rel_ms": relative_ms,
                        "ts_utc": ts,  # para poder ordenar cronológicamente después
                    }
                )
                # Un mismo evento podría contener más de un patrón;
                # si sólo queremos uno por evento, añadir break
                # break

    # Ordenamos resultados por momento UTC
    matches.sort(key=lambda x: x["ts_utc"])

    # Escritura CSV
    with open(csv_out, "w", newline='', encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["@entity.KeyAttributes.Name", "@timestamp", "texto_encontrado", "ms_relativo"])
        for m in matches:
            writer.writerow([m["name"], m["ts_local"], m["pattern"], m["rel_ms"]])

    print(f"Se generó {csv_out} con {len(matches)} coincidencias.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Busca patrones dentro de los mensajes de un dump de CloudWatch Logs y genera un CSV."
    )
    ap.add_argument("json_in", help="Archivo .json con los logs exportados de AWS CloudWatch")
    ap.add_argument("txt_in", help="Archivo .txt con los textos/patrones a buscar (uno por línea)")
    ap.add_argument("csv_out", help="Ruta del archivo .csv de salida")
    args = ap.parse_args()

    main(args.json_in, args.txt_in, args.csv_out)