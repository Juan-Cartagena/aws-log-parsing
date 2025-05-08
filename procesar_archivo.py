#!/usr/bin/env python3
"""
procesar_log.py
Lee el primer archivo .txt que encuentre en el directorio donde se ejecuta
y genera un .csv con dos columnas extra:
  • ms_desde_inicio
  • ms_desde_anterior
"""

import csv
import re
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# 1. Localizar el archivo .txt
# ---------------------------------------------------------------------------
txt_files = list(Path('.').glob('*.txt'))
if not txt_files:
    raise SystemExit('No se encontró ningún archivo .txt en la carpeta.')

txt_path = txt_files[0]
csv_path = txt_path.with_suffix('.csv')

# ---------------------------------------------------------------------------
# 2. Expresión regular para extraer la marca de tiempo ISO-8601
#    Ejemplo: 2025-05-08T07:14:42.271-05:00
# ---------------------------------------------------------------------------
TS_REGEX = re.compile(
    r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[-+]\d{2}:\d{2})'
)

# ---------------------------------------------------------------------------
# 3. Leer, procesar y escribir CSV
# ---------------------------------------------------------------------------
rows = []

with txt_path.open(encoding='utf-8') as fh:
    for raw_line in fh:
        line = raw_line.strip()

        # Saltar líneas vacías
        if not line:
            continue

        # Quitar corchetes inicial/final, si existen
        line = line.lstrip('[').rstrip(']')

        # Extraer timestamp
        m = TS_REGEX.search(line)
        if not m:
            # Si la línea no contiene timestamp, omitirla o registrar error
            continue

        ts_str = m.group(1)
        msg = line[len(ts_str):].lstrip()  # resto de la línea sin el timestamp

        rows.append((ts_str, msg))

# Si no hay filas, terminar
if not rows:
    raise SystemExit('No se encontraron timestamps en el archivo.')

# ---------------------------------------------------------------------------
# 4. Calcular diferencias de tiempo
# ---------------------------------------------------------------------------
# Convertir a datetime con zona horaria
timestamps = [datetime.fromisoformat(ts) for ts, _ in rows]

t0 = timestamps[0]

ms_from_start = []
ms_from_prev = []

prev_time = t0
for t in timestamps:
    delta_start = (t - t0).total_seconds() * 1000
    delta_prev  = (t - prev_time).total_seconds() * 1000
    ms_from_start.append(int(delta_start))
    ms_from_prev.append(int(delta_prev))
    prev_time = t

# ---------------------------------------------------------------------------
# 5. Escribir el CSV
# ---------------------------------------------------------------------------
with csv_path.open('w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    # Cabecera
    writer.writerow([
        'timestamp',
        'mensaje',
        'ms_desde_inicio',
        'ms_desde_anterior'
    ])

    for (ts_str, msg), ms_inicio, ms_prev in zip(rows, ms_from_start, ms_from_prev):
        writer.writerow([ts_str, msg, ms_inicio, ms_prev])

print(f'Archivo CSV generado: {csv_path.name}')