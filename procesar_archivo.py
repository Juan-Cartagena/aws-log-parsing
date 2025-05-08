#!/usr/bin/env python3
import os
import glob
import csv
from datetime import datetime

# ------------------------------------------------------------
# 1. Localizar el .csv
# ------------------------------------------------------------
csv_files = glob.glob("*.csv")
if not csv_files:
    raise FileNotFoundError("No se encontró ningún archivo .csv en la carpeta.")
if len(csv_files) > 1:
    raise RuntimeError("Hay más de un archivo .csv en la carpeta, asegúrate de dejar solo uno.")
in_file = csv_files[0]
base_name, _ = os.path.splitext(in_file)
out_file = f"{base_name}_timed.csv"

# ------------------------------------------------------------
# 2. Leer y procesar
# ------------------------------------------------------------
time_format = "%Y-%m-%d %H:%M:%S.%f"   # ej.: 2025-05-08 04:11:31.234

with open(in_file, newline='', encoding="utf-8") as f_in, \
     open(out_file, "w", newline='', encoding="utf-8") as f_out:

    reader = csv.reader(f_in)
    writer = csv.writer(f_out)

    header = next(reader)                      # primera línea con los nombres de columna
    timestamp_idx = 0                          # se asume que @timestamp es la primera columna
    header.extend(["accum_ms", "delta_ms"])    # nuevas columnas
    writer.writerow(header)

    base_time = prev_time = None

    for row in reader:
        if not row:                # línea vacía
            continue
        ts_str = row[timestamp_idx].strip()
        try:
            current_time = datetime.strptime(ts_str, time_format)
        except ValueError as e:
            raise ValueError(f"No se pudo interpretar la marca de tiempo «{ts_str}»: {e}")

        # Tiempo acumulado desde el primer evento
        if base_time is None:
            base_time = current_time
            accum_ms = 0
        else:
            accum_ms = int((current_time - base_time).total_seconds() * 1000)

        # Tiempo desde el evento anterior
        if prev_time is None:
            delta_ms = 0
        else:
            delta_ms = int((current_time - prev_time).total_seconds() * 1000)

        prev_time = current_time

        # Adjuntar los nuevos valores y escribir la fila
        row.extend([accum_ms, delta_ms])
        writer.writerow(row)

print(f"Procesamiento completado. Archivo generado: {out_file}")