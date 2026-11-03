import os
import sys
sys.path.insert(0, "/app")
import re
from datetime import datetime
import PyPDF2
from database_conn.connection import db_session
from database_conn.queries import log_audit, get_cached_dataframe

pdf_path = "/app/data/marcaciones_zarzal_oct2026.pdf"

print("--- 1. Extrayendo marcaciones del PDF ---", flush=True)
reader = PyPDF2.PdfReader(pdf_path)
total_pages = len(reader.pages)
print(f"Total páginas en PDF: {total_pages}", flush=True)

all_raw_lines = []
for p_idx in range(total_pages):
    text = reader.pages[p_idx].extract_text()
    for line in text.split("\n"):
        line = line.strip()
        if not line or "Número" in line or "Tipo de Registro" in line:
            continue
        all_raw_lines.append((p_idx + 1, line))

print(f"Total líneas de registros: {len(all_raw_lines)}", flush=True)

parsed_records = []
device_ip = "192.168.101.56"
downloaded_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for page_num, line in all_raw_lines:
    m = re.match(
        r"^(\d+)\s+(.+?)\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}:\d{2})\s+([ap]\.?\s*m\.?)\s*(Entrada|Salida)?\s+([A-Za-z0-9_]+)\s+(\d+)$",
        line,
        re.IGNORECASE,
    )
    if not m:
        print(f"Error parsing line on page {page_num}: {line}", flush=True)
        continue

    cedula, nombre, f_date, f_time, am_pm, estado, dispositivo, tipo_reg = m.groups()
    day, month, year = map(int, f_date.split("/"))
    hour, minute, second = map(int, f_time.split(":"))
    is_pm = "p" in am_pm.lower()
    if is_pm and hour < 12:
        hour += 12
    elif not is_pm and hour == 12:
        hour = 0
    ts_str = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
    punch = 1 if (estado and estado.lower() == "salida") else 0

    parsed_records.append({
        "device_name": dispositivo,
        "device_ip": device_ip,
        "user_id": cedula,
        "nombre": nombre,
        "ts": ts_str,
        "status": 0,
        "punch": punch,
        "uid": 0,
        "downloaded_at": downloaded_at,
    })

print(f"Marcaciones parseadas correctamente: {len(parsed_records)}", flush=True)

# 2. Verificar correspondencia con empleados
print("\n--- 2. Verificando correspondencia con colaboradores en BD ---", flush=True)
with db_session() as conn:
    with conn.cursor() as cur:
        unique_uids = list({r["user_id"] for r in parsed_records})
        cur.execute(
            """
            SELECT e.user_id, e.full_name, e.department, ua.emp_area, ua.emp_subarea 
            FROM employees e 
            LEFT JOIN users_app ua ON e.user_id = ua.username 
            WHERE e.user_id = ANY(%s)
            """,
            (unique_uids,),
        )
        emp_db = {row[0]: row for row in cur.fetchall()}

        for uid in sorted(unique_uids):
            rec_count = sum(1 for r in parsed_records if r["user_id"] == uid)
            if uid in emp_db:
                row = emp_db[uid]
                print(f"  ✅ Cédula {uid} vinculada: {row[1]} | Dpto: {row[2]} ({rec_count} marcaciones)", flush=True)
            else:
                print(f"  ⚠️ Cédula {uid} NO encontrada en employees ({rec_count} marcaciones)", flush=True)

# 3. Insertar en attendance_raw
print("\n--- 3. Insertando en tabla attendance_raw ---", flush=True)
insert_data = [
    (
        r["device_name"],
        r["device_ip"],
        r["user_id"],
        r["ts"],
        r["status"],
        r["punch"],
        r["uid"],
        r["downloaded_at"],
    )
    for r in parsed_records
]

with db_session() as conn:
    with conn.cursor() as cur:
        # Usar ON CONFLICT DO NOTHING sobre la llave única (device_ip, user_id, ts, status, punch, uid)
        cur.executemany(
            """
            INSERT INTO attendance_raw (device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at, is_ignored, is_manual)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0)
            ON CONFLICT (device_ip, user_id, ts, status, punch, uid) DO NOTHING
            """,
            insert_data,
        )
        inserted = cur.rowcount
        skipped = len(parsed_records) - inserted

print(f"Resultado de inserción: {inserted} insertadas nuevas, {skipped} ya existían (sin duplicar).", flush=True)

# 4. Registrar auditoría y limpiar caché
log_audit(
    "IMPORT_ATTENDANCE",
    f"Carga masiva PDF ZARZAL: {len(parsed_records)} procesadas ({inserted} nuevas, {skipped} previas).",
)
get_cached_dataframe.clear()
print("Auditoría registrada y caché de la app invalidado exitosamente.", flush=True)
