import os
import yaml
from datetime import datetime
from zk import ZK
from database_conn.connection import db_conn, BASE_DIR

# Apuntamos dinámicamente al archivo de configuración de los relojes
DEVICES_YAML = os.path.join(BASE_DIR, "devices.yaml")

def load_devices():
    """Lee y retorna la lista de dispositivos desde la configuración YAML."""
    if not os.path.exists(DEVICES_YAML):
        return []
    try:
        with open(DEVICES_YAML, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except yaml.YAMLError:
        return []

    devices = cfg.get("devices", [])
    if not isinstance(devices, list):
        return []

    valid = []
    for d in devices:
        if isinstance(d, dict) and d.get("ip"):
            valid.append(d)
    return valid

def save_devices(devices_list: list):
    """Guarda la lista de dispositivos actualizados en devices.yaml"""
    try:
        with open(DEVICES_YAML, "w", encoding="utf-8") as f:
            yaml.dump({"devices": devices_list}, f, allow_unicode=True, sort_keys=False)
        return True
    except Exception as e:
        print(f"Error guardando devices.yaml: {e}")
        return False

def download_attendance_from_device(device: dict):
    """Conecta por UDP al reloj y descarga las marcaciones en crudo."""
    ip = device["ip"]
    try:
        port = int(device.get("port", 4370))
    except Exception:
        port = device.get("port", 4370)
    password = device.get("password", 0)
    try:
        password = int(password)
    except Exception:
        pass
    try:
        timeout = int(device.get("timeout", 10))
    except Exception:
        timeout = device.get("timeout", 10)
    name = device.get("name", ip)

    zk = ZK(ip, port=port, timeout=timeout, password=password)
    conn = None
    downloaded_at = datetime.now().isoformat(timespec="seconds")

    try:
        conn = zk.connect()
        conn.disable_device()  # evita actividad mientras descargas
        records = conn.get_attendance()
        out = []
        for r in records:
            out.append({
                "device_name": name,
                "device_ip": ip,
                "user_id": str(r.user_id),
                "ts": r.timestamp.isoformat(sep=" ", timespec="seconds"),
                "status": int(r.status),
                "punch": int(r.punch),
                "uid": int(getattr(r, "uid", 0)),
                "downloaded_at": downloaded_at
            })
        return out, None
    except Exception as e:
        return [], str(e)
    finally:
        try:
            if conn:
                conn.enable_device()
                conn.disconnect()
        except Exception:
            pass

def sync_device_time(device: dict):
    """Sincroniza la hora del biométrico con la del servidor local."""
    ip = device["ip"]
    try: port = int(device.get("port", 4370))
    except Exception: port = device.get("port", 4370)
    password = device.get("password", 0)
    try: password = int(password)
    except Exception: pass
    try: timeout = int(device.get("timeout", 10))
    except Exception: timeout = device.get("timeout", 10)
    name = device.get("name", ip)

    zk = ZK(ip, port=port, timeout=timeout, password=password)
    conn = None

    try:
        conn = zk.connect()
        conn.disable_device()
        now = datetime.now()
        conn.set_time(now)
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            if conn:
                conn.enable_device()
                conn.disconnect()
        except Exception:
            pass

def upsert_attendance(rows: list[dict]):
    """Inserta las marcaciones en la BD evitando duplicados."""
    if not rows:
        return 0, 0
    
    data = [(
        r["device_name"], r["device_ip"], r["user_id"], r["ts"],
        r["status"], r["punch"], r["uid"], r["downloaded_at"]
    ) for r in rows]

    conn = db_conn()
    cur = conn.cursor()
    
    cur.executemany("""
        INSERT OR IGNORE INTO attendance_raw(device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at)
        VALUES(?,?,?,?,?,?,?,?)
    """, data)
    
    inserted = cur.rowcount
    skipped = len(rows) - inserted

    conn.commit()
    conn.close()
    return inserted, skipped