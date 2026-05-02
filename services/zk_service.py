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
            # --- INTERCEPTOR DE SOFTWARE (OVERRIDE) ---
            # El usuario solicita que CUALQUIER marcación que ocurra a las 2:00 PM (14:XX)
            # sea catalogada FORZOSAMENTE como Entrada (Punch 0), ignorando el estado físico del reloj.
            punch_val = int(r.punch)
            if r.timestamp.hour == 14:
                punch_val = 0
                
            out.append({
                "device_name": name,
                "device_ip": ip,
                "user_id": str(r.user_id),
                "ts": r.timestamp.isoformat(sep=" ", timespec="seconds"),
                "status": int(r.status),
                "punch": punch_val,
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

def get_device_users_status(device: dict):
    ip = device["ip"]
    try: port = int(device.get("port", 4370))
    except Exception: port = 4370
    try: password = int(device.get("password", 0))
    except Exception: password = 0
    try: timeout = int(device.get("timeout", 10))
    except Exception: timeout = 10
    
    zk = ZK(ip, port=port, timeout=timeout, password=password)
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()
        users = conn.get_users()
        try:
            templates = conn.get_templates()
            enrolled_uids = {t.uid for t in templates}
        except Exception:
            # Fallback if get_templates is not supported by firmware
            enrolled_uids = set()
        
        result = []
        for u in users:
            result.append({
                "uid": u.uid,
                "user_id": u.user_id,
                "name": u.name,
                "privilege": u.privilege,
                "has_fingerprint": u.uid in enrolled_uids
            })
            
        return result, None
    except Exception as e:
        return [], str(e)
    finally:
        try:
            if conn:
                conn.enable_device()
                conn.disconnect()
        except Exception:
            pass

def upload_user_to_device(device: dict, user_id: str, name: str, privilege: int = 0):
    ip = device["ip"]
    try: port = int(device.get("port", 4370))
    except Exception: port = 4370
    try: password = int(device.get("password", 0))
    except Exception: password = 0
    try: timeout = int(device.get("timeout", 10))
    except Exception: timeout = 10
    
    zk = ZK(ip, port=port, timeout=timeout, password=password)
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()
        
        users = conn.get_users()
        existing_uid = None
        for u in users:
            if u.user_id == str(user_id):
                existing_uid = u.uid
                break
                
        if existing_uid is None:
            max_uid = max([u.uid for u in users]) if users else 0
            existing_uid = max_uid + 1
            
        conn.set_user(uid=existing_uid, name=str(name), privilege=privilege, password="", group_id="", user_id=str(user_id))
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

def delete_user_from_device(device: dict, uid: int):
    ip = device["ip"]
    try: port = int(device.get("port", 4370))
    except Exception: port = 4370
    try: password = int(device.get("password", 0))
    except Exception: password = 0
    try: timeout = int(device.get("timeout", 10))
    except Exception: timeout = 10
    
    zk = ZK(ip, port=port, timeout=timeout, password=password)
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()
        conn.delete_user(uid=uid)
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

def sync_all_devices(devices_list: list):
    """
    Sincroniza usuarios y huellas dactilares entre TODOS los relojes.
    Paso 1: Descargar el 'maestro' sumando los usuarios y huellas de todos.
    Paso 2: Subir lo que le falte a cada reloj.
    """
    master_users = {} # user_id_str -> {name, privilege, password, group_id}
    master_templates = {} # user_id_str -> dict de {fid: TemplateObject}
    logs = []
    
    # --- PASO 1: RECOLECTAR MAESTRO ---
    for dev in devices_list:
        ip = dev["ip"]
        try: port = int(dev.get("port", 4370))
        except Exception: port = 4370
        try: password = int(dev.get("password", 0))
        except Exception: password = 0
        try: timeout = int(dev.get("timeout", 10))
        except Exception: timeout = 10
        
        zk = ZK(ip, port=port, timeout=timeout, password=password)
        conn = None
        try:
            conn = zk.connect()
            conn.disable_device()
            
            users = conn.get_users()
            try:
                templates = conn.get_templates()
            except Exception:
                templates = []
                
            uid_to_userid = {}
            for u in users:
                uid_str = str(u.user_id)
                uid_to_userid[u.uid] = uid_str
                if uid_str not in master_users:
                    master_users[uid_str] = {
                        "name": u.name,
                        "privilege": u.privilege,
                        "password": getattr(u, 'password', ''),
                        "group_id": getattr(u, 'group_id', '')
                    }
                    master_templates[uid_str] = {}
            
            for t in templates:
                uid_str = uid_to_userid.get(t.uid)
                if uid_str:
                    if uid_str not in master_templates:
                        master_templates[uid_str] = {}
                    master_templates[uid_str][t.fid] = t
                    
            logs.append(f"✅ {ip}: Extraídos {len(users)} usuarios y {len(templates)} huellas.")
        except Exception as e:
            logs.append(f"❌ {ip}: Error leyendo - {e}")
        finally:
            try:
                if conn:
                    conn.enable_device()
                    conn.disconnect()
            except Exception:
                pass
                
    # --- PASO 2: DISTRIBUIR MAESTRO ---
    for dev in devices_list:
        ip = dev["ip"]
        try: port = int(dev.get("port", 4370))
        except Exception: port = 4370
        try: password = int(dev.get("password", 0))
        except Exception: password = 0
        try: timeout = int(dev.get("timeout", 10))
        except Exception: timeout = 10
        
        zk = ZK(ip, port=port, timeout=timeout, password=password)
        conn = None
        try:
            conn = zk.connect()
            conn.disable_device()
            
            dev_users = conn.get_users()
            try:
                dev_templates = conn.get_templates()
            except Exception:
                dev_templates = []
                
            dev_userid_to_uid = {str(u.user_id): u.uid for u in dev_users}
            uid_to_userid = {u.uid: str(u.user_id) for u in dev_users}
            dev_template_keys = set()
            for t in dev_templates:
                user_id_str = uid_to_userid.get(t.uid)
                if user_id_str:
                    dev_template_keys.add((user_id_str, t.fid))
            
            # Subir Usuarios Faltantes
            users_created = 0
            max_uid = max([u.uid for u in dev_users]) if dev_users else 0
            for uid_str, u_data in master_users.items():
                if uid_str not in dev_userid_to_uid:
                    max_uid += 1
                    conn.set_user(uid=max_uid, name=u_data["name"], privilege=u_data["privilege"], password=u_data["password"], group_id=u_data["group_id"], user_id=uid_str)
                    dev_userid_to_uid[uid_str] = max_uid
                    users_created += 1
                    
            if users_created > 0:
                dev_users = conn.get_users()
                
            dev_userid_to_user_obj = {str(u.user_id): u for u in dev_users}
            
            # Subir Huellas Faltantes
            templates_created = 0
            for uid_str, templates_dict in master_templates.items():
                target_user = dev_userid_to_user_obj.get(uid_str)
                if not target_user:
                    continue
                    
                missing_fingers = []
                for fid, master_t in templates_dict.items():
                    if (uid_str, fid) not in dev_template_keys:
                        master_t.uid = target_user.uid
                        missing_fingers.append(master_t)
                        
                if missing_fingers:
                    conn.save_user_template(target_user, missing_fingers)
                    templates_created += len(missing_fingers)
                    
            logs.append(f"🚀 {ip}: {users_created} usuarios inyectados, {templates_created} huellas inyectadas.")
        except Exception as e:
            logs.append(f"❌ {ip}: Error escribiendo - {e}")
        finally:
            try:
                if conn:
                    conn.enable_device()
                    conn.disconnect()
            except Exception:
                pass
                
    return logs