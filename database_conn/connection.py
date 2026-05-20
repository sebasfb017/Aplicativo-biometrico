import os
import sqlite3
from contextlib import contextmanager

from config import APP_CONFIG, BASE_DIR

# Definición de rutas base dinámicas basadas en config
db_path_config = APP_CONFIG.get("database", {}).get("path", "data/app.db")

if not os.path.isabs(db_path_config):
    DB_PATH = os.path.join(BASE_DIR, db_path_config)
else:
    DB_PATH = db_path_config

DATA_DIR = os.path.dirname(DB_PATH)

def db_conn():
    """Establece y retorna la conexión a la base de datos SQLite."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

@contextmanager
def db_session():
    """Context manager para la base de datos.
    Asegura commit automático y cierra de forma segura la conexión."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()