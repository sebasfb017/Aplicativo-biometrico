import os
import threading
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from config import APP_CONFIG, BASE_DIR

DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "app.db")

def get_postgres_dsn() -> str:
    """Resuelve la cadena de conexión priorizando variables de entorno y secrets."""
    # 1. Variables de entorno estándar
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if dsn:
        return dsn

    # 2. Secrets de Streamlit si están disponibles
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "database" in st.secrets:
            sec_dsn = st.secrets["database"].get("dsn")
            if sec_dsn:
                return sec_dsn
    except Exception:
        pass

    # 3. Lectura directa de .streamlit/secrets.toml para ejecución externa a Streamlit
    try:
        secrets_file = os.path.join(BASE_DIR, ".streamlit", "secrets.toml")
        if os.path.exists(secrets_file):
            try:
                import tomllib
                with open(secrets_file, "rb") as f:
                    sec_data = tomllib.load(f)
            except ImportError:
                import toml
                with open(secrets_file, "r", encoding="utf-8") as f:
                    sec_data = toml.load(f)
            sec_dsn = sec_data.get("database", {}).get("dsn")
            if sec_dsn:
                return sec_dsn
    except Exception:
        pass

    # 4. Fallback parametrizado por componentes de entorno
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "nomina_db")
    if user and pwd:
        return f"postgresql://{user}:{pwd}@{host}:{port}/{dbname}"

    raise RuntimeError(
        "Configuración de conexión a PostgreSQL no encontrada. "
        "Defina la variable de entorno DATABASE_URL, POSTGRES_DSN o configure .streamlit/secrets.toml con la clave [database.dsn]."
    )

try:
    POSTGRES_DSN = get_postgres_dsn()
except RuntimeError:
    POSTGRES_DSN = ""

_pool = None
_pool_lock = threading.Lock()

def get_connection_pool(minconn: int = 2, maxconn: int = 20) -> ThreadedConnectionPool:
    """Retorna la instancia global del pool de conexiones para alta concurrencia."""
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                dsn = get_postgres_dsn()
                _pool = ThreadedConnectionPool(minconn, maxconn, dsn)
    return _pool

def close_connection_pool():
    """Cierra todas las conexiones del pool global si está activo."""
    global _pool
    if _pool and not _pool.closed:
        with _pool_lock:
            if _pool and not _pool.closed:
                _pool.closeall()
                _pool = None

class PooledConnectionWrapper:
    """Wrapper para permitir que código legacy que llama conn.close() o with conn devuelva la conexión al pool."""
    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool
        self._closed = False

    def close(self):
        if not self._closed:
            self._closed = True
            if self._conn and not self._conn.closed:
                try:
                    if not self._conn.autocommit:
                        self._conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.autocommit = False
                except Exception:
                    pass
                self._pool.putconn(self._conn)
            else:
                self._pool.putconn(self._conn, close=True)

    def __enter__(self):
        return self._conn.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        ret = self._conn.__exit__(exc_type, exc_val, exc_tb)
        self.close()
        return ret

    def __getattr__(self, name):
        return getattr(self._conn, name)

def db_conn():
    """Establece y retorna una conexión desde el pool con autocommit habilitado."""
    pool = get_connection_pool()
    conn = pool.getconn()
    conn.autocommit = True
    return PooledConnectionWrapper(conn, pool)

@contextmanager
def db_session():
    """Context manager transaccional seguro con Connection Pool.
    Asegura commit automático y devuelve la conexión al pool en el bloque finally."""
    pool = get_connection_pool()
    conn = pool.getconn()
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if conn and not conn.closed:
            try:
                if not conn.autocommit:
                    conn.rollback()
            except Exception:
                pass
            try:
                conn.autocommit = False
            except Exception:
                pass
            pool.putconn(conn)
        else:
            pool.putconn(conn, close=True)
