import os
import psycopg2
from contextlib import contextmanager
from config import APP_CONFIG, BASE_DIR

POSTGRES_DSN = 'postgresql://nomina_user:nomina_password@localhost:5432/nomina_db'

DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "app.db")

def db_conn():
    """Establece y retorna la conexión a la base de datos PostgreSQL."""
    conn = psycopg2.connect(POSTGRES_DSN)
    return conn


@contextmanager
def db_session():
    """Context manager para la base de datos PostgreSQL.
    Asegura commit automático y cierra de forma segura la conexión."""
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
