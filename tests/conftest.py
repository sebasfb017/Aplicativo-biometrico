import os
import pytest
import psycopg2
from database_conn.connection import get_postgres_dsn, close_connection_pool
from database_conn.setup import init_db

TEST_SCHEMA = "test_run_schema"


@pytest.fixture(scope="session", autouse=True)
def pg_test_schema():
    """Aisla por completo la suite de pruebas en un esquema PostgreSQL temporal dedicado."""
    os.environ.setdefault("DEFAULT_ADMIN_PASSWORD", "Cambiar123!")
    dsn = get_postgres_dsn()

    # 1. Crear el esquema aislado conectando directamente sin PGOPTIONS
    raw_conn = psycopg2.connect(dsn)
    raw_conn.autocommit = True
    with raw_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {TEST_SCHEMA}")
    raw_conn.close()

    # 2. Configurar PGOPTIONS para que el pool y todas las conexiones apunten al esquema de pruebas
    os.environ["PGOPTIONS"] = f"-c search_path={TEST_SCHEMA}"
    close_connection_pool()
    init_db()

    yield TEST_SCHEMA

    # 3. Limpieza al finalizar la sesión completa de pruebas
    close_connection_pool()
    if "PGOPTIONS" in os.environ:
        del os.environ["PGOPTIONS"]
    try:
        clean_conn = psycopg2.connect(dsn)
        clean_conn.autocommit = True
        with clean_conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
        clean_conn.close()
    except Exception:
        pass
