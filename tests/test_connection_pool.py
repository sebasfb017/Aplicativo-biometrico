import os
import re
import pytest
from database_conn.connection import (
    get_connection_pool,
    db_session,
    db_conn,
    get_postgres_dsn,
    PooledConnectionWrapper,
)
from database_conn.setup import init_db


def test_no_hardcoded_credentials():
    """Criterio 1: No debe existir contraseñas en texto claro en connection.py."""
    with open("database_conn/connection.py", "r", encoding="utf-8") as f:
        content = f.read()
    assert "nomina_password" not in content


def test_connection_pool_instance():
    """Fase 2.1: El pool debe instanciarse con ThreadedConnectionPool (min 2, max 20)."""
    pool = get_connection_pool(2, 20)
    assert pool is not None
    assert pool.minconn == 2
    assert pool.maxconn == 20


def test_db_session_commit_and_cleanup():
    """Fase 2.2: db_session debe ejecutar consultas y devolver la conexión al pool."""
    with db_session() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            res = cur.fetchone()[0]
            assert res == 1


def test_db_session_rollback_on_error():
    """Fase 2.2: db_session debe hacer rollback y devolver la conexión al pool si ocurre un error."""
    with pytest.raises(Exception):
        with db_session() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM non_existent_table_xyz_123")


def test_pooled_connection_wrapper_close():
    """Fase 2.3: db_conn debe devolver PooledConnectionWrapper que regresa la conexión al pool al llamar close()."""
    conn = db_conn()
    assert isinstance(conn, PooledConnectionWrapper)
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    conn.close()
    assert conn._closed is True


def test_concurrency_resilience_50_sessions():
    """Criterio 2: Ejecutar 50 adquisiciones y devoluciones consecutivas sin agotar el pool."""
    for i in range(50):
        with db_session() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT %s", (i,))
                val = cur.fetchone()[0]
                assert val == i


def test_init_db_idempotent():
    """Fase 4.1 & 4.2: init_db debe ejecutarse de forma declarativa e idempotente."""
    init_db()
    init_db()  # Segunda ejecución no debe lanzar error ni violar unicidad


def test_zero_sql_fstrings():
    """Criterio 3: Cero apariciones de SQL f-strings en exceptions_view.py y queries.py."""
    for filepath in ["views/exceptions_view.py", "database_conn/queries.py"]:
        with open(filepath, "r", encoding="utf-8") as f:
            code = f.read()
        matches = re.findall(
            r'f[\"\']{1,3}\s*(?:SELECT|UPDATE|INSERT|DELETE)', code, re.IGNORECASE
        )
        assert len(matches) == 0, f"Found SQL f-string in {filepath}: {matches}"


def test_email_service_env_override(monkeypatch):
    """Fase 1.3: email_service debe cargar credenciales desde variables de entorno."""
    from services.email_service import load_smtp_config
    monkeypatch.setenv("SMTP_USER", "test_user@dolormed.com")
    monkeypatch.setenv("SMTP_PASSWORD", "super_secret_smtp_123")
    monkeypatch.setenv("SMTP_SERVER", "smtp.office365.com")
    monkeypatch.setenv("SMTP_PORT", "587")

    cfg = load_smtp_config()
    assert cfg["smtp_user"] == "test_user@dolormed.com"
    assert cfg["smtp_password"] == "super_secret_smtp_123"
    assert cfg["smtp_server"] == "smtp.office365.com"
    assert cfg["smtp_port"] == 587


def test_whatsapp_service_env_override(monkeypatch):
    """Fase 1.4: whatsapp_service debe cargar webhook URL desde variables de entorno."""
    from services.whatsapp_service import get_n8n_webhook_url
    test_url = "https://n8n.customdomain.com/webhook/test-token"
    monkeypatch.setenv("N8N_WHATSAPP_WEBHOOK", test_url)
    assert get_n8n_webhook_url() == test_url

    monkeypatch.delenv("N8N_WHATSAPP_WEBHOOK", raising=False)
    monkeypatch.setenv("N8N_WEBHOOK_URL", "https://n8n-alt.customdomain.com/webhook/test-2")
    assert get_n8n_webhook_url() == "https://n8n-alt.customdomain.com/webhook/test-2"


def test_gitignore_contains_sensitive_patterns():
    """Fase 1.5: .gitignore debe excluir smtp_config.yaml, secrets.toml, *.db y .env."""
    with open(".gitignore", "r", encoding="utf-8") as f:
        gitignore_content = f.read()
    assert "smtp_config.yaml" in gitignore_content
    assert "secrets.toml" in gitignore_content
    assert "*.db" in gitignore_content
    assert ".env" in gitignore_content


def test_acid_rollback_after_db_conn_release():
    """Hallazgo 2: Verificar que tras usar db_conn(autocommit=True), db_session respete rollback si hay excepción."""
    # 1. Tomar y cerrar conexión desde db_conn (la cual activa autocommit=True)
    c = db_conn()
    assert c.autocommit is True
    c.close()

    # 2. Tomar conexión con db_session, insertar un registro temporal y provocar excepción
    test_token = "probe_acid_test_token_123"
    with pytest.raises(RuntimeError):
        with db_session() as conn:
            assert conn.autocommit is False
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO user_sessions (token, username, created_at, expires_at) VALUES (%s, %s, %s, %s)",
                    (test_token, "admin", "2026-01-01", "2026-01-02"),
                )
            raise RuntimeError("Fallo intencional para verificar rollback")

    # 3. Comprobar que el registro NO quedó persistido en la base de datos
    with db_session() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_sessions WHERE token = %s", (test_token,))
            count = cur.fetchone()[0]
            assert count == 0

