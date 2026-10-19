# Plan Técnico: 001 - Seguridad, Manejo de Secretos y Optimización de la Capa de Datos

## 1. Arquitectura de Solución Propuesta

### 1.1. Abstracción Centralizada de Configuración (`config.py`)
- Extender `config.py` para resolver credenciales en este orden de precedencia:
  1. Variables de entorno (`os.environ`).
  2. Archivo `st.secrets` de Streamlit (`.streamlit/secrets.toml`).
  3. Valores por defecto locales no productivos (solo desarrollo).
- Métodos auxiliares: `get_db_dsn()`, `get_smtp_credentials()`, `get_webhook_url()`.

### 1.2. Pool de Conexiones PostgreSQL (`database_conn/connection.py`)
- Reemplazar la creación ad-hoc de `psycopg2.connect` por un `ThreadedConnectionPool`:
  ```python
  from psycopg2.pool import ThreadedConnectionPool
  # minconn=2, maxconn=20
  ```
- Refactorizar `db_session()` como context manager que toma una conexión del pool (`pool.getconn()`), ejecuta en bloque transaccional con `try/commit/rollback` y siempre la devuelve al pool en el bloque `finally` (`pool.putconn(conn)`).
- Mantener compatibilidad transparente con todo el código existente que utiliza `with db_session() as conn:`.

### 1.3. Parametrización Completa de Consultas (`database_conn/queries.py` y `views/`)
- Reemplazar las cláusulas `IN {status_filter}` por listas de `%s`:
  ```python
  # De: WHERE lr.status IN {status_filter}
  # A:
  status_placeholders = ",".join(["%s"] * len(status_list))
  # WHERE lr.status IN ({status_placeholders}) con params agregados al tuple
  ```
- Eliminar la inyección de fragmentos SQL condicionales (`cond_zarzal`, `cond_serv_gen`, `cond_orientador`) reescribiéndolas como condiciones lógicas booleanas parametrizadas dentro de la misma consulta SQL.

### 1.4. Script DDL Declarativo e Idempotente (`database_conn/setup.py`)
- Implementar en `init_db()` la definición formal del esquema relacional:
  - `CREATE TABLE IF NOT EXISTS users_app (...)`
  - `CREATE TABLE IF NOT EXISTS employees (...)`
  - `CREATE TABLE IF NOT EXISTS leave_requests (...)`
  - `CREATE TABLE IF NOT EXISTS attendance_raw (...)`
  - `CREATE TABLE IF NOT EXISTS biometric_id_map (...)`
  - `CREATE TABLE IF NOT EXISTS audit_logs (...)`
  - Índices clave sobre campos de búsqueda frecuente: `leave_requests(user_id, status)`, `attendance_raw(user_id, ts, device_name)`.

---

## 2. Plan de Rollback y Mitigación de Riesgos
- **Riesgo**: Agotamiento del pool si una sesión no libera la conexión.
  - *Mitigación*: Uso obligatorio de `finally: pool.putconn(conn)` dentro de `db_session()`. Timeout de checkout en pool.
- **Riesgo**: Bloqueo de inicio de la aplicación si faltan variables de entorno en servidores existentes.
  - *Mitigación*: Mantener como valor de respaldo la configuración de red actual (`nomina_user:nomina_password@localhost:5432/nomina_db`), emitiendo una advertencia de seguridad (`logging.warning`).
