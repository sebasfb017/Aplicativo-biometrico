# Tareas: 001 - Seguridad, Manejo de Secretos y Optimización de la Capa de Datos

## Lista de Tareas Secuenciales

### Fase 1: Secretos y Configuración
- [x] 1.1 Crear `.streamlit/secrets.toml.example` documentando todas las claves requeridas (`POSTGRES_DSN`, `SMTP_USER`, `SMTP_PASSWORD`, `N8N_WEBHOOK_URL`).
- [x] 1.2 Refactorizar `database_conn/connection.py` para leer `POSTGRES_DSN` desde `os.getenv("DATABASE_URL")` o `st.secrets.get("database", {}).get("dsn")` con fallback seguro.
- [x] 1.3 Refactorizar `services/email_service.py` para cargar credenciales desde `st.secrets` o variables de entorno antes de consultar el archivo YAML local.
- [x] 1.4 Refactorizar `services/whatsapp_service.py` para obtener la URL del webhook de n8n desde `os.getenv("N8N_WHATSAPP_WEBHOOK")` o `st.secrets`.
- [x] 1.5 Verificar que `.gitignore` excluya `configs/smtp_config.yaml`, `.streamlit/secrets.toml` y `*.db`.

### Fase 2: Connection Pooling
- [x] 2.1 Implementar `ThreadedConnectionPool` en `database_conn/connection.py` con rango de 2 a 20 conexiones concurrentes.
- [x] 2.2 Reimplementar `db_session()` garantizando devolución (`putconn`) en bloque `finally`.
- [x] 2.3 Adaptar `db_conn()` para usar el pool o deprecada progresivamente hacia `db_session()`.

### Fase 3: Sanitización y Parametrización SQL
- [x] 3.1 Reemplazar interpolación de `{status_filter}` en `views/exceptions_view.py:383, 412, 432, 444` por tuplas parametrizadas `IN (%s, %s, ...)`.
- [x] 3.2 Eliminar fragmentos de cadena dinámicos (`cond_zarzal`, `cond_serv_gen`, `cond_orientador`) convirtiéndolos a parámetros relacionales limpios.
- [x] 3.3 Revisar `database_conn/queries.py` para garantizar que toda función use tuplas de parámetros posicionales.

### Fase 4: Inicialización DDL y Limpieza
- [x] 4.1 Escribir el DDL de PostgreSQL completo e idempotente en `database_conn/setup.py` (`init_db()`).
- [x] 4.2 Agregar índices B-Tree en `leave_requests(user_id, status)` y `attendance_raw(user_id, ts)`.
- [x] 4.3 Eliminar el archivo residual `database_conn/nomina.db` del árbol de trabajo.

---

## Criterios de Aceptación y Autoverificación con Pytest

1. **Gestión de Secretos**:
   - `python -c "from database_conn.connection import POSTGRES_DSN; assert 'nomina_password' not in open('database_conn/connection.py').read()"`
2. **Resiliencia del Pool de Conexiones**:
   - Ejecutar prueba de concurrencia simulada abriendo y cerrando 50 sesiones consecutivas sin errores de conexión agotada.
3. **Parametrización SQL**:
   - Cero apariciones de `f"SELECT` o `f"UPDATE` con variables concatenadas en `views/exceptions_view.py` y `database_conn/queries.py`.
4. **Prueba Automatizada**:
   - Crear test unitario `tests/test_connection_pool.py` verificando adquisición y devolución correcta de conexiones.
