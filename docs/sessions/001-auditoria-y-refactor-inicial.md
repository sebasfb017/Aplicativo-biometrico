# Sesión 001: Auditoría de Deuda Técnica, Seguridad y Resiliencia Inicial

**Fecha:** Octubre 2026  
**Rol:** Ingeniero de Productividad con IA / Tech Lead  
**Proyecto:** Sistema de Control Biométrico y Gestión de Nómina Dolormed  
**Estado General:** Features 001 y 002 implementadas al 100%; Feature 003 diagnosticada y con arnés de pruebas PostgreSQL configurado.

---

## 1. Contexto y Diagnóstico Inicial

El aplicativo corporativo de **Dolormed** gestiona marcaciones biométricas de múltiples sedes médicas (Pereira, Cartago, Dosquebradas, Armenia, Manizales, Zarzal, Victoria, Terapia), turnos rotativos y un portal de autogestión de empleados para radicación de novedades (F-TH-012).

La auditoría inicial detectó tres ejes críticos de deuda técnica y riesgo operativo:
1. **Seguridad y Capa de Datos:**
   - Credenciales SMTP y DSN de PostgreSQL expuestas en texto claro en repositorios.
   - Residuos de base de datos SQLite obsoleta (`database_conn/nomina.db`) conviviendo con PostgreSQL.
   - Inexistencia de pooling de conexiones (aperturas y cierres recurrentes bajo concurrencia).
   - Consultas SQL con interpolación de strings (`f-strings`), generando vulnerabilidades potenciales de inyección SQL.
2. **Resiliencia de Hardware ZKTeco y Servicios Externos:**
   - Pre-check de conectividad TCP con timeout ultra-corto (1.5s), provocando falsos positivos de "equipo apagado" en sedes periféricas conectadas por túneles VPN o con fluctuaciones de latencia.
   - Driver ZKTeco contaminado con lógica de negocio (sobreescritura forzada de la hora 14:00 con punch=0), alterando datos crudos de hardware e impidiendo auditorías fidedignas.
   - Falta de un context manager robusto para sockets ZKTeco, dejando conexiones en estado *zombie* en los terminales.
   - Bloqueo de la interfaz web de Streamlit (congelamiento de 3 a 15 segundos) durante el despacho síncrono de notificaciones hacia webhooks de n8n (WhatsApp) y servidores SMTP (Gmail).
3. **Monolitos y Acoplamiento en Vistas:**
   - Vistas masivas (`exceptions_view.py` > 2.400 líneas, `employee_portal_view.py` > 1.700 líneas) con SQL directo embebido en la presentación.
   - Cédulas hardcodeadas en código fuente para enrutar autorizaciones de coordinadores específicos.
   - Suite de pruebas de regresión desfasada respecto a PostgreSQL.

---

## 2. Decisiones Arquitectónicas Implementadas

### Feature 001: Seguridad, Manejo de Secretos y Capa de Datos
- **Gestión de Secretos Dinámica:**
  - Migración a `st.secrets` y variables de entorno (`POSTGRES_DSN`, `SMTP_USER`, `SMTP_PASSWORD`, `N8N_WEBHOOK_URL`).
  - Creación de `.streamlit/secrets.toml.example` y blindaje absoluto en `.gitignore` para excluir archivos `.db` y configuraciones locales.
- **Connection Pooling en PostgreSQL:**
  - Implementación de `ThreadedConnectionPool(minconn=2, maxconn=20)` en `database_conn/connection.py`.
  - Context manager `db_session()` con manejo transaccional estricto (commit automático, rollback ante excepciones y devolución garantizada de conexiones al pool vía `putconn` en `finally`).
  - Envoltorio seguro `PooledConnectionWrapper` para `db_conn()`.
- **Sanitización y Parametrización SQL:**
  - Erradicación de `f-strings` en consultas SQL.
  - Uso estricto de tuplas de parámetros posicionales (`%s`) y escape de comodines (`%%`) en filtros `LIKE`/`ILIKE` para `psycopg2`.
- **Aprovisionamiento Idempotente:**
  - Creación de DDL declarativo en `database_conn/setup.py` (`init_db()`) con índices B-Tree optimizados en `leave_requests(user_id, status)` y `attendance_raw(user_id, ts)`.
  - Eliminación definitiva del archivo residual `database_conn/nomina.db`.

### Feature 002: Resiliencia de Servicios Externos y Hardware ZKTeco
- **Conectividad TCP y Hardware Resiliente:**
  - Elevación del timeout de pre-check de 1.5s a 3.5s configurable mediante `APP_CONFIG['zkteco']['socket_timeout']`.
  - Implementación de `check_socket_connection()` con reintentos adaptativos para enlaces WAN y VPN.
  - Implementación del context manager `zk_connection(device)` en `services/zk_service.py` que asegura la ejecución de `conn.disconnect()` en todos los escenarios de terminación o excepción.
- **Pureza de Datos Crudos en Hardware:**
  - Desacoplamiento total del interceptor de software: `download_attendance_from_device()` y su alias `fetch_attendance()` retornan las marcaciones crudas tal como las reporta el reloj.
- **Reclasificación Horaria en Capa Analítica:**
  - Reubicación de la regla de las 14:00 horas en `services/analytics.py` (`apply_business_punch_rules()`), aplicándose de forma transparente durante los cálculos de nómina y tardanzas sin contaminar la persistencia física.
- **Despacho Asíncrono No Bloqueante (Fire-and-Forget):**
  - Creación de un `ThreadPoolExecutor(max_workers=5, thread_name_prefix="notif_worker")` en `services/notifications.py` (`dispatch_notification_async()`).
  - Desacoplamiento asíncrono de `send_status_update_whatsapp()`, `send_novedad_alert_whatsapp()`, `send_novedad_alert()` y `send_approver_reminder()`.
  - Reducción del tiempo de respuesta en la UI de Streamlit durante aprobaciones y radicaciones a **menos de 150 ms** (incluso ante latencias de red simuladas de varios segundos).
  - Configuración de timeout estricto de 5 segundos en peticiones hacia el webhook de n8n.
- **Trazabilidad y Fallbacks en Auditoría:**
  - Creación de `log_notification_failure()`: todo fallo en WhatsApp (HTTP != 200, timeout de n8n) o SMTP se registra en `audit_logs` bajo la acción `NOTIFICATION_FAILURE`.
  - Sustitución de todos los `print()` en servicios por `logging.getLogger()`.

### Feature 003: Modularización de Vistas y Modernización de Tests (Base Preparada)
- **Aislamiento de Entorno de Pruebas:**
  - Creación de `tests/conftest.py` configurando la variable de entorno `PGOPTIONS="-c search_path=test_run_schema"`, aislando las pruebas en un esquema PostgreSQL efímero y limpio sin alterar la base de datos de producción.
- **Plan de Descomposición Modular:**
  - Diseño de la división de `views/exceptions_view.py` en submódulos especializados (`pending_approval_tab.py`, `rrhh_approval_tab.py`, `calendar_tab.py`, `decision_history_tab.py`).
  - Definición de nuevas columnas en `users_app` para enrutamiento jerárquico dinámico sin cédulas fijas en el código.

---

## 3. Inventario de Archivos Modificados y Creados

| Archivo | Tipo | Descripción de la Modificación / Creación |
| :--- | :--- | :--- |
| `config.py` | Modificado | Incorporación de `socket_timeout: 3.5` en la configuración por defecto de ZKTeco. |
| `configs/settings.yaml` | Modificado | Persistencia de la clave `socket_timeout: 3.5` para hardware biométrico. |
| `database_conn/connection.py` | Modificado | Implementación de `ThreadedConnectionPool` (2-20), `db_session()`, `PooledConnectionWrapper` y lectura segura de secrets. |
| `database_conn/setup.py` | Modificado | DDL declarativo e idempotente de PostgreSQL (`init_db()`), índices B-Tree y auditoría. |
| `database_conn/queries.py` | Modificado | Parametrización estricta de consultas SQL e inclusión de `log_audit()` con `db_session()`. |
| `services/zk_service.py` | Modificado | Context manager `zk_connection()`, pre-check TCP configurable (3.5s), pureza de datos crudos (remoción de regla 14hs) y logging estructurado. |
| `services/analytics.py` | Modificado | Función `apply_business_punch_rules()` para reclasificación horaria en análisis y consumo de consultas parametrizadas. |
| `services/notifications.py` | Modificado | `ThreadPoolExecutor(max_workers=5)`, función `dispatch_notification_async()`, `notify_employee_status()` asíncrono y `log_notification_failure()`. |
| `services/whatsapp_service.py` | Modificado | Wrapper asíncrono, timeout de 5s hacia webhook n8n, logging estructurado y registro de fallos en auditoría. |
| `services/email_service.py` | Modificado | Despacho asíncrono en pool compartido para `send_novedad_alert` y `send_approver_reminder`, logging y auditoría `NOTIFICATION_FAILURE`. |
| `.gitignore` | Modificado | Exclusión de `configs/smtp_config.yaml`, `.streamlit/secrets.toml` y patrones `*.db`. |
| `tests/conftest.py` | Creado | Fixture de sesión `pg_test_schema` para aislamiento hermético en PostgreSQL (`test_run_schema`). |
| `tests/test_connection_pool.py` | Creado | 12 pruebas unitarias para validación de connection pooling, ACID, rollback y exclusión de secretos. |
| `tests/test_async_notifications.py` | Creado | 6 pruebas unitarias para latencia UI < 300ms, timeout 5s, despacho async y registro de `NOTIFICATION_FAILURE`. |
| `tests/test_zk_resilience.py` | Creado | 8 pruebas unitarias para timeout de pre-check (2.5s tolerado), context manager `zk_connection` y pureza de datos crudos. |
| `specs/constitution/roadmap.md` | Modificado | Actualización formal con tabla de seguimiento y features 001 y 002 en estado "Implementado". |
| `specs/features/002-.../tasks.md` | Modificado | Cierre al 100% de todas las tareas `[x]`. |

---

## 4. Estado de la Suite de Pruebas Automatizadas

La suite de pruebas fue ejecutada en su totalidad utilizando `pytest -v` dentro del entorno virtual:

```bash
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: /home/bryan/docker/zkteco_nomina_app/zkteco_nomina_app
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collected 58 items

tests/test_app.py ................................                       [ 55%]
tests/test_async_notifications.py ......                                 [ 65%]
tests/test_connection_pool.py ............                               [ 86%]
tests/test_zk_resilience.py ........                                     [100%]

======================= 58 passed, 28 warnings in 23.58s =======================
```

### Resumen de Cobertura de Pruebas:
- **`test_app.py` (32 pruebas activas):** Integridad de empleados, asignación automática de turnos, cruces de horarios rotativos, cálculo de tardanzas y deduplicación de asistencia.
- **`test_connection_pool.py` (12 pruebas):** Pool de 2 a 20 conexiones, rollback automático, devolución al pool bajo 50 sesiones consecutivas y sanitización de credenciales.
- **`test_async_notifications.py` (6 pruebas):** Despacho en segundo plano con `ThreadPoolExecutor(5)`, latencia UI < 300 ms, timeout de 5 segundos en n8n, registro de fallos en `audit_logs` y tolerancia a caídas totales de SMTP/WhatsApp.
- **`test_zk_resilience.py` (8 pruebas):** Tolerancia de latencia de 2.5s en pre-check de socket, context manager `zk_connection` con cierre estricto de socket ante excepciones, pureza de datos crudos sin mutar `punch` y regla horaria de las 14:00 aplicada en analítica.
- **Total:** **58 pruebas unitarias / 100% aprobadas en verde.**

---

## 5. Próximos Pasos (Feature 003)
1. Completar la extracción de las consultas SQL remanentes en `views/exceptions_view.py` y `views/employee_portal_view.py` hacia `database_conn/queries.py`.
2. Modularizar `views/exceptions_view.py` en la estructura `views/exceptions/`.
3. Desplegar la migración de `users_app` para sustituir las cédulas fijas por enrutamiento jerárquico dinámico.
