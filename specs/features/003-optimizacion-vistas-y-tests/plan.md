# Plan Técnico: 003 - Optimización de Vistas Monolíticas y Modernización de Tests

## 1. Arquitectura de Solución Propuesta

### 1.1. Extracción y Centralización de Consultas SQL
- Crear en `database_conn/queries.py` las funciones que reemplacen el SQL crudo de las vistas:
  - `db_get_pending_requests_by_role(role, user_dict, managed_area, managed_department)`
  - `db_get_calendar_exceptions(role, start_date, end_date, user_dict)`
  - `db_get_employee_requests_summary(user_id)`
- En `views/exceptions_view.py` y `views/employee_portal_view.py`, reemplazar cada bloque `with db_session() as conn: pd.read_sql_query(...)` por una simple invocación al método correspondiente de `queries.py`.

### 1.2. Descomposición Modular de `views/exceptions_view.py`
Dividir el archivo monolítico en un paquete de responsabilidades delimitadas:
```
views/exceptions/
├── __init__.py               # Re-exporta page_exceptions() para compatibilidad total con app.py
├── pending_approval_tab.py   # Renderizado de tarjetas de aprobación para Coordinadores y Jefes
├── rrhh_approval_tab.py      # Bandeja avanzada de Talento Humano y Nómina
├── calendar_tab.py           # Vista interactiva del Calendario de Ausencias
└── decision_history_tab.py   # Historial consolidado de decisiones y auditoría
```
- Mantener la firma exacta `def page_exceptions():` en `views/exceptions/__init__.py` para que `app.py` no requiera cambios.

### 1.3. Matriz de Aprobaciones Relacional (Fin del Código Hardcodeado)
- Añadir en `users_app`:
  - `approver_override_id VARCHAR(50)`: Cédula del coordinador o aprobador directo asignado para casos especiales (ej. Laura Aristizábal -> Angy Jaramillo).
  - `direct_to_approved BOOLEAN DEFAULT FALSE`: Indicador de si sus permisos no requieren paso por RRHH tras la firma del coordinador (ej. Valentina Tabares, Helen, Ángela).
- Reescribir la lógica en `db_approve_leave_request_coord()` y `db_get_next_approver_info()` para consultar estos campos en lugar de evaluar cadenas de texto `if str(user_id) in [...]`.

### 1.4. Modernización del Arnés de Pruebas (`tests/`)
- Adaptar `tests/conftest.py` con fixtures especializadas:
  - Fixture `test_db`: Conecta a PostgreSQL usando una base de datos de pruebas o esquema temporal aislado (`TEST_DATABASE_URL`).
  - Mock de `db_session`: Permite ejecutar pruebas unitarias puras de lógica de negocio (validación de contraseñas, cálculo de turnos, reglas de roles) sin requerir el motor de PostgreSQL activo.

---

## 2. Plan de Rollback y Mitigación de Riesgos
- **Riesgo**: Ruptura de imports en módulos que consumen `exceptions_view.py`.
  - *Mitigación*: `views/exceptions/__init__.py` expondrá todos los símbolos públicos existentes (`page_exceptions`, `show_exception_details`, `rejection_reason_dialog`).
- **Riesgo**: Inconsistencias de datos durante la migración de cédulas hardcodeadas.
  - *Mitigación*: Script de migración en `database_conn/setup.py` que inserte los valores actuales en las nuevas columnas de `users_app` antes de retirar los `if/else` del código Python.
