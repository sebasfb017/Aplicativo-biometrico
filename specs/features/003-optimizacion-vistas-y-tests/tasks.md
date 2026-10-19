# Tareas: 003 - Optimización de Vistas Monolíticas y Modernización de Tests

## Lista de Tareas Secuenciales

### Fase 1: Extracción de Consultas SQL a `queries.py`
- [ ] 1.1 Identificar y extraer las 12 consultas SQL de `views/exceptions_view.py` hacia funciones dedicadas en `database_conn/queries.py`.
- [ ] 1.2 Identificar y extraer las 9 consultas SQL de `views/employee_portal_view.py` hacia `database_conn/queries.py`.
- [ ] 1.3 Verificar que ninguna de las vistas importe `psycopg2` ni ejecute `conn.cursor()` directamente.

### Fase 2: Eliminación de Reglas y Cédulas Hardcodeadas
- [ ] 2.1 Crear script de migración para agregar las columnas `approver_override_id` y `direct_to_approved` en la tabla `users_app`.
- [ ] 2.2 Migrar las excepciones existentes de los IDs `100389343`, `119279359`, `111734829`, `38795759` a los nuevos campos de la base de datos.
- [ ] 2.3 Refactorizar `db_get_next_approver_info()` y `db_approve_leave_request_coord()` para eliminar los condicionales con números de cédula fijos.

### Fase 3: Modularización de `views/exceptions_view.py`
- [ ] 3.1 Crear la estructura de paquete `views/exceptions/`.
- [ ] 3.2 Migrar el panel de pendientes a `pending_approval_tab.py`.
- [ ] 3.3 Migrar el panel de RRHH a `rrhh_approval_tab.py`.
- [ ] 3.4 Migrar el calendario a `calendar_tab.py`.
- [ ] 3.5 Migrar el historial a `decision_history_tab.py`.
- [ ] 3.6 Configurar `views/exceptions/__init__.py` para mantener retrocompatibilidad total con `app.py`.

### Fase 4: Modernización de la Suite de Pruebas
- [ ] 4.1 Crear `tests/conftest.py` con fixtures limpias para emular `db_session()` mediante mocks o conexión PostgreSQL de pruebas.
- [ ] 4.2 Actualizar `tests/test_app.py` eliminando el fixture obsoleto de SQLite (`app.db`).
- [ ] 4.3 Añadir pruebas unitarias de regresión para validar el enrutamiento dinámico de permisos sin cédulas hardcodeadas.
- [ ] 4.4 Verificar que `pytest -v` se ejecute al 100% en verde.

---

## Criterios de Aceptación y Autoverificación con Pytest

1. **Cero SQL en Vistas**:
   - `grep -r "pd.read_sql" views/exceptions/ views/employee_portal_view.py` debe retornar 0 resultados.
2. **Cero Cédulas en Código Fuente**:
   - `grep -r "100389343\|119279359\|111734829" database_conn/ views/` debe retornar 0 resultados (salvo en logs o fixtures de test explícitos).
3. **Mantenibilidad y Tamaño de Archivos**:
   - Ningún archivo individual en `views/` debe superar las 700 líneas de código.
4. **Ejecución Exitosa de Tests**:
   - `pytest -v` debe correr sin advertencias de deprecación de SQLite y validar el 100% de los casos de prueba de usuarios y permisos.
