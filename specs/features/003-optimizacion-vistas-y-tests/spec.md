# Especificación: 003 - Optimización de Vistas Monolíticas y Modernización de Tests

## 1. Problema y Diagnóstico

### 1.1. Vistas Monolíticas con Alto Acoplamiento a la Base de Datos
- **Ubicación**: `views/exceptions_view.py` (2.452 líneas) y `views/employee_portal_view.py` (1.754 líneas).
- **Problema de Arquitectura**:
  - `views/exceptions_view.py` contiene más de 12 consultas SQL inline con `db_session()` y `get_cached_dataframe()`, contraviniendo la regla fundamental de `AGENTS.md` (*"NO SQL en views/"*).
  - `views/employee_portal_view.py` ejecuta 9 consultas SQL directas (`pd.read_sql_query`) en los flujos de creación y seguimiento de permisos.
- **Impacto de Mantenibilidad**: Archivos masivos de más de 2.000 líneas dificultan la lectura, generan conflictos frecuentes en control de versiones y aumentan exponencialmente la probabilidad de romper funcionalidades adyacentes al editar un botón o etiqueta visual.

### 1.2. Reglas Organizacionales y Cédulas Hardcodeadas en Código Fuente
- **Ubicaciones Detectadas**:
  - `views/exceptions_view.py:759, 760`: `lr.user_id = '100389343'`
  - `views/exceptions_view.py:808`: `lr.user_id IN ('119279359', '111304019')`
  - `database_conn/queries.py:458, 568`: `str(user_id) == "119279359"`
  - `database_conn/queries.py:623`: `str(user_id) == "100389343"`
  - `database_conn/queries.py:956`: `str(user_id) in ['119279359', '111734829', '38795759']`
- **Impacto Operativo**: Cada vez que un empleado cambia de sede, asciende o asume una coordinación, un programador debe alterar el código fuente en múltiples archivos e implementar un nuevo commit en producción.

### 1.3. Suite de Pruebas Automatizadas Obsoleta
- **Ubicación**: `tests/test_app.py` (1.048 líneas).
- **Problema**: Las pruebas unitarias fueron escritas asumiendo una base de datos local SQLite (`app.db`), mientras que el sistema ahora corre sobre PostgreSQL 15.
- **Impacto de Calidad**: `pytest` no puede ejecutarse de manera confiable en el pipeline de CI/CD, perdiendo el arnés de protección contra regresiones.

---

## 2. Objetivos de la Especificación
1. Extraer la totalidad de consultas SQL de `views/exceptions_view.py` y `views/employee_portal_view.py` hacia funciones con nombre, tipadas y documentadas en `database_conn/queries.py`.
2. Modularizar `views/exceptions_view.py` en sub-componentes especializados dentro de un paquete `views/exceptions/`.
3. Modelar en la tabla `users_app` las columnas relacionales para excepciones de enrutamiento (`supervisor_id`, `skip_rrhh_approval`), eliminando todas las cédulas hardcodeadas en Python.
4. Actualizar `tests/test_app.py` para operar con fixtures compatibles con PostgreSQL o con un adaptador de pruebas parametrizado.
