---
name: db-safe-queries
description: Estándar para conexiones seguras, transacciones, consultas parametrizadas y caché en PostgreSQL para Dolormed.
---

# Estándar de Consultas Seguras y Persistencia en PostgreSQL

## 1. Conexiones y Transacciones (`database_conn/connection.py`)
- **Mutaciones (INSERT, UPDATE, DELETE)**: Usar SIEMPRE `db_session()`. Context manager con commit automático y rollback ante fallos:
  ```python
  from database_conn.connection import db_session

  with db_session() as conn:
      cur = conn.cursor()
      cur.execute("UPDATE leave_requests SET status = %s WHERE id = %s", (status, req_id))
  ```
- **Lecturas Rápidas**: Si usas `db_conn()`, cerrar SIEMPRE la conexión en bloque `finally` (`conn.close()`).

## 2. Parametrización Estricta (Cero SQL Injection)
- **Placeholders**: Usar `%s` con tupla de parámetros. PROHIBIDO concatenar texto con f-strings (`f"WHERE id = {id}"`).
- **Comodines LIKE / ILIKE**: En psycopg2, `%` es carácter especial. Escapar siempre los wildcards como `%%`:
  ```python
  # Correcto: escapando % para evitar errores de formateo
  cur.execute("SELECT * FROM employees WHERE department ILIKE '%%' || %s || '%%'", (dept_name,))
  cur.execute("SELECT * FROM audit_logs WHERE details LIKE %s", (f"%Permiso #{req_id}%",))
  ```
- **Listas Dinámicas (IN)**:
  ```python
  placeholders = ",".join(["%s"] * len(ids))
  cur.execute(f"SELECT * FROM users_app WHERE username IN ({placeholders})", tuple(ids))
  ```

## 3. Caché de Lectura e Invalidación (`queries.py`)
- **Lecturas Pesadas**: Usar `get_cached_dataframe(query, params=tuple)`.
- **Invalidación Inmediata**: Tras CUALQUIER mutación (`INSERT`, `UPDATE`, `DELETE`), limpiar la caché global:
  ```python
  from database_conn.queries import get_cached_dataframe

  # Inmediatamente tras mutar datos:
  get_cached_dataframe.clear()
  ```

## 4. Trazabilidad y Auditoría Obligatoria
- Toda decisión sobre permisos, novedades o reasignación de marcaciones debe registrarse con `log_audit()`:
  ```python
  from database_conn.queries import log_audit
  log_audit("APPROVE_LEAVE", f"Permiso #{req_id} aprobado por {user['full_name']}")
  ```

## 5. Prohibiciones Estrictas de Base de Datos
- ❌ Prohibido ejecutar SQL directo dentro de `views/`. Centralizar toda consulta en `database_conn/queries.py`.
- ❌ Prohibido dejar conexiones abiertas sin bloque `with` o `try/finally`.
- ❌ Prohibido utilizar o depender de archivos SQLite (`*.db`). El entorno productivo opera exclusivamente en PostgreSQL 15.
