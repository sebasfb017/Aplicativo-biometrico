# Especificación: 001 - Seguridad, Manejo de Secretos y Optimización de la Capa de Datos

## 1. Problema y Diagnóstico

### 1.1. Secretos y Credenciales en Texto Claro
- **Ubicación**: `database_conn/connection.py:6`
  - `POSTGRES_DSN = 'postgresql://nomina_user:nomina_password@localhost:5432/nomina_db'` se encuentra fijado en código fuente sin soporte de variables de entorno ni `st.secrets`.
- **Ubicación**: `services/email_service.py:16-33, 46-56` y `configs/smtp_config.yaml`
  - La contraseña SMTP (`smtp_password`) se lee y escribe en disco en texto plano.
- **Ubicación**: `services/whatsapp_service.py:7`
  - URL fija de red interna: `"http://192.168.52.29:5678/webhook/d4387103-a20e-4576-8ef1-6eb644d9aa7b"`.
- **Impacto de Seguridad**: Exposición inmediata de contraseñas de infraestructura, base de datos y buzones SMTP corporativos en el historial de Git (CWE-798).

### 1.2. Falta de Connection Pooling y Saturación de Conexiones
- **Ubicación**: `database_conn/connection.py:11-15` y `database_conn/connection.py:18-31`
  - Cada invocación de `db_conn()` y `db_session()` crea un socket TCP nuevo hacia PostgreSQL (`psycopg2.connect`) y lo destruye al terminar.
- **Impacto de Rendimiento**: Con Streamlit manejando múltiples sesiones simultáneas de empleados y líderes, se generan picos de apertura de conexiones, provocando agotamiento de `max_connections` y latencias adicionales por negociación SSL/TCP.

### 1.3. Riesgos de Inyección SQL e Interpolación de Cadenas
- **Ubicación**: `views/exceptions_view.py:383, 412, 432, 444`
  - Concatenación directa de filtros de estado: `WHERE lr.status IN {status_filter}` mediante f-strings.
- **Ubicación**: `views/exceptions_view.py:401, 745`
  - Cláusulas de ruteo inyectadas como cadenas: `cond_zarzal = f"OR ((SELECT direct_routing... = 'COORD')"`
- **Impacto de Seguridad**: Fragilidad ante inyección de SQL y anulación del plan de caché de consultas precompiladas de PostgreSQL (CWE-89).

### 1.4. Residuos de Base de Datos y Falta de Migraciones
- **Ubicación**: `database_conn/nomina.db` (archivo residual en el repositorio) y `database_conn/setup.py:9-10` (`init_db()` vacío con `pass`).
- **Impacto**: Falta de un DDL canónico y reproducible para aprovisionar el esquema PostgreSQL en nuevos entornos de desarrollo o contingencia.

---

## 2. Objetivos de la Especificación
1. Centralizar la configuración sensible en variables de entorno con fallback a `.streamlit/secrets.toml`.
2. Implementar un pool de conexiones seguro (`ThreadedConnectionPool`) con descarte automático de conexiones muertas.
3. Eliminar toda interpolación de f-strings en sentencias SQL, migrando a consultas 100% parametrizadas con tuplas.
4. Implementar un mecanismo de inicialización DDL declarativo en `database_conn/setup.py` y excluir formalmente archivos `.db` locales.
