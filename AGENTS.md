# AGENTS.md — System Prompt & Guía de Desarrollo

## 1. Resumen y Arquitectura
Aplicación web corporativa para control de asistencia biométrica, cálculo de novedades de nómina y portal de autogestión de empleados para **Dolormed**.
Arquitectura modular en capas:
- `app.py`: Punto de entrada, configuración de página, inyección PWA/CSS y router por roles.
- `views/`: Módulos de interfaz de usuario en Streamlit (dashboard, asistencias, portal de empleado, aprobaciones, turnos).
- `components/`: Componentes visuales reutilizables e interactivos (`timeline_component.py`).
- `services/`: Lógica de integración externa (ZKTeco PyZK, correo SMTP, WhatsApp vía n8n, analítica, festivos, PDF).
- `database_conn/`: Conectividad PostgreSQL (`connection.py`), consultas centralizadas (`queries.py`) e inicialización (`setup.py`).
- `utils/`: Autenticación (`auth.py`), validación de contraseñas, temas y constantes.

## 2. Stack Tecnológico Exacto
- **Runtime**: Python 3.10+ (Debian slim en Docker).
- **Frontend / Core**: Streamlit >= 1.30, streamlit-option-menu >= 0.3.13, streamlit-calendar >= 1.4.0, streamlit-lottie >= 0.0.3.
- **Base de Datos**: PostgreSQL 15 (`psycopg2-binary >= 2.9.9`), `pandas >= 2.1`.
- **Biometría & Hardware**: `pyzk >= 0.9` (protocolo UDP/TCP ZKEM).
- **Procesamiento de Documentos**: `fpdf2 >= 2.7.0`, `PyPDF2 >= 3.0.1`, `openpyxl >= 3.1`.
- **Seguridad**: `bcrypt >= 4.0`.
- **Automatización & Tiempo**: `APScheduler >= 3.10.0`, `holidays >= 0.40`.
- **Visualización & IA**: `plotly >= 5.18.0`, `google-generativeai == 0.8.2`, `pyyaml >= 6.0`.
- **Testing**: `pytest >= 7.0`.

## 3. Comandos de Ejecución y Pruebas
```bash
# Ejecutar aplicación con Docker Compose
docker compose up -d

# Ejecutar aplicación localmente en entorno virtual
streamlit run app.py --server.address=0.0.0.0 --server.port=8501

# Inicializar esquema de base de datos
python -c "from database_conn.setup import init_db; init_db()"

# Ejecutar suite de pruebas
pytest -v
```

## 4. Convenciones Estrictas de Código
- **Estado de Sesión (`st.session_state`)**:
  - `st.session_state["user"]`: Diccionario del usuario autenticado (`username`, `full_name`, `role`, `emp_area`, etc.).
  - `st.session_state["menu_selection"]`: Control de navegación activa en el sidebar.
  - Verificar siempre existencia de claves antes de mutación para evitar excepciones `KeyError`.
- **Autenticación y Roles**:
  - Usar `utils/auth.py` (`require_role(*allowed_roles)` y `verify_login`).
  - Roles soportados: `admin`, `nomina`, `coordinador`, `jefe_area`, `empleado`.
  - Equivalencia de permisos: Empleados con `emp_subarea` en `['Nomina', 'Talento humano']` poseen permisos efectivos de `nomina`.
- **Capa de Datos**:
  - Usar siempre `db_session()` (context manager transaccional) o `db_conn()` de `database_conn/connection.py`.
  - Centralizar toda operación SQL en `database_conn/queries.py`.
  - Cachear lecturas pesadas con `get_cached_dataframe(query, params)` y vaciar caché con `get_cached_dataframe.clear()` tras escrituras.
- **Biometría y Hardware**:
  - Toda comunicación con terminales ZKTeco debe utilizar `connect_with_retry` y respetar los timeouts definidos en `APP_CONFIG`.
- **Trazabilidad y Auditoría**:
  - Toda mutación de estado de solicitudes, cambios de turnos o reasignación de marcaciones debe registrarse con `log_audit()` en `audit_logs`.

## 5. Prohibiciones Estrictas (Zero Tolerance)
- ❌ **NO SQL en `views/`**: Ninguna vista debe ejecutar SQL directo; delegar siempre a funciones en `database_conn/queries.py`.
- ❌ **NO f-strings en SQL**: Prohibido interpolar strings en consultas (`f"SELECT... {var}"`). Usar SIEMPRE parámetros (`%s`, tupla).
- ❌ **NO escapar comodines indebidamente**: En SQL parametrizado con `psycopg2`, usar `%%` para wildcards literales (`LIKE '%%texto%%'`).
- ❌ **NO modificar `legacy/`**: Carpeta de archivo histórico; no agregar ni alterar archivos allí.
- ❌ **NO exponer credenciales ni subir bases de datos**: Prohibido commitear contraseñas, tokens, archivos `.db` (`nomina.db`, `app.db`) o claves privadas.
