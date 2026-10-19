# Constitución del Sistema — Deuda Técnica y Roadmap

## 1. Deuda Técnica Detectada en el Código

### 1.1. SQL Crudo Disperso en la Capa de Presentación (`views/`)
- **Problema**: Múltiples vistas (`exceptions_view.py`, `attendance_view.py`, `employee_portal_view.py`, `employee_360_view.py`) contienen bloques SQL `pd.read_sql_query` y `cur.execute` directos en lugar de consumir métodos centralizados en `database_conn/queries.py`.
- **Riesgo**: Duplicidad de lógica, dificultad en auditoría de seguridad y alto costo de mantenimiento ante cambios de esquema.

### 1.2. Vistas Monolíticas de Alta Complejidad Ciclomática
- **Problema**: `views/exceptions_view.py` supera las 2.400 líneas y `views/employee_portal_view.py` supera las 1.700 líneas, mezclando lógica de negocio, validaciones, consultas a base de datos y renderizado HTML/CSS.
- **Riesgo**: Alto riesgo de regresión en cada refactorización o cambio visual.

### 1.3. Reglas de Negocio y Cédulas "Hardcoded"
- **Problema**: Existen excepciones manuales codificadas con IDs específicos (ej. `user_id == '119279359'`, `'100389343'`, `'111734829'`) para coordinadores y enrutamientos directos en `queries.py` y `exceptions_view.py`.
- **Riesgo**: Fragilidad operativa; cualquier cambio en el personal de la clínica requiere alterar y desplegar código fuente.

### 1.4. Gestión de Secretos y Configuración en Texto Plano
- **Problema**: Archivos como `configs/smtp_config.yaml` almacenan credenciales de correo en texto claro. Asimismo, `services/whatsapp_service.py` tiene la URL del webhook de n8n con IP fija en el código fuente.
- **Riesgo**: Vulnerabilidad de seguridad y acoplamiento estricto a la topología de red local.

### 1.5. Residuos de SQLite y Desfase en Pruebas Unitarias
- **Problema**: Persisten archivos SQLite obsoletos (`database_conn/nomina.db`, carpeta `legacy/`). El archivo `database_conn/setup.py` tiene `init_db()` como función vacía (`pass`). La suite `tests/test_app.py` mantiene fixtures adaptadas para SQLite (`app.db`), generando discrepancias con el motor PostgreSQL de producción.
- **Riesgo**: Pruebas automáticas poco representativas del entorno productivo real.

### 1.6. Contaminación del Directorio `static/`
- **Problema**: Los comprobantes y archivos PDF subidos por empleados se guardan o duplican dentro de `static/`, mezclándose con los recursos PWA (`manifest.json`, `sw.js`, iconos).
- **Riesgo**: Ineficiencia de almacenamiento y posible exposición pública de documentos confidenciales.

---

## 2. Orden Recomendado de Mejoras (Roadmap de Refactorización)

```
Fase 1: Seguridad & Desacoplamiento (Prioridad Crítica)
   │
   ▼
Fase 2: Abstracción de Datos & Matriz de Aprobaciones (Prioridad Alta)
   │
   ▼
Fase 3: Modularización de Vistas Monolíticas (Prioridad Media)
   │
   ▼
Fase 4: Modernización de Pruebas & Almacenamiento (Prioridad Mantenimiento)
```

### Fase 1: Seguridad, Secretos y Limpieza Inmediata
1. **Migración a `st.secrets` y Variables de Entorno**:
   - Mover credenciales SMTP (`configs/smtp_config.yaml`), DSN de PostgreSQL y webhooks de n8n a `.streamlit/secrets.toml` o variables de entorno Docker.
2. **Saneamiento de Repositorio**:
   - Asegurar en `.gitignore` la exclusión absoluta de archivos `*.db` y credenciales.
   - Reubicar los archivos PDF subidos a un directorio privado segregado (`uploads/`), sirviéndolos vía streaming o autenticación estricta.

### Fase 2: Matriz Dinámica de Aprobaciones y Capa de Datos Pura
1. **Tabla de Jerarquías Organizacionales (`approval_workflows`)**:
   - Sustituir las condiciones hardcoded de usuarios por una tabla relacional en PostgreSQL que defina: Solicitante (área/subárea/usuario) ➔ Aprobador Nivel 1 ➔ Aprobador Nivel 2.
2. **Extracción Total de SQL de `views/` a `database_conn/queries.py`**:
   - Mover cada consulta de lectura y escritura a funciones puras documentadas y tipadas en `queries.py`.
   - Implementar un esquema formal en `database_conn/setup.py` que permita crear la estructura PostgreSQL desde cero de forma reproducible.

### Fase 3: Modularización de Componentes de Interfaz
1. **Descomposición de `views/exceptions_view.py`**:
   - Dividir en sub-módulos enfocados: `views/exceptions/coord_panel.py`, `views/exceptions/jefe_panel.py`, `views/exceptions/rrhh_panel.py`, `views/exceptions/calendar_tab.py`.
2. **Descomposición de `views/employee_portal_view.py`**:
   - Separar el formulario de radicación, el visor de radicados F-TH-012 y el historial de solicitudes.

### Fase 4: Suite de Pruebas con PostgreSQL y CI/CD
1. **Actualización de `tests/` con Testcontainers o DB Mock**:
   - Actualizar `tests/test_app.py` para usar PostgreSQL (vía contenedor de pruebas o fixtures aisladas) reflejando fielmente el entorno real.
2. **Pipeline de Integración Continua**:
   - Ejecución automática de linter (`ruff`) y tests (`pytest`) antes de cada integración a la rama principal.

---

## 3. Estado de Features y Especificaciones

| Feature ID | Nombre | Estado | Resumen de Implementación |
| :--- | :--- | :--- | :--- |
| **001** | **Seguridad, Manejo de Secretos y Optimización de la Capa de Datos** | **Implementado** | Secrets centralizados en `st.secrets`/ENV, pooling `ThreadedConnectionPool` (2-20), consultas SQL parametrizadas, DDL idempotente en `setup.py` y eliminación de residuos SQLite. |
| **002** | **Resiliencia de Servicios Externos y Hardware ZKTeco** | **Implementado** | Pre-check socket configurable (3.5s default), context manager `zk_connection()` con disconnect garantizado, separación de datos crudos (regla 14:00 movida a `analytics.py`), despacho asíncrono con `ThreadPoolExecutor(max_workers=5)` en WhatsApp/Email, timeout 5s en n8n y registro `NOTIFICATION_FAILURE` en `audit_logs`. |
| **003** | **Optimización de Vistas Monolíticas y Suite de Tests** | *Pendiente* | Modularización de `exceptions_view.py` y `employee_portal_view.py`, integración continua y cobertura completa. |

