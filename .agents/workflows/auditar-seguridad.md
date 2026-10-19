---
name: auditar-seguridad
description: Protocolo de verificación de seguridad, credenciales expuestas, SQL Injection y control de acceso.
trigger: manual
---

# Workflow: Auditoría de Seguridad y Control de Acceso

Este flujo de trabajo es de ejecución obligatoria antes de realizar un commit, abrir un Pull Request o desplegar una nueva versión en producción para **Dolormed**.

---

## Procedimiento de Inspección para el Agente

### 1. Inspección de Cambios Pendientes en Git
Ejecutar la verificación de diferencias en el árbol de trabajo:
```bash
git status
git diff --cached
git diff
```

---

### 2. Detección de Secretos y Credenciales Expuestas
Verificar que ningún archivo contenga secretos en texto claro:
- ❌ **Buscar contraseñas fijas:**
  ```bash
  grep -rnE "(password|passwd|secret|api_key|token) *= *['\"][^'\"]+['\"]" . \
    --exclude-dir={.git,.venv,venv,__pycache__,test_env} \
    --exclude=".streamlit/secrets.toml.example"
  ```
- ❌ **Verificar exclusión en `.gitignore`:**
  Asegurar que `.gitignore` contenga:
  - `configs/smtp_config.yaml`
  - `.streamlit/secrets.toml`
  - `*.db`
  - `.env`

---

### 3. Detección de Vulnerabilidades SQL (Cero SQL Injection)
Verificar que todas las consultas a la base de datos cumplan con las reglas de `AGENTS.md` y `db-safe-queries`:
- ❌ **Prohibido f-strings en SQL:**
  ```bash
  grep -rnE "f[\"'].*(SELECT|INSERT|UPDATE|DELETE)" views/ database_conn/ services/
  ```
  *Criterio:* Cero apariciones. Toda consulta debe usar `%s` con tupla de parámetros.
- ❌ **Prohibido SQL directo en `views/`:**
  ```bash
  grep -rnE "cur\.execute|pd\.read_sql" views/
  ```
  *Criterio:* Toda consulta debe delegarse a funciones centralizadas en `database_conn/queries.py`.
- ⚠️ **Comodines LIKE / ILIKE en `psycopg2`:**
  Asegurar que los caracteres `%` literales se escapen siempre como `%%` (ej. `ILIKE '%%' || %s || '%%'`).
- 🔒 **Transaccionalidad:**
  Toda mutación (`INSERT`, `UPDATE`, `DELETE`) debe estar encapsulada en `with db_session() as conn:`.

---

### 4. Auditoría de Roles y Control de Acceso (`utils/auth.py`)
Revisar que ningún endpoint o vista permita escalamiento de privilegios:
- Toda página en `views/` debe verificar la sesión:
  ```python
  from utils.auth import require_role
  require_role("admin", "nomina")  # O los roles correspondientes
  ```
- Verificar equivalencia de permisos:
  Los usuarios con `emp_subarea` en `['Nomina', 'Talento humano']` deben contar con permisos efectivos de `nomina`.
- Verificar auditoría obligatoria:
  Toda aprobación, rechazo o modificación de datos sensibles debe invocar:
  ```python
  from services.notifications import log_audit
  log_audit("ACCION_REALIZADA", "Detalle descriptivo...")
  ```

---

### 5. Generación del Informe de Auditoría
El agente debe emitir un informe con el siguiente formato:

```markdown
### 🛡️ Informe de Auditoría de Seguridad

- **Secretos y Credenciales:** [OK / FALLO] (Detalles si aplica)
- **Sanitización SQL (Cero f-strings):** [OK / FALLO]
- **Arquitectura de Datos (Cero SQL en views):** [OK / FALLO]
- **Control de Acceso y Roles:** [OK / FALLO]
- **Registro en Bitácora (Audit Logs):** [OK / FALLO]

**Veredicto Final:** [APROBADO PARA COMMIT / BLOQUEADO POR VULNERABILIDAD]
```
Si el veredicto es **BLOQUEADO**, el agente debe listar los archivos y números de línea exactos que deben corregirse de inmediato.
