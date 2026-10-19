---
description: Plantilla automática para obligar a crear specs/features/XXX-nombre/ con spec.md, plan.md y tasks.md antes de escribir código.
---

# /nueva-spec

> [!IMPORTANT]
> **REGLA DE ORO DE PRODUCTIVIDAD:**
> Está terminantemente prohibido escribir código en `views/`, `services/`, `database_conn/`, `components/` o `utils/` antes de que la especificación técnica esté formalmente redactada, versionada y aprobada.

Cuando el usuario solicite implementar una nueva funcionalidad, módulo, refactorización o cambio arquitectónico, el agente de IA debe ejecutar estrictamente los siguientes pasos:

---

## Paso 1: Asignar Identificador Consecutivo y Estructura
1. Inspeccionar la carpeta `specs/features/` para identificar el último número consecutivo utilizado (ej. `001`, `002`, `003`).
2. Definir el siguiente identificador con nombre en formato kebab-case:
   ```bash
   specs/features/XXX-nombre-descriptivo/
   ```
3. Crear el directorio correspondiente.

---

## Paso 2: Redactar `spec.md` (Definición del Problema y Objetivos)
Crear el archivo `specs/features/XXX-nombre-descriptivo/spec.md` incluyendo:
- **1. Problema y Diagnóstico:**
  - Descripción clara del dolor operativo o fallo técnico actual.
  - Archivos y números de línea exactos donde reside el problema.
  - Impacto en el usuario final, la nómina o el rendimiento del sistema.
- **2. Objetivos de la Especificación:**
  - Lista numerada de objetivos SMART (Específicos, Medibles, Alcanzables, Relevantes y con Plazo).
- **3. Restricciones y Reglas de Negocio:**
  - Casos borde, reglas de roles Dolormed (`admin`, `nomina`, `coordinador`, `jefe_area`, `empleado`) y consideraciones legales/laborales.

---

## Paso 3: Redactar `plan.md` (Diseño Técnico y Rollback)
Crear el archivo `specs/features/XXX-nombre-descriptivo/plan.md` incluyendo:
- **1. Arquitectura de Solución Propuesta:**
  - Modificaciones en la Capa de Datos (PostgreSQL, DDL en `setup.py`, funciones en `queries.py`).
  - Lógica de Servicios (ZKTeco, SMTP, n8n WhatsApp, FPDF2, Analytics).
  - Componentes de Interfaz en Streamlit (paleta Dolormed, glassmorphism, modales `@st.dialog`).
- **2. Patrones de Diseño Requeridos:**
  - Context managers (`db_session`, `zk_connection`), worker pools asíncronos (`ThreadPoolExecutor`), consultas parametrizadas (`%s`, `%%`).
- **3. Plan de Mitigación de Riesgos y Rollback:**
  - Qué puede fallar, cómo mitigar la contingencia y cómo revertir el cambio sin pérdida de datos.

---

## Paso 4: Redactar `tasks.md` (Checklist Secuencial y Criterios)
Crear el archivo `specs/features/XXX-nombre-descriptivo/tasks.md` incluyendo:
- **Lista de Tareas Secuenciales:**
  - Tareas atómicas organizadas por fases lógicas con casillas `- [ ]`:
    - *Fase 1: Modelo de Datos y Persistencia.*
    - *Fase 2: Lógica de Servicios y Backend.*
    - *Fase 3: Interfaz de Usuario y Vistas.*
    - *Fase 4: Suite de Pruebas Unitarias.*
- **Criterios de Aceptación y Autoverificación:**
  - Condiciones exactas y medibles para considerar la feature completa.
  - Definición explícita de las pruebas unitarias que deberán añadirse en `tests/` y ejecutarse con `pytest`.

---

## Paso 5: Actualizar el Roadmap Institucional
1. Abrir `specs/constitution/roadmap.md`.
2. Añadir la nueva feature a la tabla de seguimiento en estado `*En Planificación*`.

---

## Paso 6: Solicitar Validación del Usuario
1. Presentar un resumen ejecutivo de la especificación al usuario.
2. **DETENERSE AQUÍ.** No iniciar la escritura de código hasta que el usuario revise y confirme la especificación mediante la palabra clave *"Proceder"*, *"Aprobado"* o sus comentarios de ajuste.
