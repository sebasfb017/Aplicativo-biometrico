---
name: verificar-docker
description: Protocolo de validación de Docker, Docker Compose, sincronización de dependencias y ejecución de pruebas.
trigger: manual
---

# Workflow: Verificación de Entorno Docker y Pruebas Unitarias

Este flujo de trabajo asegura que las dependencias del proyecto, las recetas de construcción en Docker y la suite de pruebas automatizadas se encuentren perfectamente sincronizadas antes de desplegar en producción.

---

## Procedimiento de Validación para el Agente

### 1. Auditoría de Dependencias (`requirements.txt`)
Validar que todas las librerías importadas en el código fuente estén declaradas en `requirements.txt`:
1. Verificar librerías clave del stack:
   - Frontend: `streamlit`, `streamlit-option-menu`, `streamlit-calendar`, `streamlit-lottie`
   - Biometría & Hardware: `pyzk`
   - Base de Datos: `psycopg2-binary`, `pandas`
   - Documentos & Visualización: `fpdf2`, `PyPDF2`, `openpyxl`, `plotly`
   - Automatización & Seguridad: `bcrypt`, `APScheduler`, `holidays`, `google-generativeai`
   - Testing: `pytest`
2. Si se detecta un nuevo `import` que no figura en `requirements.txt`, agregarlo inmediatamente respetando los rangos de versiones (`>=X.Y`).

---

### 2. Validación de `Dockerfile`
Verificar las siguientes directivas obligatorias:
- **Imagen Base:** `FROM python:3.10-slim`
- **Dependencias del SO:**
  ```dockerfile
  RUN apt-get update && apt-get install -y \
      build-essential \
      libpq-dev \
      gcc \
      && rm -rf /var/lib/apt/lists/*
  ```
- **Copiado en Capas:**
  `COPY requirements.txt .` antes de `COPY . .` para aprovechar la caché de Docker.
- **Exposición y Comando:**
  `EXPOSE 8501` y arranque de Streamlit con flags de servidor:
  `CMD ["streamlit", "run", "app.py", "--server.runOnSave=true", "--server.address=0.0.0.0"]`

---

### 3. Validación de `docker-compose.yml`
Verificar la coherencia de servicios para el entorno de Dolormed:
- **Servicio `nomina_app`:**
  - `network_mode: "host"`: **Requisito mandatorio** para permitir tráfico UDP/TCP directo con los relojes ZKTeco ubicados en la subred física de la clínica (`192.168.52.X`).
  - Volumen montado: `.:/app` para desarrollo o imagen autocontenida para producción.
  - Zona horaria: `TZ=America/Bogota`.
  - Dependencia: `depends_on: [postgres_db]`.
- **Servicio `postgres_db`:**
  - Imagen: `postgres:15`.
  - Puerto expuesto: `5432:5432`.
  - Volumen persistente: `postgres_data:/var/lib/postgresql/data`.

---

### 4. Ejecución de la Suite de Pruebas Unitarias
Ejecutar la suite completa antes de autorizar cualquier construcción:
```bash
.venv/bin/pytest -v
```
**Criterio de Aceptación:**
- 100% de los tests en verde (cero fallos y cero errores).
- Las pruebas no deben depender de terminales físicos ZKTeco ni de conexiones reales a n8n/SMTP (uso mandatorio de mocks).

---

### 5. Generación del Informe y Comandos de Despliegue
Si todo está conforme, emitir el informe final y los comandos para poner en marcha el contenedor:

```bash
# Reconstruir imágenes y levantar en segundo plano
docker compose build
docker compose up -d

# Verificar logs del aplicativo
docker compose logs -f nomina_app
```
