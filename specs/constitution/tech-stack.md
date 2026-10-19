# Constitución del Sistema — Stack Tecnológico y Servicios

## 1. Capa de Base de Datos y Persistencia
- **Motor**: PostgreSQL 15 en contenedor Docker (`postgres_nomina`).
- **Conectividad (`database_conn/connection.py`)**:
  - `db_conn()`: Conexión directa con `autocommit = True`.
  - `db_session()`: Context manager transaccional (`try/commit/rollback/finally`).
- **Caché y Optimización**:
  - `get_cached_dataframe(query, params)`: Envoltorio sobre `@st.cache_data` con expiración temporal para reducir latencia en tablas de alta concurrencia.
  - Invalidador explícito `get_cached_dataframe.clear()` tras operaciones DML/mutaciones.
- **Tablas Principales**:
  - `users_app`: Cuentas del sistema, roles, contraseñas hash con bcrypt, áreas gestionadas, bloqueos por intentos fallidos.
  - `employees`: Padrón maestro de empleados (cédula `user_id`, nombres, correo, departamento, cargo).
  - `attendance_raw`: Marcaciones brutas descargadas desde terminales (`user_id`, `ts`, `device_name`, `status`, `punch_type`).
  - `leave_requests`: Solicitudes de novedades y permisos con estados (`PENDING_COORD`, `PENDING_JEFE`, `PENDING_RRHH`, `APPROVED`, `REJECTED`, `CANCELLED`).
  - `shifts` & `schedules`: Parametrización de turnos y asignación por colaborador/fecha.
  - `biometric_id_map`: Diccionario de traducción entre IDs secuenciales de hardware y cédulas reales de empleados.
  - `audit_logs`: Bitácora inmutable de eventos del sistema (aprobaciones, rechazos, correcciones manuales).
  - `holidays`: Calendario oficial de días festivos en Colombia.

---

## 2. Servicios de Integración (`services/`)

### 2.1. Biometría ZKTeco (`services/zk_service.py`)
- **Protocolo**: Comunicación TCP/UDP por puerto 4370 utilizando la librería `pyzk`.
- **Estrategia de Conexión**: `connect_with_retry()` implementa reintentos con backoff y control de timeout (`DEFAULT_TIMEOUT = 10s`).
- **Traducción Automática de IDs**: Función `_load_biometric_id_map()` precarga las equivalencias para traducir IDs internos de los dispositivos (e.g. terminales con numeración secuencial de 1 a 4 dígitos) a cédulas oficiales antes de la inserción masiva (`upsert_attendance`).
- **Sincronización Horaria**: `sync_device_time()` ajusta el reloj interno del dispositivo según el huso horario corporativo (`America/Bogota`).

### 2.2. Notificaciones por WhatsApp (`services/whatsapp_service.py`)
- **Arquitectura**: Desacoplada mediante webhook HTTP hacia un flujo de automatización en **n8n**.
- **Casos de Uso**:
  - Actualización de estado de permisos en tiempo real (`send_status_update_whatsapp`).
  - Envío de PIN de 6 dígitos para recuperación de credenciales y reseteo de contraseñas.
- **Sanitización de Datos**: Limpieza automática de prefijos telefónicos para asegurar formato internacional E.164 (`57` Colombia).

### 2.3. Mensajería de Correo SMTP (`services/email_service.py`)
- **Configuración Dinámica**: Carga y guardado en caliente desde `configs/smtp_config.yaml`.
- **Plantillas HTML**: Correos con diseño responsive y corporativo (Glassmorphism / paleta azul):
  - Alertas de nueva solicitud radicada dirigidas al aprobador correspondiente.
  - Notificaciones de aprobación/rechazo al empleado.
  - Recordatorios periódicos para solicitudes estancadas.
  - Códigos de verificación PIN para restablecimiento de acceso.

### 2.4. Festivos Nacionales (`services/holiday_service.py`)
- **Cálculo Oficial**: Utiliza `holidays.CO` para generar el calendario oficial bajo la Ley Emiliani (traslado de festivos a lunes).
- **Sincronización Idempotente**: Inserción `ON CONFLICT (date) DO UPDATE` para garantizar consistencia sin duplicar registros.

### 2.5. Recordatorios Asíncronos (`services/reminder_job.py`)
- **Ejecución en Background**: Hilo daemon desacoplado (`threading.Thread`) que detecta solicitudes en espera por más de `DAYS_THRESHOLD = 2` días.
- **Agrupamiento Inteligente**: Agrupa novedades pendientes por correo de líder para enviar un único resumen consolidado y prevenir saturación de bandeja.

### 2.6. Generación Documental (`services/notifications.py`)
- **Motor PDF**: Construcción con `fpdf2` del comprobante formal **F-TH-012** (Formato Institucional de Solicitud de Permisos).
- **Integridad**: Incrusta la cadena completa de aprobaciones obtenida de `audit_logs` con fechas, firmas digitales y marcas temporales.

---

## 3. Soporte PWA (Progressive Web App)
Ubicado en `static/` e inyectado en `app.py`:
- **`manifest.json`**: Define nombre de aplicación, tema corporativo (`#0D6EFD`), iconos (`icon-192x192.png`, `icon-512x512.png`) y modo `standalone`.
- **`sw.js`**: Service Worker registrado para habilitar instalación nativa en dispositivos móviles (Android/iOS) y equipos de escritorio.
- **Inyección DOM**: Componente `streamlit.components.v1.html` que inyecta etiquetas `<link rel="manifest">` y meta tags de Apple directamente en `window.parent.document.head`.
