# Especificación: 002 - Resiliencia de Servicios Externos y Hardware ZKTeco

## 1. Problema y Diagnóstico

### 1.1. Fragilidad y Falsos Positivos en Conexión a Relojes ZKTeco
- **Ubicación**: `services/zk_service.py:87-92` y `services/zk_service.py:386-391`
  - Comprobación previa de socket con timeout ultra-corto: `socket.create_connection((ip, port), timeout=1.5)`.
- **Impacto Operativo**: En sedes periféricas conectadas por VPN o enlaces con fluctuación de latencia (Zarzal, Victoria, Terapia), un retardo de 1.6 segundos marca el biométrico erróneamente como *"El equipo está apagado o sin red"*, impidiendo la descarga de marcaciones de decenas de trabajadores.

### 1.2. Descarga Monolítica en Memoria y Bloqueo de Hilos
- **Ubicación**: `services/zk_service.py:134-156`
  - Invocación `records = conn.get_attendance()` sin paginación ni streaming.
- **Impacto de Rendimiento**: Con equipos que acumulan entre 20.000 y 80.000 marcaciones históricas, la descarga bloquea el hilo de ejecución de Python durante varios minutos, consumiendo picos severos de memoria RAM e impidiendo que otros usuarios interactúen con la interfaz.

### 1.3. Reglas de Negocio Contaminando el Driver de Hardware
- **Ubicación**: `services/zk_service.py:137-143`
  - Heurística forzada en bajo nivel: `if r.timestamp.hour == 14: punch_val = 0`.
- **Impacto de Mantenibilidad**: Modificar los datos crudos del hardware imposibilita auditorías fidedignas de marcaciones originales y viola el principio de responsabilidad única (SRP).

### 1.4. Bloqueo de Interfaz por Notificaciones Sincrónicas (WhatsApp y SMTP)
- **Ubicación**: `services/whatsapp_service.py:55` y `services/email_service.py:46-75`
  - Llamadas bloqueantes a red: `requests.post(N8N_WEBHOOK_URL, timeout=10)` y `smtplib.SMTP(smtp_server, smtp_port)`.
- **Impacto de Usuario**: Cada vez que un líder aprueba o rechaza una solicitud, la pantalla de Streamlit se "congela" entre 3 y 15 segundos mientras espera la respuesta de n8n o la negociación TLS de Gmail, causando que los usuarios den múltiples clics accidentales o reporten fallos de la plataforma.

---

## 2. Objetivos de la Especificación
1. Implementar comprobación de conectividad TCP con timeout configurable (`3.5s - 5.0s`) y retry adaptativo.
2. Desacoplar la heurística de las 14:00 horas del driver ZKTeco, trasladándola a la etapa de procesamiento analítico en `services/analytics.py`.
3. Convertir el envío de notificaciones (WhatsApp y Correo) en un patrón asíncrono no bloqueante (Fire-and-Forget en segundo plano con cola o worker seguro).
4. Implementar logging estructurado (`logging.getLogger("zkteco")`) para trazabilidad de errores en hardware y red.
