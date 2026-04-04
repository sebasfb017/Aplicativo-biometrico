<div align="center">
  <h1>🕒 ZKTeco Nómina - Dolormed</h1>
  <p><strong>Un sistema integral de control de asistencia, sincronización biométrica y gestión de permisos construido con Streamlit.</strong></p>
  
  [![Python version](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
  [![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B.svg)](https://streamlit.io/)
  [![ZKTeco](https://img.shields.io/badge/Biometrics-ZKTeco-green.svg)]()
  [![SQLite](https://img.shields.io/badge/Database-SQLite-003B57.svg)]()
</div>

## 📖 Descripción General

**ZKTeco Nómina** es una aplicación web (impulsada por Streamlit) diseñada para interactuar con dispositivos biométricos **ZKTeco** a través de red local (UDP 4370) mediante `pyzk`. La plataforma permite automatizar procesos engorrosos de RRHH como: descargar registros de asistencia de las máquinas, centralizarlos en una base de datos SQLite y generar reportes analíticos de tardanzas por mes basados en horarios semanales rotativos.

Además, cuenta con un **Portal del Empleado**, una herramienta digital que reemplaza el uso de formularios de papel (como el F-TH-012) permitiendo enviar solicitudes de novedades (permisos, vacaciones, excepciones) con su respectivo flujo de aprobaciones para RRHH y gestión de nómina.

---

## ✨ Características Principales

- 🔄 **Sincronización Biométrica Directa**: Conexión a múltiples equipos por IP (UDP) para descargar el registro de checadas directamente del hardware.
- 👥 **Gestión Central de Empleados**: Importación mediante formato CSV para vincular el `user_id` de la huella/rostro biométrico con nombres reales, correos y departamentos.
- 📅 **Horarios Diferenciados (Franjas)**: Soporte de horarios complejos con hasta 2 franjas laborales por día, cálculo de tolerancia en minutos (_grace minutes_) e interfaz optimizada de visualización semanal o global.
- 📊 **Reportes y Exportación**: Generador automático de cálculo de tardanzas e inconsistencias, con validación de registros y exportación completa a **Microsoft Excel**.
- 📝 **Portal de Solicitudes Digitales (Novedades)**: Interfaz diseñada para los empleados que requieran solicitar justificaciones directamente proporcionando su identificación.
- ✅ **Panel Interactivo de RRHH (Aprobaciones)**: Tablero de mando "Monitoreo Global" para aprobar, rechazar o visualizar solicitudes de permisos digitales.

---

## 🛠️ Tecnologías Utilizadas

- **Frontend / Backend:** Python 3.10+, Streamlit
- **Base de Datos:** SQLite (ligera, local, 0 configuraciones externas)
- **Integración Equipo Físico:** `pyzk`
- **Manejo de Datos Analíticos:** Pandas

---

## 🚀 Requisitos Previos

1. **Python 3.10 o superior** instalado. [Descargar Python](https://www.python.org/downloads/)
   > ⚠️ **Importante**: Durante la instalación de Python en Windows, es obligatorio marcar la casilla **"Add python.exe to PATH"**.
2. **Conectividad de Red**: Asegurar que el puerto UDP `4370` esté habilitado en el firewall de Windows y que haya comunicación IP hacia los dispositivos biométricos involucrados.
3. **Git** (_Opcional_, necesario solo para clonar el repositorio).

---

## 💻 Guía de Instalación Local (Desarrolladores)

Sigue estos pasos en estricto orden para preparar tu entorno de desarrollo en **Windows**:

### Paso 1: Clonar y preparar el directorio

Abre tu terminal (PowerShell por defecto) y ejecuta:

```powershell
git clone <URL-DEL-REPOSITORIO>
cd Aplicativo-biometrico
```

## 2) Guía de Configuración Local (Desarrolladores)

Sigue estos pasos en orden para levantar el entorno de desarrollo en Windows desde cero:

**Paso A: Clonar y preparar el directorio**
Abre una terminal de PowerShell y ejecuta:
```powershell
git clone <URL-DEL-REPOSITORIO>
cd Aplicativo-biometrico
```

**Paso B: Permisos de ejecución (Solo Windows)**
Por defecto, PowerShell bloquea la activación de entornos virtuales. Para evitar el error de seguridad (`PSSecurityException`), otorga permisos locales ejecutando este comando (acepta con "S" o "Y" cuando te lo solicite):
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Paso C: Crear y activar el entorno virtual**
Aísla las dependencias del proyecto ejecutando:
```powershell
python -m venv venv
.\venv\Scripts\activate
```
*(Validación: Deberías ver el prefijo `(venv)` al inicio de tu línea de comandos).*

**Paso D: Instalar dependencias**
Con el entorno activado, instala todas las librerías necesarias:
```powershell
pip install -r requirements.txt
```

**Paso E: Ejecución del servidor**
Puedes levantar la aplicación ejecutando el script automático o llamando a Streamlit directamente:
```powershell
.\run_windows.bat
# O alternativamente:
# streamlit run app.py
```

El navegador abrirá automáticamente el portal en `http://localhost:8501`. 
* **Credenciales maestras por defecto:** `admin` / `Cambiar123!`

---
## 3) Configurar biométricos
Pon las IP reales (y `password` si el equipo lo usa).

## 4) Cargar empleados
En la app (Menú > Empleados, solo admin):
- Sube un CSV con columnas: `user_id,full_name,email,department`.
- Opcionalmente puedes añadir `profile_id` (el nombre del perfil o su ID)
  para que el sistema aplique reglas de horarios avanzadas.
- Esto asocia los IDs de los biométricos con nombres reales y, si lo deseas,
  con una categoría de personal (Enfermería, Administrativo, etc.).
- Los nombres aparecerán al ver las marcaciones

## 5) Cargar horarios

### 🎯 Carga tradicional (horarios completos)
En la app, entra con:
- usuario: admin
- contraseña: Cambiar123!

Luego:
- Menú > Horarios (solo admin)
- Sube el archivo CSV con horarios semanales

El formato admite hasta dos franjas por día. Usa estas columnas:

```
week_start,dow,start_time,end_time,start_time_2,end_time_2,grace_minutes
```

Las columnas `end_time`, `start_time_2` y `end_time_2` son opcionales;
`start_time` siempre se utiliza para calcular tardanzas. El resto se guarda
para referencia y se puede ver en la tabla.

La pantalla de horarios carga por defecto sólo las últimas 52 semanas para
mantener la interfaz rápida; actívalo si necesitas ver todo y utiliza el
botón de descarga CSV para editar conjuntos grandes.

También puedes generar los horarios automáticamente con un patrón más
arriba (patrón de inicio separados por comas y `|` para segunda franja),
lo que es conveniente si las semanas rotan entre dos o más horarios.

---

## 🔐 Configuración Básica de Roles

### 👑 Rol Administrador (Gestores de Nómina / RRHH)

- **Credenciales por defecto:** Usuario `admin` / Contraseña `Cambiar123!`
- ⚙️ **Configuración Biométrica**: Pon las IP Reales y la Contraseña de Administrador (si es que la máquina lo usa) haciendo uso de `devices.yaml` o de la UI.
- 📥 **Sincronización**: Click en Menú > Sincronizar para traerte todas las nuevas marcaciones.
- 🧑‍💼 **Carga de Empleados**: Menú > Empleados. Requiere CSV con las columnas base `user_id`, `full_name`, `email`, `department`. Opcionalmente se puede asociar una etiqueta `profile_id`.
- ⏰ **Carga de Horarios**: Sube tu archivo base con las estructuras de horario `week_start, dow, start_time, end_time, start_time_2, end_time_2, grace_minutes`.
- 📁 **Descarga en Excel**: Menú > Tardanzas del Mes -> Calcular, luego usa el botón de Descarga de Excel.
- ✨ **Control de Novedades**: Revisa las pestañas para gestionar aprobaciones digitales y validar justificativas que el equipo te comparta.

### 💼 Rol Empleado (Usuario Final)

- Sin necesidad de usuario administrador, cualquier empleado debe acceder al sitio e interactuar con el **Generador de Solicitudes**.
- El usuario podrá aplicar un permiso temporal (en vez de usar hojas físicas) validando su ingreso por medio del sistema, el cual lo dejará en estado "Pendiente" para ser posteriormente aprobado en la nómina.

---

<div align="center">
  <p>Optimizado para brindar reportes fiables y centralizar la base de asistencia de colaboradores.<br>
</div>
