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

### Paso 2: Permisos de ejecución de Scripts (Obligatorio en PowerShell)

Para evitar el error de seguridad (`PSSecurityException`) al activar entornos virtuales aislados de Python en Windows:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

_(Presiona "S" o "Y" cuando se te solicite confirmación)_

### Paso 3: Crear y activar el entorno virtual

Es vital manejar este proyecto de forma aislada a las librerías de tu PC general:

```powershell
python -m venv venv
.\venv\Scripts\activate
```

_(Validación: Deberías ver con éxito el prefijo `(venv)` al inicio de cada línea en la terminal)._

### Paso 4: Instalar dependencias

```powershell
pip install -r requirements.txt
```

### Paso 5: Levantamiento del Servidor

Puedes ejecutar el sistema mediante el script automatizado provisto o haciendo el llamado nativo de la librería Streamlit:

```powershell
.\run_windows.bat

# Alternativa directa de framework:
streamlit run app.py
```

> ✅ **Listo**. El portal abrirá de forma automática en tu navegador por defecto la URL: `http://localhost:8501`.

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
