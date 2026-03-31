# ZKTeco Nómina - Dolormed

Esta app (Streamlit) se conecta a biométricos ZKTeco por red (UDP 4370) usando `pyzk`, descarga marcaciones, las guarda en SQLite y genera reporte de tardanzas por mes según horarios semanales dinámicos.

## 1) Requisitos Previos
- **Python 3.10 o superior** recomendado.
- Durante la instalación de Python en Windows, es obligatorio marcar la casilla **"Add python.exe to PATH"**.
- Conectividad de red hacia los biométricos (Puerto UDP 4370 permitido entre el PC y los equipos).

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

## 6) Uso
1) Menú > **Empleados** (solo admin) -> Carga CSV con nombres
2) Menú > **Sincronizar** -> Descargar marcaciones
3) Menú > **Ver marcaciones** -> Visualiza registros con nombres
4) Menú > **Tardanzas del mes** -> Calcular tardanzas -> Descargar Excel
