# ZKTeco Nómina (descarga marcaciones + tardanzas)

Esta app (Streamlit) se conecta a biométricos ZKTeco por red (UDP 4370) usando `pyzk`,
descarga marcaciones, las guarda en SQLite y genera reporte de tardanzas por mes
según horarios semanales (porque cambian cada semana).

## 1) Requisitos
- Python 3.10+ recomendado
- Conectividad hacia los biométricos por red
- Puerto UDP 4370 permitido entre el PC y los equipos

## 2) Instalación (Windows / PowerShell)
Dentro de esta carpeta:

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## 3) Configurar biométricos
Edita `devices.yaml` y pon las IP reales (y `password` si el equipo lo usa).

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

### 🚀 Carga optimizada usando códigos y perfiles
Esta es la novedad inspirada en el sistema PHP del colega. En vez de subir
tabla completa, basta con un CSV ligero que contenga un código por día.

1. Asegúrate de que cada empleado tenga un **perfil** asignado (Enfermería,
   Admisiones, Tecnólogos RX, Administrativo). Ver más abajo.
2. Menú > Horarios > *Cargar turnos por código*.
3. CSV con columnas mínimas:

```
user_id,week_start,dow,shift_code
```

- `shift_code` puede ser M, T, N, RX1, RX2, OFICINA, C, L, etc.
- El sistema traduce el código al turno real según el perfil del empleado,
  aplica recargos nocturnos y respeta festivos (si el perfil descansa).

Este método reduce el tamaño del archivo en un ~90% y permite manejar
programaciones mensuales de la forma en que tu colega lo hacía en PHP.

La carga de empleados (siguiente sección) ahora admite un campo opcional
`profile_id` que puede ser el nombre del perfil o su ID.


## Horarios predeterminados
Si colocas un archivo CSV llamado `default_schedules.csv` dentro del
subdirectorio `data/` (se crea automáticamente tras usar la app), el
sistema lo cargará a la base de datos en el arranque.
Puedes gestionar este archivo desde la interfaz de "Horarios":

1. Subir un CSV con horarios y hacer clic en "Guardar este CSV como
   predeterminados".
2. Verás el nombre del archivo actual y podrás aplicarlo con el botón
   "Aplicar predeterminados".

Esto es útil para empaquetar un conjunto de horarios que deben cargarse
siempre al iniciar la aplicación.
## 6) Uso
1) Menú > **Empleados** (solo admin) -> Carga CSV con nombres
2) Menú > **Sincronizar** -> Descargar marcaciones
3) Menú > **Ver marcaciones** -> Visualiza registros con nombres
4) Menú > **Tardanzas del mes** -> Calcular tardanzas -> Descargar Excel

## 7) Pruebas unitarias 🧪
Se incluye un conjunto de pruebas básicas con `pytest` para
verificar los cálculos de tardanzas, carga de horarios y
manejo de usuarios. Antes de ejecutar las pruebas asegúrate de
instalar la dependencia adicional:

```powershell
pip install pytest
pytest -q
```

## 6) Roles
- admin: usuarios + horarios + sincronizar + reportes
- nomina: sincronizar + reportes

## Datos
Se guardan en `data/app.db` (SQLite) dentro de esta carpeta.
