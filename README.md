# ZKTeco Nómina

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
