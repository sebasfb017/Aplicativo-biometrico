import holidays
from datetime import datetime
from database_conn.connection import db_conn

def sync_colombian_holidays(years_ahead=5):
    """
    Descarga y sincroniza los días festivos de Colombia para el año actual
    y los próximos X años utilizando la librería 'holidays'.
    """
    current_year = datetime.now().year
    years_to_fetch = list(range(current_year, current_year + years_ahead + 1))
    
    # Obtener festivos de Colombia para los años calculados
    co_holidays = holidays.CO(years=years_to_fetch)
    
    conn = db_conn()
    cur = conn.cursor()
    
    # Asegurarnos de que el Constraint exista en la tabla para hacer UPSERT
    # La tabla original puede no tener un UNIQUE constraint formal si se creó básica
    # Pero asumiendo que date_key ya existe por el \d holidays que vimos.
    
    inserted_or_updated = 0
    now_str = datetime.now().isoformat()
    
    for date_obj, name in sorted(co_holidays.items()):
        date_str = date_obj.isoformat()
        
        cur.execute(
            """
            INSERT INTO holidays (date, description, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (date) 
            DO UPDATE SET description = EXCLUDED.description
            """,
            (date_str, name, now_str)
        )
        inserted_or_updated += 1
        
    conn.commit()
    conn.close()
    
    return inserted_or_updated, years_to_fetch
