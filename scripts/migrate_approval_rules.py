import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database_conn.connection import db_conn

def migrate():
    print("Iniciando migración de base de datos...")
    conn = db_conn()
    cur = conn.cursor()
    
    # 1. Añadir columnas a users_app
    print("Añadiendo columnas a users_app...")
    try:
        cur.execute("ALTER TABLE users_app ADD COLUMN direct_routing VARCHAR(50) DEFAULT NULL;")
        print("Columna direct_routing añadida.")
    except Exception as e:
        print(f"Nota: Columna direct_routing quizás ya existe ({e})")
        conn.rollback()

    try:
        cur.execute("ALTER TABLE users_app ADD COLUMN skip_jefe_approval BOOLEAN DEFAULT FALSE;")
        print("Columna skip_jefe_approval añadida.")
    except Exception as e:
        print(f"Nota: Columna skip_jefe_approval quizás ya existe ({e})")
        conn.rollback()

    # 2. Configurar reglas quemadas en la DB
    # Regla: Lina Cardona -> Va directo al Jefe (PENDING_JEFE), saltando Coordinador
    print("Actualizando Lina Cardona...")
    cur.execute("UPDATE users_app SET direct_routing = 'JEFE' WHERE username = '111623881';")
    
    # Regla: ZARZAL_EMPLOYEES -> PENDING_COORD
    zarzal_employees = [
        "100389343", "100628953", "100643956", "100748910", 
        "111421083", "111644217", "38797265", "66681600", "16732215"
    ]
    print("Actualizando empleados Zarzal...")
    cur.execute("UPDATE users_app SET direct_routing = 'COORD' WHERE username = ANY(%s);", (zarzal_employees,))

    # Regla: Usuarios especiales -> PENDING_RRHH
    special_users = ["119279359", "111627893"]
    print("Actualizando usuarios especiales (rrhh direct)...")
    cur.execute("UPDATE users_app SET direct_routing = 'RRHH', skip_jefe_approval = TRUE WHERE username = ANY(%s);", (special_users,))

    # Regla: juridico -> PENDING_RRHH (No usamos IDs aquí, pero lo marcamos general para el rol)
    print("Actualizando usuarios de juridico...")
    cur.execute("UPDATE users_app SET direct_routing = 'RRHH', skip_jefe_approval = TRUE WHERE role = 'juridico';")

    conn.commit()
    conn.close()
    print("Migración completada con éxito.")

if __name__ == "__main__":
    migrate()
