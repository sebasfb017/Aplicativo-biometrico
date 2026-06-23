import os
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "settings.yaml")

def load_config():
    if not os.path.exists(CONFIG_PATH):
        # Crear configuración por defecto si no existe
        default_config = {
            "database": {
                "path": "data/app.db"
            },
            "zkteco": {
                "default_port": 4370,
                "timeout": 10
            }
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            yaml.dump(default_config, f, allow_unicode=True, sort_keys=False)
        return default_config
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

APP_CONFIG = load_config()
