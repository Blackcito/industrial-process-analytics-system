#credentials.py
import os

DB_CONFIGS = {
    ## Equipment database (Server)
    "equipment": {
        "host": os.getenv("DB_EQUIPMENT_HOST", "localhost"),
        "user": os.getenv("DB_EQUIPMENT_USER", "user"),
        "password": os.getenv("DB_EQUIPMENT_PASS", "password"),
        "database": os.getenv("DB_EQUIPMENT_NAME", "db"),
        "port": int(os.getenv("DB_EQUIPMENT_PORT", 3306)),
    },
    
    # Code database (Server)
    "Code": {
        "host": os.getenv("DB_CODE_HOST", "localhost"),
        "user": os.getenv("DB_CODE_USER", "user"),
        "password": os.getenv("DB_CODE_PASS", "password"),
        "database": os.getenv("DB_CODE_NAME", "db"),
        "port": int(os.getenv("DB_CODE_PORT", 3306)),
    },
    
    # Combined database
    "Combined": {
        "host": os.getenv("DB_COMBINED_HOST", "localhost"),
        "user": os.getenv("DB_COMBINED_USER", "user"),
        "password": os.getenv("DB_COMBINED_PASS", "password"),
        "database": os.getenv("DB_COMBINED_NAME", "db"),
        "port": int(os.getenv("DB_COMBINED_PORT", 3307)),
    },
}

REDIS_CONFIG = {
    "USE_REDIS_FLAG": os.getenv("USE_REDIS", "False").lower() == "true",
    "REDIS_IP": os.getenv("REDIS_HOST", "localhost"),
    "REDIS_PORT": int(os.getenv("REDIS_PORT", 6379)),
    "REDIS_CHANNEL": os.getenv("REDIS_CHANNEL", "default:channel"),
}
