import os
import json
from pathlib import Path
from dotenv import load_dotenv
from typing import cast, Dict, Any

load_dotenv(dotenv_path="/opt/airflow/.env", override=True)

def load_locations(path: str) -> list[dict]:
    """Obtiene los datos del JSON."""
    with open(Path(path), "r", encoding="utf=8") as file:
        return json.load(file)

def get_env_variable(var_name: str, required: bool = True) -> str:
    """Obtiene una variable de entorno y valida si es requerida."""
    value = os.getenv(var_name)
    if required and (value is None or value.strip() == ""):
        raise EnvironmentError(f"La variable de entorno '{var_name}' no está definida.")
    return cast(str, value)

def get_api_key() -> str:
    """Devuelve la API KEY para la conexión con OpenWeather."""

    return get_env_variable("API_KEY")

def get_mongo_config() -> Dict[str, Any]:
    """Devuelve la configuración para conexión a MongoDB."""

    return {
        "host": get_env_variable("MONGO_HOST"),
        "port": int(get_env_variable("MONGO_PORT")),
        "database": get_env_variable("MONGO_DB"),
        "collection": get_env_variable("MONGO_COLLECTION")
    }

def get_mysql_config() -> Dict[str, Any]:
    """Devuelve la configuración para conexión a MySQL."""
    return {
        "host": get_env_variable("MYSQL_HOST"),
        "port": int(get_env_variable("MYSQL_PORT")),
        "database": get_env_variable("MYSQL_DB"),
        "user": get_env_variable("MYSQL_USER"),
        "password": get_env_variable("MYSQL_PASSWORD")
    }
