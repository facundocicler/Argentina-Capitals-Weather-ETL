import requests
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=5, max=15),
    reraise=True
)
def fetch_single_location(api_key: str, loc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrae datos del clima para una única ubicación.
    """
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": loc["lat"], "lon": loc["lon"], "appid": api_key}

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    data["province"] = loc["province"]
    data["city"] = loc["city"]
    data["ingestion_datetime"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return data

def fetch_data(api_key: str, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extrae datos del clima actual para una lista de ubicaciones.
    Aplica reintentos automáticos con backoff exponencial ante fallos.
    """
    data_list = []

    for loc in locations:
        try:
            data = fetch_single_location(api_key, loc)
            data_list.append(data)
            logging.info(f"Datos extraídos exitosamente para {loc['city']}, {loc['province']}")
        
        except RetryError as retry_err:
            last_exception = retry_err.last_attempt.exception()
            logging.error(f"[Fallo permanente] No se pudo obtener datos para {loc['city']}, {loc['province']} tras varios intentos: {last_exception}")
        
        except requests.exceptions.RequestException as e:
            
            logging.error(f"Error inesperado para {loc['city']}, {loc['province']}: {e}")

    return data_list