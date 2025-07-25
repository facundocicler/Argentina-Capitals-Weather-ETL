import pymysql
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def wait_for_mysql(db_config: dict, timeout: int = 180) -> None:
    """
    Espera a que el servicio MySQL esté disponible, hasta un tiempo máximo.
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            conn.close()
            logging.info("MySQL está disponible.")
            return
        except Exception:
            logging.info("Esperando a que MySQL esté disponible...")
            time.sleep(5)
    raise TimeoutError("MySQL no estuvo disponible dentro del tiempo límite.")

def parse_ingestion_datetime(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")