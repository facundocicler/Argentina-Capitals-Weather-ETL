from typing import Any, Dict, List
from pyspark.sql import DataFrame
from pymongo.collection import Collection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_to_mongo(collection: Collection, data: List[dict]) -> None:
    """
    Inserta múltiples documentos en MongoDB, evitando duplicados por ciudad
    dentro de una ventana de 45 minutos.
    """
   
    collection.create_index([("name", 1), ("dt", -1)])

    inserted_count = 0
    for doc in data:
        try:
            city_name = doc.get("name")
            city_dt = doc.get("dt")         # UNIX

            latest = collection.find_one(
                {"name": city_name},
                sort=[("dt", -1)]
            )

            if latest and (city_dt - latest["dt"]) < 2700:  # 45 min
                logging.info(f"[{city_name}] Ya existe un registro reciente. Salteando.")
                continue

            collection.insert_one(doc)
            logging.info(f"[{city_name}] Insertado exitosamente.")
            inserted_count += 1

        except KeyError as e:
            logging.error(f"[{doc.get('name', 'UNKNOWN')}] Faltan claves esperadas: {e}")
        except Exception as e:
            logging.exception(f"[{doc.get('name', 'UNKNOWN')}] Error al insertar.")

    logging.info(f"Total de documentos insertados: {inserted_count}")

def load_to_mysql(df: DataFrame, db_config: dict, table_name: str) -> None:
    """
    Carga un DataFrame de Spark en una tabla MySQL usando JDBC.
    """
    try:
        df.write.format('jdbc') \
            .option("url", f"jdbc:mysql://{db_config['host']}:{db_config['port']}/{db_config['database']}") \
            .option("dbtable", table_name) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()

        logging.info(f"Datos cargados a MySQL")

    except Exception as e:
        logging.exception("Error en la carga de datos a MySQL")
        raise
