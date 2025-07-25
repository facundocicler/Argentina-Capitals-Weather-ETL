import logging
from pymongo import MongoClient
from pymongo.collection import Collection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_to_mongo(host: str, port: int, database: str, collection: str) -> Collection:
    """Establece la conexión con una colección de MongoDB."""
    try:
        client = MongoClient(host=host, port=port)
        db = client[database]
        return db[collection]
    except Exception as e:
        logging.error(f"❌ Error al conectar con MongoDB: {e}")
        raise