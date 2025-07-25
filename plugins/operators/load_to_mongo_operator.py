from airflow.models import BaseOperator
from src.load import load_to_mongo
from config.mongo_config import connect_to_mongo
from config.config_loader import get_mongo_config

class LoadRawDataOperator(BaseOperator):
    def execute(self, context):
        try:
            mongo_config = get_mongo_config()

            self.log.info("Extrayendo datos desde xcom...")
            raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract_weather_data')

            if not raw_data:
                raise ValueError("No se encontraron datos en XCom para la clave 'raw_data'.")

            self.log.info("Datos recuperados desde XCom correctamente.")
            collection = connect_to_mongo(**mongo_config)

            load_to_mongo(collection, raw_data)
            self.log.info("Carga a MongoDB completada exitosamente.")

        except Exception as e:
            self.log.error(f"Error durante la carga de datos a MongoDB: {e}")
            raise