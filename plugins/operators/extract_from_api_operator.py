from airflow.models import BaseOperator
from src.extract import fetch_data
from config.config_loader import get_api_key, load_locations

class ExtractWeatherDataOperator(BaseOperator):
    def execute(self, context):
        try:
            self.log.info("Iniciando extracción de datos del clima de todas las capitales...")
            locations = load_locations('config/argentina_locations.json')
            raw_data = fetch_data(api_key=get_api_key(), locations=locations)

            if not raw_data:
                raise ValueError("No se extrajeron datos de la API.")

            self.log.info(f"Se extrajeron datos de {len(raw_data)} ciudades.")
            context['ti'].xcom_push(key='raw_data', value=raw_data)

        except Exception as e:
            self.log.error(f"Error durante la extracción: {e}")
            raise