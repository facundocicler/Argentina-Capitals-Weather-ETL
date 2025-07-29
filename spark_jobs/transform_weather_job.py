import logging
from config.spark_config import get_spark_session
from src.transform import flatten_weather_df, transform_with_spark
from config.config_loader import get_mongo_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    try:
        mongo_config = get_mongo_config()

        spark = get_spark_session("TransformJob", mongo_config)

        df = transform_with_spark(spark, , mongo_config)

        flat_df = flatten_weather_df(df)
        output_path = "/tmp/flat_weather_data.parquet"

        logging.info(f"Guardando datos transformados en: {output_path}")
        flat_df.write.mode("overwrite").parquet(output_path)
        logging.info("Archivo Parquet guardado exitosamente.")

    except Exception as e:
        logging.exception("Error inesperado durante la transformación de datos.")
        raise

if __name__ == "__main__":
    main()
