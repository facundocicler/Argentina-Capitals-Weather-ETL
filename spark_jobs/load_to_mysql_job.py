import logging
from config.config_loader import get_mysql_config
from config.spark_config import get_spark_session
from src.mysql_utils import filter_new_records
from src.load import load_to_mysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():  
    try:
        mysql_config = get_mysql_config()

        spark = get_spark_session("Load_To_MySQL", mongo_config=None)

        parquet_path = "/tmp/flat_weather_data.parquet"
        new_df = spark.read.parquet(parquet_path)

        if new_df.rdd.isEmpty():
            logging.warning("El archivo Parquet está vacío. Cancelando carga.")
            raise

        df_to_insert = filter_new_records(spark, new_df, mysql_config, "argentina_weather_data")

        if df_to_insert.rdd.isEmpty():
            logging.info("Nada nuevo que cargar.")
            return
        
        load_to_mysql(df_to_insert, mysql_config, "argentina_weather_data")

    except Exception as e:
        logging.exception("Error inesperado durante la carga en MySQL")
        raise

if __name__ == "__main__":
    main()