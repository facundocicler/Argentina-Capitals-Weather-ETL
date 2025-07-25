import logging
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def filter_new_records(spark: SparkSession, new_df: DataFrame, db_config: dict, table_name: str) -> DataFrame:
    """
    Filtra los registros nuevos que aún no existen en la base de datos MySQL.
    """
    try:
        existing_df = (
            spark.read
            .format("jdbc")
            .option("url", f"jdbc:mysql://{db_config['host']}:{db_config['port']}/{db_config['database']}")
            .option("dbtable", table_name)
            .option("user", db_config['user'])
            .option("password", db_config['password'])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
            .select("id", "ingestion_datetime")
        )

        df_to_insert = new_df.join(
            existing_df,
            on=["id", "ingestion_datetime"],
            how="left_anti"
        )

        if df_to_insert.rdd.isEmpty():
            logging.info("Todos los registros ya existen. Nada que insertar.")
        else:
            logging.info(f"{df_to_insert.count()} registros nuevos listos para insertar.")

        return df_to_insert

    except Exception as e:
        logging.exception("Error al filtrar registros duplicados.")
        raise