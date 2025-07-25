from pyspark.sql import SparkSession
from typing import Optional, Dict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_spark_session(app_name: str, mongo_config: Optional[Dict[str, str]] = None) -> SparkSession:
    """
    Inicializa una SparkSession con conectores para MongoDB y MySQL.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
                "mysql:mysql-connector-java:8.0.33")

    if mongo_config:
        uri = f"mongodb://{mongo_config['host']}:{mongo_config['port']}/{mongo_config['database']}.{mongo_config['collection']}"
        builder = builder \
            .config("spark.mongodb.read.connection.uri", uri) \
            .config("spark.mongodb.write.connection.uri", uri)

    spark = builder.getOrCreate()
    return spark