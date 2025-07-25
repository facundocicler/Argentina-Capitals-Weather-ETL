import logging
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col, when, from_unixtime, from_utc_timestamp, round as spark_round

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def transform_with_spark(spark: SparkSession) -> DataFrame:
    """
    Carga los datos desde MongoDB y devuelve un DataFrame Spark
    """
    df = spark.read.format("mongodb") \
        .option("uri", "mongodb://mongo:27017/bs_as_weather.raw_weather_data") \
        .load()

    if df is None or df.rdd.isEmpty():
        logging.warning("El DataFrame extraído está vacío. Finalizando sin guardar datos.")
        raise RuntimeError("DataFrame vacío")

    required_top_level_columns = {"coord", "main", "sys", "weather", "wind", "clouds", "visibility", "dt"}
    missing = required_top_level_columns - set(df.columns)
    if missing:
        raise KeyError(f"Faltan columnas de nivel superior: {missing}")

    count = df.count()
    logging.info(f"Cantidad de registros: {count}")

    return df

def kelvin_to_celsius(kelvin_col: Column) -> Column:
    """
    Convierte una columna de temperatura de Kelvin a Celsius, redondeando a 2 decimales
    """
    return spark_round(kelvin_col - 273.15, 2)

def wind_direction_expr(deg_col: Column) -> Column:
    """
    Convierte grados en punto cardinal usando expresiones condicionales Spark
    """
    return (
        when((deg_col >= 337.5) | (deg_col < 22.5), "N")
        .when((deg_col >= 22.5) & (deg_col < 67.5), "NE")
        .when((deg_col >= 67.5) & (deg_col < 112.5), "E")
        .when((deg_col >= 112.5) & (deg_col < 157.5), "SE")
        .when((deg_col >= 157.5) & (deg_col < 202.5), "S")
        .when((deg_col >= 202.5) & (deg_col < 247.5), "SW")
        .when((deg_col >= 247.5) & (deg_col < 292.5), "W")
        .when((deg_col >= 292.5) & (deg_col < 337.5), "NW")
        .otherwise("Unknown")
    )

def unix_to_ts_arg(unix_col: Column) -> Column:
    """
    Convierte timestamps UNIX a zona horaria de Buenos Aires
    """
    return from_utc_timestamp(from_unixtime(unix_col), 'America/Argentina/Buenos_Aires')

def flatten_weather_df(df: DataFrame) -> DataFrame:
    """
    Selecciona y transforma columnas relevantes del DataFrame con datos de clima
    haciendo conversiones de unidades y renombrando columnas con nombres claros
    """
    return df.select(
        col("id").alias("id"),
        col("city").alias("city_name"),
        col("province").alias("province"),
        col("sys").getField("country").alias("country"),
        col("coord").getField("lat").alias("latitude"),
        col("coord").getField("lon").alias("longitude"),
        kelvin_to_celsius(col("main").getField("temp")).alias("temperature_celsius"),
        kelvin_to_celsius(col("main").getField("feels_like")).alias("feels_like_celsius"),
        kelvin_to_celsius(col("main").getField("temp_min")).alias("temp_min_celsius"),
        kelvin_to_celsius(col("main").getField("temp_max")).alias("temp_max_celsius"),
        col("main").getField("humidity").alias("humidity_percent"),
        col("main").getField("pressure").alias("pressure_hpa"),
        col("wind").getField("speed").alias("wind_speed_mps"),
        col("wind").getField("deg").alias("wind_direction_deg"),
        wind_direction_expr(col("wind").getField("deg")).alias("wind_direction_card"),
        col("clouds").getField("all").alias("cloudiness_percent"),
        col("weather").getItem(0).getField("main").alias("weather_main"),
        col("weather").getItem(0).getField("description").alias("weather_description"),
        col("visibility").alias("visibility_meters"),
        unix_to_ts_arg(col("sys").getField("sunrise")).alias("sunrise_ts"),
        unix_to_ts_arg(col("sys").getField("sunset")).alias("sunset_ts"),
        unix_to_ts_arg(col("dt")).alias("date_time")
    )
