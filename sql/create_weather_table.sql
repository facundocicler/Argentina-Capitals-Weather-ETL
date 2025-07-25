CREATE DATABASE IF NOT EXISTS weather;

USE weather;

CREATE TABLE IF NOT EXISTS argentina_weather_data (
  id                    INT,
  city_name             VARCHAR(40),
  province              VARCHAR(40),
  country               CHAR(2),
  latitude              DOUBLE,
  longitude             DOUBLE,
  temperature_celsius   DOUBLE,
  feels_like_celsius    DOUBLE,
  temp_min_celsius      DOUBLE,
  temp_max_celsius      DOUBLE,
  humidity_percent      INT,
  pressure_hpa          INT,
  wind_speed_mps        DOUBLE,
  wind_direction_deg    INT,
  wind_direction_card   VARCHAR(3),
  cloudiness_percent    INT,
  weather_main          VARCHAR(40),
  weather_description   VARCHAR(100),
  visibility_meters     INT,
  sunrise_ts            TIMESTAMP,
  sunset_ts             TIMESTAMP,
  date_time             TIMESTAMP,
  PRIMARY KEY (id, date_time)
);
