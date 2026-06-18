"""
Validation LOCALE du job Silver (gratuit, sans Glue).
Teste taxi + météo + zones(CSV). Le 311 (DynamoDB) se teste en Glue.

Lancer depuis n'importe où :
    python silver/run_local.py
"""
import sys
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)  # pour importer build_trips

from pyspark.sql import SparkSession      # noqa: E402
import build_trips as sb                  # noqa: E402

BRONZE = os.path.join(ROOT, "data", "bronze")
ZONES_CSV = os.path.join(ROOT, "data", "taxi_zone_lookup.csv")


def main():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("silver-local")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    taxi = sb.load_taxi(spark, BRONZE)
    weather = sb.load_weather(spark, BRONZE)
    zones = sb.load_zones(spark, source="csv", csv_path=ZONES_CSV)

    trips = sb.build_silver_trips(taxi, zones, weather, agg311=None)

    sb.match_rate(trips, "pickup_borough", "zones")
    sb.match_rate(trips, "temperature_2m", "meteo")
    print("Lignes Silver :", trips.count())
    trips.select("taxi_type", "pickup_datetime", "pickup_borough",
                 "dropoff_borough", "temperature_2m", "total_amount").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()