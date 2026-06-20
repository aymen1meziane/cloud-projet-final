"""
Silver — Construction de la table enrichie des courses (projet NYC Mobility, RNCP36739).

Grain de sortie : UNE ligne par course taxi, enrichie de :
  - borough / zone de prise en charge ET de dépose (jointure RDS zones, x2)
  - météo de l'heure de prise en charge (jointure Open-Meteo sur l'heure)
  - nb de signalements 311 du borough x heure (jointure 311 AGRÉGÉ -> pas d'explosion)

Sources Bronze :
  - Taxi   : Parquet S3 (yellow = tpep_*, green = lpep_*)
  - Météo  : JSON S3 (tableaux parallèles sous "hourly")
  - 311    : DynamoDB (connecteur Glue) -> agrégé par borough x heure
  - Zones  : RDS PostgreSQL (JDBC)
"""

import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Clé naturelle harmonisée d'une course (validée unique sur les vraies données)
TAXI_KEY = ["vendor_id", "pickup_datetime", "dropoff_datetime",
            "pu_location_id", "do_location_id", "total_amount"]

WEATHER_HOURLY_COLS = ["time", "temperature_2m", "precipitation", "windspeed_10m", "weathercode"]

S3_BRONZE = "s3a://nyc-mobility-bronze-am2024/bronze"
S3_SILVER = "s3a://nyc-mobility-silver-am2024/trips"
DDB_TABLE_311 = "nyc-311-bronze"


# ── Briques réutilisables ──────────────────────────────────────
def filter_period(df, ts_col, start="2024-01-01", end="2024-03-01"):
    """Retire les timestamps aberrants (défaut connu des fichiers TLC)."""
    before = df.count()
    clean = df.filter((F.col(ts_col) >= start) & (F.col(ts_col) < end))
    print(f"[filter_period] {before - clean.count()} ligne(s) hors plage retirée(s)")
    return clean


def deduplicate(df, key_cols, order_col="ingested_at"):
    """Dédup par clé naturelle (filet de sécurité ; garde la plus récente si dispo)."""
    if order_col in df.columns:
        w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return df.dropDuplicates(key_cols)


def assert_no_fanout(before_count, df_after, name):
    """Garantit qu'une jointure n'a PAS multiplié les lignes (anti-explosion)."""
    after = df_after.count()
    if after != before_count:
        raise ValueError(f"[{name}] explosion détectée : {before_count} -> {after} lignes !")
    print(f"[{name}] OK — {after} lignes (aucune multiplication)")
    return df_after


def match_rate(df, col, name):
    """Affiche le taux de match d'une jointure (preuve qualité pour le rapport)."""
    total = df.count()
    matched = df.filter(F.col(col).isNotNull()).count()
    print(f"[match] {name}: {100 * matched / total:.1f}% ({matched}/{total})")


def pseudonymize_gps(df, lat="latitude", lon="longitude", ndigits=2):
    """RGPD : arrondit les coordonnées (2 décimales ~ 1 km) -> privacy by design."""
    return (df.withColumn(lat, F.round(F.col(lat).cast("double"), ndigits))
              .withColumn(lon, F.round(F.col(lon).cast("double"), ndigits)))


# ── Lecture / transformation des sources ───────────────────────
def _read_taxi_type(spark, bronze_base, taxi_type):
    """Lit un type de taxi et le ramène au schéma harmonisé commun."""
    prefix = "tpep" if taxi_type == "yellow" else "lpep"
    raw = spark.read.parquet(f"{bronze_base}/taxi/*/*/taxi_type={taxi_type}/")
    return (raw.select(
        F.col("VendorID").alias("vendor_id"),
        F.col(f"{prefix}_pickup_datetime").alias("pickup_datetime"),
        F.col(f"{prefix}_dropoff_datetime").alias("dropoff_datetime"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("passenger_count"),
        F.col("trip_distance"),
        F.col("fare_amount"),
        F.col("total_amount"),
    ).withColumn("taxi_type", F.lit(taxi_type)))


def load_taxi(spark, bronze_base=S3_BRONZE):
    """Yellow + Green harmonisés, filtrés, dédupliqués, avec pickup_hour."""
    taxi = _read_taxi_type(spark, bronze_base, "yellow") \
        .unionByName(_read_taxi_type(spark, bronze_base, "green"))
    taxi = filter_period(taxi, "pickup_datetime")
    taxi = deduplicate(taxi, TAXI_KEY)
    return taxi.withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_datetime")))


def load_weather(spark, bronze_base=S3_BRONZE):
    """JSON imbriqué -> une ligne par heure, clé weather_hour (timestamp)."""
    raw = spark.read.option("multiLine", True).json(f"{bronze_base}/weather/*/*/*.json")
    exploded = raw.select(
        F.explode(F.arrays_zip(*[F.col(f"hourly.{c}").alias(c) for c in WEATHER_HOURLY_COLS]))
        .alias("h")
    ).select(*[F.col(f"h.{c}").alias(c) for c in WEATHER_HOURLY_COLS])
    return exploded.select(
        F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm").alias("weather_hour"),
        "temperature_2m", "precipitation", "windspeed_10m", "weathercode",
    ).dropDuplicates(["weather_hour"])


def load_zones(spark, args):
    """Référentiel zones depuis RDS (jdbc), prend directement le dictionnaire args."""
    
    jdbc_url = f"jdbc:postgresql://{args['RDS_HOST']}:{args['RDS_PORT']}/{args['RDS_DBNAME']}"
    
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "zones")
            .option("user", args["RDS_USER"])
            .option("password", args["RDS_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load().select(
                F.col("location_id"), 
                F.col("borough"), 
                F.col("zone_name").alias("zone") # <-- CORRECTION ICI : Sélection et renommage de 'zone_name'
            ))


def load_311_agg(glue_context):
    """311 (DynamoDB) AGRÉGÉ par borough x heure -> évite l'explosion N x M."""
    raw = glue_context.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={"dynamodb.input.tableName": DDB_TABLE_311,
                            "dynamodb.throughput.read.percent": "0.5"},
    ).toDF()
    return agg_311(raw)


def agg_311(raw):
    """Logique d'agrégation 311 (isolée pour être testable hors Glue)."""
    return (raw
            .filter(F.col("borough") != "unknown")
            .withColumn("b311_hour",
                        F.date_trunc("hour", F.to_timestamp("created_date",
                                                            "yyyy-MM-dd'T'HH:mm:ss.SSS")))
            .groupBy(F.upper("borough").alias("b311_borough"), "b311_hour")
            .agg(F.count(F.lit(1)).alias("nb_311"),
                 F.countDistinct("complaint_type").alias("nb_types_311")))


# ── Assemblage de la table Silver ──────────────────────────────
def build_silver_trips(taxi, zones, weather, agg311=None):
    """Joint toutes les sources autour de la course taxi (grain = 1 course)."""
    n = taxi.count()

    # 1) Double jointure zones : prise en charge + dépose (alias distincts)
    z_pu = zones.select(F.col("location_id").alias("_pu"),
                        F.col("borough").alias("pickup_borough"),
                        F.col("zone").alias("pickup_zone"))
    z_do = zones.select(F.col("location_id").alias("_do"),
                        F.col("borough").alias("dropoff_borough"),
                        F.col("zone").alias("dropoff_zone"))
    df = (taxi.join(z_pu, F.col("pu_location_id") == F.col("_pu"), "left").drop("_pu")
              .join(z_do, F.col("do_location_id") == F.col("_do"), "left").drop("_do"))

    # 2) Jointure météo sur l'heure de prise en charge
    df = df.join(weather, df.pickup_hour == weather.weather_hour, "left").drop("weather_hour")

    # 3) Jointure 311 agrégé : borough (MAJUSCULES des deux côtés) + heure
    if agg311 is not None:
        df = (df.join(agg311,
                      (F.upper(F.col("pickup_borough")) == F.col("b311_borough")) &
                      (F.col("pickup_hour") == F.col("b311_hour")), "left")
                .drop("b311_borough", "b311_hour")
                .fillna({"nb_311": 0, "nb_types_311": 0}))

    return assert_no_fanout(n, df, "build_silver_trips")


# ── Points d'entrée ────────────────────────────────────────────
def main():
    """Run complet sur AWS Glue (S3 + RDS JDBC + DynamoDB)."""
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    from awsglue.utils import getResolvedOptions

    # 1. Initialisation de Spark
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session

    # 2. Récupération stricte des paramètres (identique au script de test)
    # S'il manque un paramètre dans la console, le script s'arrêtera ici avec une vraie erreur lisible
    args = getResolvedOptions(
        sys.argv,
        ["RDS_HOST", "RDS_PORT", "RDS_DBNAME", "RDS_USER", "RDS_PASSWORD"]
    )

    # 3. Lancement des chargements (on passe 'args' directement à load_zones)
    taxi = load_taxi(spark)
    zones = load_zones(spark, args)
    weather = load_weather(spark)
    agg311 = load_311_agg(glue_context)

    # 4. Assemblage et écriture
    trips = build_silver_trips(taxi, zones, weather, agg311)
    match_rate(trips, "pickup_borough", "zones")
    match_rate(trips, "temperature_2m", "meteo")
    match_rate(trips, "nb_311", "311")

    trips.write.mode("overwrite").partitionBy("taxi_type").parquet(f"{S3_SILVER}/")
    print("Silver — table trips écrite avec succès.")


if __name__ == "__main__":
    main()