"""
Silver — Déduplication des sources Bronze (projet NYC Mobility, RNCP36739).

Principe medallion :
  - Bronze reste BRUT et immuable (doublons inclus) -> on n'y touche pas.
  - Silver nettoie : on déduplique ici, par clé naturelle, en gardant la
    version la plus récemment ingérée (colonne `ingested_at` si présente).

Exécution : job AWS Glue (PySpark) ou Spark local.
  - S3 (Taxi, Météo)   : lecture Parquet directe.
  - DynamoDB (311)     : connecteur Glue `dynamodb`.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------
# Clés naturelles par source (à documenter dans le rapport).
# NB : adapter les noms de colonnes Taxi après un df.printSchema()
#      (Yellow = tpep_*, Green = lpep_*, casse variable selon l'année TLC).
# --------------------------------------------------------------------------
KEYS_311 = ["unique_key"]                       # vraie clé primaire
KEYS_WEATHER = ["time"]                          # une ligne par heure
KEYS_TAXI_YELLOW = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                    "PULocationID", "DOLocationID", "total_amount"]   # heuristique
KEYS_TAXI_GREEN = ["VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
                   "PULocationID", "DOLocationID", "total_amount"]    # heuristique

S3_BRONZE = "s3a://nyc-mobility-bronze-am2024/bronze"
S3_SILVER = "s3a://nyc-mobility-bronze-am2024/silver"
DDB_TABLE_311 = "nyc-311-bronze"


# --------------------------------------------------------------------------
# Briques réutilisables
# --------------------------------------------------------------------------
def deduplicate(df, key_cols, order_col="ingested_at"):
    """Garde une seule ligne par clé naturelle.

    Si `order_col` existe -> garde la plus récente (row_number sur fenêtre).
    Sinon -> dropDuplicates simple (les doublons d'ingestion étant identiques,
    n'importe quelle ligne conservée convient).
    """
    if order_col in df.columns:
        w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
        return (df.withColumn("_rn", F.row_number().over(w))
                  .filter(F.col("_rn") == 1)
                  .drop("_rn"))
    return df.dropDuplicates(key_cols)


def assert_unique(df, key_cols, name):
    """Garde-fou + preuve qualité (Bloc 5). Lève une erreur si des doublons restent."""
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    if total != distinct:
        raise ValueError(f"[{name}] {total - distinct} doublon(s) restant(s) sur {key_cols}")
    print(f"[{name}] OK — {total} lignes uniques sur {key_cols}")
    return df


def report_dedup(df_before, df_after, name):
    """Affiche le bilan avant/après — chiffres à mettre dans le rapport."""
    before, after = df_before.count(), df_after.count()
    print(f"[{name}] {before} -> {after} lignes  ({before - after} doublon(s) supprimé(s))")


# --------------------------------------------------------------------------
# Lecture des sources
# --------------------------------------------------------------------------
def read_taxi(spark, taxi_type, key_cols):
    """Lit un type de taxi (yellow|green) depuis S3, dédup + écrit en Silver."""
    raw = spark.read.parquet(f"{S3_BRONZE}/taxi/*/*/taxi_type={taxi_type}/")
    # 1) passe peu coûteuse : tue les lignes 100 % identiques (relance de fichier)
    raw = raw.dropDuplicates()
    # 2) dédup logique sur la clé naturelle
    clean = deduplicate(raw, key_cols)
    report_dedup(raw, clean, f"taxi_{taxi_type}")
    assert_unique(clean, key_cols, f"taxi_{taxi_type}")
    clean.write.mode("overwrite").parquet(f"{S3_SILVER}/taxi/taxi_type={taxi_type}/")
    return clean


def read_weather(spark):
    raw = spark.read.parquet(f"{S3_BRONZE}/weather/*/*/").dropDuplicates()
    clean = deduplicate(raw, KEYS_WEATHER)
    report_dedup(raw, clean, "weather")
    assert_unique(clean, KEYS_WEATHER, "weather")
    clean.write.mode("overwrite").parquet(f"{S3_SILVER}/weather/")
    return clean


def read_311(glue_context):
    """Lit la table DynamoDB via le connecteur Glue, puis dédup (ceinture +
    bretelles : la PK unique_key garantit déjà l'unicité côté DynamoDB)."""
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={"dynamodb.input.tableName": DDB_TABLE_311,
                            "dynamodb.throughput.read.percent": "0.5"},
    )
    raw = dyf.toDF()
    clean = deduplicate(raw, KEYS_311)
    report_dedup(raw, clean, "nyc_311")
    assert_unique(clean, KEYS_311, "nyc_311")
    clean.write.mode("overwrite").parquet(f"{S3_SILVER}/nyc_311/")
    return clean


# --------------------------------------------------------------------------
# Point d'entrée (Glue job)
# --------------------------------------------------------------------------
def main():
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    read_taxi(spark, "yellow", KEYS_TAXI_YELLOW)
    read_taxi(spark, "green", KEYS_TAXI_GREEN)
    read_weather(spark)
    read_311(glue_context)

    print("Silver — déduplication terminée.")


if __name__ == "__main__":
    main()