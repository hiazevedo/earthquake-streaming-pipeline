# Databricks notebook source
import time
from datetime import datetime, timezone, timedelta
import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

RAW_JSON_PATH   = "/Volumes/earthquake_pipeline/bronze/raw_json"
BRONZE_TABLE    = "earthquake_pipeline.bronze.earthquakes"
SILVER_TABLE    = "earthquake_pipeline.silver.earthquakes"
GOLD_ALERTS     = "earthquake_pipeline.gold.alerts"

INTERVALO_SEG   = 300   # coleta a cada 5 minutos
MAX_CICLOS      = 3    # roda por 1 hora (12 x 5min)

print("✅ Configurações carregadas")
print(f"   Intervalo  : {INTERVALO_SEG}s ({INTERVALO_SEG//60} minutos)")
print(f"   Max ciclos : {MAX_CICLOS}")
print(f"   Duração    : ~{(INTERVALO_SEG * MAX_CICLOS)//60} minutos")

# COMMAND ----------

# DBTITLE 1,CÉLULA 2 — Funções do pipeline
# Funções do pipeline

def fetch_and_save(minutes_back: int = 10) -> int:
    """Coleta eventos recentes da API USGS e salva no Volume."""

    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes_back)

    params = {
        "format":       "geojson",
        "starttime":    start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime":      end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": 1.0,
        "limit":        500,
        "orderby":      "time"
    }

    resp = requests.get(
        "https://earthquake.usgs.gov/fdsnws/event/1/query",
        params=params, timeout=30
    )
    resp.raise_for_status()
    data = resp.json()

    eventos = len(data["features"])
    if eventos == 0:
        return 0

    timestamp = end_time.strftime("%Y%m%d_%H%M%S")
    filepath  = f"{RAW_JSON_PATH}/earthquakes_{timestamp}.json"
    batch     = {
        "collected_at" : end_time.isoformat(),
        "total_events" : eventos,
        "source"       : "USGS Earthquake Hazards Program",
        "features"     : data["features"]
    }
    dbutils.fs.put(filepath, json.dumps(batch), overwrite=True)
    return eventos


def run_bronze_stream() -> int:
    """Processa novos JSONs do Volume para a tabela Bronze."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType,
        DoubleType, IntegerType, ArrayType
    )

    geometry_schema = StructType([
        StructField("type",        StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True)
    ])
    properties_schema = StructType([
        StructField("mag",     DoubleType(),  True),
        StructField("place",   StringType(),  True),
        StructField("time",    LongType(),    True),
        StructField("updated", LongType(),    True),
        StructField("tz",      IntegerType(), True),
        StructField("url",     StringType(),  True),
        StructField("detail",  StringType(),  True),
        StructField("felt",    IntegerType(), True),
        StructField("cdi",     DoubleType(),  True),
        StructField("mmi",     DoubleType(),  True),
        StructField("alert",   StringType(),  True),
        StructField("status",  StringType(),  True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig",     IntegerType(), True),
        StructField("net",     StringType(),  True),
        StructField("code",    StringType(),  True),
        StructField("ids",     StringType(),  True),
        StructField("sources", StringType(),  True),
        StructField("types",   StringType(),  True),
        StructField("nst",     IntegerType(), True),
        StructField("dmin",    DoubleType(),  True),
        StructField("rms",     DoubleType(),  True),
        StructField("gap",     DoubleType(),  True),
        StructField("magType", StringType(),  True),
        StructField("type",    StringType(),  True),
        StructField("title",   StringType(),  True),
    ])
    feature_schema = StructType([
        StructField("type",       StringType(),      True),
        StructField("properties", properties_schema, True),
        StructField("geometry",   geometry_schema,   True),
        StructField("id",         StringType(),      True),
    ])
    root_schema = StructType([
        StructField("collected_at", StringType(),              True),
        StructField("total_events", IntegerType(),             True),
        StructField("source",       StringType(),              True),
        StructField("features",     ArrayType(feature_schema), True),
    ])

    CHECKPOINT = "/Volumes/earthquake_pipeline/checkpoints/streaming/bronze"

    query = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
        .option("multiLine", "true")
        .schema(root_schema)
        .load(RAW_JSON_PATH)
        .withColumn("feature",      F.explode("features"))
        .withColumn("event_id",     F.col("feature.id"))
        .withColumn("magnitude",    F.col("feature.properties.mag"))
        .withColumn("place",        F.col("feature.properties.place"))
        .withColumn("event_time",   F.to_timestamp(
                                      F.col("feature.properties.time") / 1000))
        .withColumn("updated_time", F.to_timestamp(
                                      F.col("feature.properties.updated") / 1000))
        .withColumn("alert",        F.col("feature.properties.alert"))
        .withColumn("status",       F.col("feature.properties.status"))
        .withColumn("tsunami",      F.col("feature.properties.tsunami"))
        .withColumn("sig",          F.col("feature.properties.sig"))
        .withColumn("net",          F.col("feature.properties.net"))
        .withColumn("mag_type",     F.col("feature.properties.magType"))
        .withColumn("felt",         F.col("feature.properties.felt"))
        .withColumn("nst",          F.col("feature.properties.nst"))
        .withColumn("dmin",         F.col("feature.properties.dmin"))
        .withColumn("rms",          F.col("feature.properties.rms"))
        .withColumn("gap",          F.col("feature.geometry.coordinates")[2])
        .withColumn("longitude",    F.col("feature.geometry.coordinates")[0])
        .withColumn("latitude",     F.col("feature.geometry.coordinates")[1])
        .withColumn("depth_km",     F.col("feature.geometry.coordinates")[2])
        .withColumn("collected_at", F.to_timestamp("collected_at"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .drop("features", "feature")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/writer")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()
    return spark.table(BRONZE_TABLE).count()


def run_silver_stream() -> int:
    """Processa Bronze → Silver com deduplicação e enriquecimento."""
    CHECKPOINT = "/Volumes/earthquake_pipeline/checkpoints/streaming/silver"

    query = (
        spark.readStream
        .format("delta")
        .table(BRONZE_TABLE)
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("magnitude").isNotNull())
        .withWatermark("event_time", "24 hours")
        .dropDuplicates(["event_id"])
        .withColumn("magnitude_class",
            F.when(F.col("magnitude") >= 7.0, "Major")
             .when(F.col("magnitude") >= 6.0, "Strong")
             .when(F.col("magnitude") >= 5.0, "Moderate")
             .when(F.col("magnitude") >= 4.0, "Light")
             .when(F.col("magnitude") >= 2.5, "Minor")
             .otherwise("Micro"))
        .withColumn("depth_class",
            F.when(F.col("depth_km") <= 70,  "Shallow")
             .when(F.col("depth_km") <= 300, "Intermediate")
             .otherwise("Deep"))
        .withColumn("tsunami_warning",
            F.when(F.col("tsunami") == 1, True).otherwise(False))
        .withColumn("geo_region",
            F.when(
                (F.col("latitude").between(-56, 15)) &
                (F.col("longitude").between(-82, -34)), "South America")
             .when(
                (F.col("latitude").between(15, 72)) &
                (F.col("longitude").between(-168, -52)), "North America")
             .when(
                (F.col("latitude").between(35, 72)) &
                (F.col("longitude").between(-25, 45)), "Europe")
             .when(
                (F.col("latitude").between(0, 55)) &
                (F.col("longitude").between(52, 150)), "Asia")
             .when(
                (F.col("latitude").between(-50, 0)) &
                (F.col("longitude").between(110, 180)), "Oceania")
             .otherwise("Pacific / Other"))
        .withColumn("event_year",  F.year("event_time"))
        .withColumn("event_month", F.month("event_time"))
        .withColumn("event_day",   F.dayofmonth("event_time"))
        .withColumn("event_hour",  F.hour("event_time"))
        .withColumn("magnitude",   F.round("magnitude", 2))
        .withColumn("depth_km",    F.round("depth_km",  2))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("alert", "tsunami", "_ingested_at")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(SILVER_TABLE)
    )
    query.awaitTermination()
    return spark.table(SILVER_TABLE).count()


def run_gold() -> int:
    """Reconstrói Gold Alerts com os dados mais recentes da Silver."""
    df = spark.table(SILVER_TABLE)
    df_alerts = (
        df.withColumn("risk_score",
            F.round(
                (F.col("magnitude") / 10.0 * 60) +
                (F.when(F.col("tsunami_warning"), 30).otherwise(0)) +
                (F.coalesce(F.col("sig"), F.lit(0)) / 1000.0 * 10), 2))
          .withColumn("risk_level",
            F.when(F.col("risk_score") >= 70, "🔴 CRITICAL")
             .when(F.col("risk_score") >= 50, "🟠 HIGH")
             .when(F.col("risk_score") >= 30, "🟡 MEDIUM")
             .otherwise("🟢 LOW"))
          .orderBy(F.desc("risk_score"))
    )
    df_alerts.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(GOLD_ALERTS)
    return df_alerts.count()

print("✅ Funções do pipeline definidas")

# COMMAND ----------

# Loop principal de streaming contínuo

print("=" * 60)
print("  🌍 EARTHQUAKE STREAMING PIPELINE — INICIANDO")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

ciclo          = 0
total_coletado = 0

while ciclo < MAX_CICLOS:
    ciclo += 1
    agora = datetime.now().strftime("%H:%M:%S")

    print(f"\n{'─' * 55}")
    print(f"  🔄 CICLO {ciclo}/{MAX_CICLOS} — {agora}")
    print(f"{'─' * 55}")

    # ── 1. Coletar da API ─────────────────────────────────────────────────────
    try:
        novos = fetch_and_save(minutes_back=INTERVALO_SEG // 60 + 2)
        total_coletado += novos
        print(f"  📡 API USGS     → {novos} novos eventos coletados")
    except Exception as e:
        print(f"  ❌ Erro API     → {str(e)}")
        novos = 0

    # ── 2. Processar Bronze ───────────────────────────────────────────────────
    try:
        bronze_total = run_bronze_stream()
        print(f"  🥉 Bronze       → {bronze_total:,} registros totais")
    except Exception as e:
        print(f"  ❌ Erro Bronze  → {str(e)}")

    # ── 3. Processar Silver ───────────────────────────────────────────────────
    try:
        silver_total = run_silver_stream()
        print(f"  🥈 Silver       → {silver_total:,} registros totais")
    except Exception as e:
        print(f"  ❌ Erro Silver  → {str(e)}")

    # ── 4. Atualizar Gold ─────────────────────────────────────────────────────
    try:
        gold_total = run_gold()
        print(f"  🥇 Gold         → {gold_total:,} alertas atualizados")
    except Exception as e:
        print(f"  ❌ Erro Gold    → {str(e)}")

    # ── 5. Mini relatório do ciclo ────────────────────────────────────────────
    try:
        criticos = spark.sql("""
            SELECT COUNT(*) AS n
            FROM earthquake_pipeline.gold.alerts
            WHERE risk_level = '🔴 CRITICAL'
        """).collect()[0][0]

        tsunamis = spark.sql("""
            SELECT COUNT(*) AS n
            FROM earthquake_pipeline.gold.alerts
            WHERE tsunami_warning = true
        """).collect()[0][0]

        print(f"  ⚠️  Críticos     → {criticos} eventos CRITICAL")
        print(f"  🌊 Tsunamis     → {tsunamis} alertas ativos")
    except:
        pass

    # ── 6. Aguardar próximo ciclo (exceto no último) ──────────────────────────
    if ciclo < MAX_CICLOS:
        print(f"\n  ⏳ Aguardando {INTERVALO_SEG}s para o próximo ciclo...")
        time.sleep(INTERVALO_SEG)

print(f"""
{'=' * 60}
  ✅ STREAMING PIPELINE CONCLUÍDO
{'=' * 60}
  Ciclos executados : {ciclo}
  Total coletado    : {total_coletado} eventos
  Duração total     : ~{(INTERVALO_SEG * ciclo)//60} minutos
  Finalizado em     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'=' * 60}
""")