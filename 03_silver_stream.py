# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configurações
BRONZE_TABLE    = "earthquake_pipeline.bronze.earthquakes"
SILVER_TABLE    = "earthquake_pipeline.silver.earthquakes"
CHECKPOINT_PATH = "/Volumes/earthquake_pipeline/checkpoints/streaming/silver"

print("✅ Configurações carregadas")
print(f"   Fonte   : {BRONZE_TABLE}")
print(f"   Destino : {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Ler Bronze como stream
df_bronze_stream = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
)

print(f"✅ Stream Bronze criado")
print(f"   isStreaming: {df_bronze_stream.isStreaming}")

# COMMAND ----------

# DBTITLE 1,Transformações Silver
# Limpeza + enriquecimento + classificações

df_silver = (
    df_bronze_stream

    # Limpeza básica
    .filter(F.col("magnitude").isNotNull())
    .filter(F.col("event_time").isNotNull())
    .filter(F.col("latitude").isNotNull())
    .filter(F.col("longitude").isNotNull())

    # Deduplicação por event_id (withWatermark para streaming)
    .withWatermark("event_time", "24 hours")
    .dropDuplicates(["event_id"])

    # Classificação de magnitude (escala Richter)
    .withColumn("magnitude_class",
        F.when(F.col("magnitude") >= 8.0, "Great")
         .when(F.col("magnitude") >= 7.0, "Major")
         .when(F.col("magnitude") >= 6.0, "Strong")
         .when(F.col("magnitude") >= 5.0, "Moderate")
         .when(F.col("magnitude") >= 4.0, "Light")
         .when(F.col("magnitude") >= 2.5, "Minor")
         .otherwise("Micro")
    )

    # Classificação de profundidade
    .withColumn("depth_class",
        F.when(F.col("depth_km") <= 70,  "Shallow")   # superficial
         .when(F.col("depth_km") <= 300, "Intermediate")
         .otherwise("Deep")
    )

    # Nível de alerta padronizado
    .withColumn("alert_level",
        F.when(F.col("alert") == "red",    "🔴 Critical")
         .when(F.col("alert") == "orange", "🟠 High")
         .when(F.col("alert") == "yellow", "🟡 Medium")
         .when(F.col("alert") == "green",  "🟢 Low")
         .otherwise("⚪ Unknown")
    )

    # Flag de tsunami
    .withColumn("tsunami_warning",
        F.when(F.col("tsunami") == 1, True).otherwise(False)
    )

    # Geoenriquecimento: derivar hemisfério a partir de lat/lon
    .withColumn("hemisphere_lat",
        F.when(F.col("latitude") >= 0, "North").otherwise("South")
    )
    .withColumn("hemisphere_lon",
        F.when(F.col("longitude") >= 0, "East").otherwise("West")
    )

    # Derivar região geográfica aproximada por longitude/latitude
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
            (F.col("latitude").between(-10, 38)) &
            (F.col("longitude").between(-18, 52)), "Africa & Middle East")
         .when(
            (F.col("latitude").between(0, 55)) &
            (F.col("longitude").between(52, 150)), "Asia")
         .when(
            (F.col("latitude").between(-50, 0)) &
            (F.col("longitude").between(110, 180)), "Oceania")
         .when(
            (F.col("latitude") <= -60), "Antarctica")
         .otherwise("Pacific / Other")
    )

    # Extrair ano, mês, dia para facilitar análises
    .withColumn("event_year",  F.year("event_time"))
    .withColumn("event_month", F.month("event_time"))
    .withColumn("event_day",   F.dayofmonth("event_time"))
    .withColumn("event_hour",  F.hour("event_time"))

    # Arredondamentos
    .withColumn("magnitude",  F.round("magnitude", 2))
    .withColumn("depth_km",   F.round("depth_km",  2))
    .withColumn("latitude",   F.round("latitude",  4))
    .withColumn("longitude",  F.round("longitude", 4))

    # Auditoria
    .withColumn("_silver_processed_at", F.current_timestamp())

    # Remover colunas desnecessárias na Silver
    .drop("url", "ids", "sources", "types", "code",
          "alert", "tsunami", "_ingested_at")
)

print("✅ Transformações Silver definidas")
print(f"   Colunas: {len(df_silver.columns)}")

# COMMAND ----------

# DBTITLE 1,Escrever stream na tabela Silver
print("💾 Iniciando escrita na camada Silver...")

query_silver = (
    df_silver
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

query_silver.awaitTermination()
print("✅ Silver escrito com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação da Silver
print("🔍 Validando camada Silver...\n")

df_s = spark.table(SILVER_TABLE)
print(f"📊 Total de registros: {df_s.count():,}")

print("\n📊 Distribuição por magnitude_class:")
display(spark.sql("""
    SELECT magnitude_class, COUNT(*) AS total,
           ROUND(AVG(magnitude), 2)  AS mag_media,
           ROUND(AVG(depth_km),  2)  AS prof_media_km
    FROM earthquake_pipeline.silver.earthquakes
    GROUP BY magnitude_class
    ORDER BY mag_media DESC
"""))

print("\n🌍 Distribuição por geo_region:")
display(spark.sql("""
    SELECT geo_region,
           COUNT(*)                    AS total_eventos,
           ROUND(AVG(magnitude), 2)    AS mag_media,
           SUM(CASE WHEN tsunami_warning THEN 1 ELSE 0 END) AS alertas_tsunami
    FROM earthquake_pipeline.silver.earthquakes
    GROUP BY geo_region
    ORDER BY total_eventos DESC
"""))

print("\n⚠️ Eventos com alerta de tsunami:")
display(spark.sql("""
    SELECT event_id, magnitude, magnitude_class,
           place, geo_region, depth_km,
           depth_class, alert_level, event_time
    FROM earthquake_pipeline.silver.earthquakes
    WHERE tsunami_warning = true
    ORDER BY magnitude DESC
"""))

print("\n✅ SILVER CONCLUÍDO!")