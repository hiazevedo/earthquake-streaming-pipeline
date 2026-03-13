# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, ArrayType
)
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configurações
RAW_JSON_PATH   = "/Volumes/earthquake_pipeline/bronze/raw_json"
BRONZE_TABLE    = "earthquake_pipeline.bronze.earthquakes"
CHECKPOINT_PATH = "/Volumes/earthquake_pipeline/checkpoints/streaming/bronze"

print("✅ Configurações carregadas")
print(f"   Fonte      : {RAW_JSON_PATH}")
print(f"   Destino    : {BRONZE_TABLE}")
print(f"   Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

# DBTITLE 1,Definir schema do JSON da USGS
# Sempre definir schema explícito no streaming — nunca usar inferência
# Schema da geometria GeoJSON
geometry_schema = StructType([
    StructField("type",        StringType(), True),
    StructField("coordinates", ArrayType(DoubleType()), True)
])

# Schema das propriedades do evento
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

# Schema do feature (cada terremoto)
feature_schema = StructType([
    StructField("type",       StringType(),       True),
    StructField("properties", properties_schema,  True),
    StructField("geometry",   geometry_schema,    True),
    StructField("id",         StringType(),       True),
])

# Schema raiz do arquivo JSON
root_schema = StructType([
    StructField("collected_at", StringType(),              True),
    StructField("total_events", IntegerType(),             True),
    StructField("source",       StringType(),              True),
    StructField("features",     ArrayType(feature_schema), True),
])

print("✅ Schema definido com sucesso")
print(f"   Campos no root    : {len(root_schema.fields)}")
print(f"   Campos properties : {len(properties_schema.fields)}")

# COMMAND ----------

# DBTITLE 1,Auto Loader: ler JSONs do Volume como stream
print("📡 Iniciando Auto Loader...")

df_stream_raw = (
    spark.readStream
    .format("cloudFiles")                        # Auto Loader
    .option("cloudFiles.format", "json")         # formato dos arquivos
    .option("cloudFiles.schemaLocation",         # schema inference location
            f"{CHECKPOINT_PATH}/schema")
    .option("multiLine", "true")                 # JSON multilinha
    .schema(root_schema)                         # schema explícito
    .load(RAW_JSON_PATH)
)

print("✅ Stream criado com Auto Loader")
print(f"   isStreaming: {df_stream_raw.isStreaming}")

# COMMAND ----------

# DBTITLE 1,Explodir array de features e achatar o JSON
# Cada arquivo tem N terremotos em um array — precisamos de 1 linha por evento

df_stream_flat = (
    df_stream_raw

    # Explodir o array features — 1 linha por terremoto
    .withColumn("feature", F.explode("features"))

    # Extrair campos de properties
    .withColumn("event_id",       F.col("feature.id"))
    .withColumn("magnitude",      F.col("feature.properties.mag"))
    .withColumn("place",          F.col("feature.properties.place"))
    .withColumn("event_time",     F.to_timestamp(
                                    F.col("feature.properties.time") / 1000))
    .withColumn("updated_time",   F.to_timestamp(
                                    F.col("feature.properties.updated") / 1000))
    .withColumn("alert",          F.col("feature.properties.alert"))
    .withColumn("status",         F.col("feature.properties.status"))
    .withColumn("tsunami",        F.col("feature.properties.tsunami"))
    .withColumn("sig",            F.col("feature.properties.sig"))
    .withColumn("net",            F.col("feature.properties.net"))
    .withColumn("mag_type",       F.col("feature.properties.magType"))
    .withColumn("felt",           F.col("feature.properties.felt"))
    .withColumn("cdi",            F.col("feature.properties.cdi"))
    .withColumn("mmi",            F.col("feature.properties.mmi"))
    .withColumn("nst",            F.col("feature.properties.nst"))
    .withColumn("dmin",           F.col("feature.properties.dmin"))
    .withColumn("rms",            F.col("feature.properties.rms"))
    .withColumn("gap",            F.col("feature.properties.gap"))
    .withColumn("url",            F.col("feature.properties.url"))

    # Extrair coordenadas geográficas
    .withColumn("longitude",      F.col("feature.geometry.coordinates")[0])
    .withColumn("latitude",       F.col("feature.geometry.coordinates")[1])
    .withColumn("depth_km",       F.col("feature.geometry.coordinates")[2])

    # Metadados de auditoria
    .withColumn("collected_at",   F.to_timestamp("collected_at"))
    .withColumn("_ingested_at",   F.current_timestamp())
    .withColumn("_source_file",   F.col("_metadata.file_path"))

    # Remover colunas intermediárias
    .drop("features", "feature", "total_events", "source")
)

print("✅ Transformações de achatamento definidas")
print(f"   Colunas geradas: {len(df_stream_flat.columns)}")

# COMMAND ----------

# DBTITLE 1,Escrever stream na tabela Bronze (Delta Lake)
# CÉLULA 5 — Escrever stream na tabela Bronze (Delta Lake)
print("💾 Iniciando escrita na camada Bronze...")

query_bronze = (
    df_stream_flat
    .writeStream
    .format("delta")
    .outputMode("append")                        # append-only para Bronze
    .option("checkpointLocation",
            f"{CHECKPOINT_PATH}/bronze_writer")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                  # processa tudo e para
    .toTable(BRONZE_TABLE)
)

query_bronze.awaitTermination()
print("✅ Bronze escrito com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação da tabela Bronze
print("🔍 Validando camada Bronze...\n")

df_bronze = spark.table(BRONZE_TABLE)
total = df_bronze.count()

print(f"📊 Total de registros: {total:,}")
print(f"\n📋 Schema:")
df_bronze.printSchema()

print("\n👀 Amostra dos dados:")
display(df_bronze.select(
    "event_id", "magnitude", "place",
    "event_time", "latitude", "longitude",
    "depth_km", "tsunami", "alert",
    "_source_file"
).orderBy(F.desc("magnitude")).limit(10))

print("\n📊 Distribuição por magnitude:")
display(spark.sql("""
    SELECT
        CASE
            WHEN magnitude >= 7.0 THEN '≥ 7.0 — Major'
            WHEN magnitude >= 6.0 THEN '6.0–6.9 — Strong'
            WHEN magnitude >= 5.0 THEN '5.0–5.9 — Moderate'
            WHEN magnitude >= 4.0 THEN '4.0–4.9 — Light'
            WHEN magnitude >= 2.5 THEN '2.5–3.9 — Minor'
            ELSE '< 2.5 — Micro'
        END AS categoria,
        COUNT(*) AS total
    FROM earthquake_pipeline.bronze.earthquakes
    GROUP BY categoria
    ORDER BY MIN(magnitude) DESC
"""))

print("\n✅ BRONZE CONCLUÍDO!")