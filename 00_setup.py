# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração do ambiente, catalog, schemas e volumes

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente, catalog, schemas e volumes
print("=" * 60)
print("  SETUP — earthquake-streaming-pipeline")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Criar Catalog
spark.sql("""
    CREATE CATALOG IF NOT EXISTS earthquake_pipeline
    COMMENT 'Catalog do projeto earthquake-streaming-pipeline'
""")
print("\n✅ Catalog criado: earthquake_pipeline")

# COMMAND ----------

# DBTITLE 1,Criar Schemas
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS earthquake_pipeline.bronze
    COMMENT 'Raw data — eventos brutos da API USGS'
""")
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS earthquake_pipeline.silver
    COMMENT 'Dados limpos, tipados e enriquecidos'
""")
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS earthquake_pipeline.gold
    COMMENT 'Alertas e agregações analíticas'
""")
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS earthquake_pipeline.checkpoints
    COMMENT 'Checkpoints do Structured Streaming'
""")
print("✅ Schemas criados: bronze, silver, gold, checkpoints")

# COMMAND ----------

# DBTITLE 1,Criar Volumes
spark.sql("""
    CREATE VOLUME IF NOT EXISTS earthquake_pipeline.bronze.raw_json
    COMMENT 'Arquivos JSON brutos coletados da API USGS'
""")
spark.sql("""
    CREATE VOLUME IF NOT EXISTS earthquake_pipeline.checkpoints.streaming
    COMMENT 'Checkpoints do Structured Streaming'
""")
print("✅ Volumes criados: bronze/raw_json, checkpoints/streaming")

# COMMAND ----------

# DBTITLE 1,Definir paths globais
RAW_JSON_PATH    = "/Volumes/earthquake_pipeline/bronze/raw_json"
CHECKPOINT_PATH  = "/Volumes/earthquake_pipeline/checkpoints/streaming"
BRONZE_TABLE     = "earthquake_pipeline.bronze.earthquakes"
SILVER_TABLE     = "earthquake_pipeline.silver.earthquakes"
GOLD_ALERTS      = "earthquake_pipeline.gold.alerts"
GOLD_SUMMARY     = "earthquake_pipeline.gold.summary"

print(f""" SETUP CONCLUÍDO COM SUCESSO! ✅

Catalog   : earthquake_pipeline
Bronze    : earthquake_pipeline.bronze
Silver    : earthquake_pipeline.silver
Gold      : earthquake_pipeline.gold
Raw JSON  : /Volumes/earthquake_pipeline/bronze/raw_json
""")

# COMMAND ----------

# DBTITLE 1,Validar estrutura
print("📋 Schemas criados:")
display(spark.sql("SHOW SCHEMAS IN earthquake_pipeline"))

print("\n📦 Volumes no schema bronze:")
display(spark.sql("SHOW VOLUMES IN earthquake_pipeline.bronze"))

print("\n📦 Volumes no schema checkpoints:")
display(spark.sql("SHOW VOLUMES IN earthquake_pipeline.checkpoints"))