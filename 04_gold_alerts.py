# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

SILVER_TABLE = "earthquake_pipeline.silver.earthquakes"
GOLD_ALERTS  = "earthquake_pipeline.gold.alerts"
GOLD_SUMMARY = "earthquake_pipeline.gold.summary"
GOLD_HEATMAP = "earthquake_pipeline.gold.heatmap_region"
GOLD_HOURLY  = "earthquake_pipeline.gold.hourly_activity"

print("✅ Configurações carregadas")

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)
print(f"📥 Silver carregado: {df_silver.count():,} registros")

# COMMAND ----------

print("🔨 Construindo gold.alerts...")

df_alerts = (
    df_silver
    .withColumn("risk_score",
        # Score de risco: magnitude (peso 60%) + tsunami (peso 30%) + sig (peso 10%)
        F.round(
            (F.col("magnitude") / 10.0 * 60) +
            (F.when(F.col("tsunami_warning"), 30).otherwise(0)) +
            (F.coalesce(F.col("sig"), F.lit(0)) / 1000.0 * 10),
        2)
    )
    .withColumn("risk_level",
        F.when(F.col("risk_score") >= 70, "🔴 CRITICAL")
         .when(F.col("risk_score") >= 50, "🟠 HIGH")
         .when(F.col("risk_score") >= 30, "🟡 MEDIUM")
         .otherwise("🟢 LOW")
    )
    .select(
        "event_id", "event_time", "magnitude", "magnitude_class",
        "place", "geo_region", "latitude", "longitude",
        "depth_km", "depth_class", "tsunami_warning",
        "alert_level", "risk_score", "risk_level",
        "sig", "felt", "net", "mag_type",
        "event_year", "event_month", "event_day", "event_hour"
    )
    .orderBy(F.desc("risk_score"))
)

(
    df_alerts
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_ALERTS)
)
print(f"✅ gold.alerts salvo: {df_alerts.count():,} registros")

# COMMAND ----------

print("🔨 Construindo gold.summary...")

df_summary = (
    df_silver
    .groupBy("geo_region", "magnitude_class")
    .agg(
        F.count("*")                              .alias("total_eventos"),
        F.round(F.avg("magnitude"),          2)   .alias("mag_media"),
        F.round(F.max("magnitude"),          2)   .alias("mag_maxima"),
        F.round(F.avg("depth_km"),           2)   .alias("prof_media_km"),
        F.sum(F.col("tsunami_warning")
               .cast("int"))                      .alias("alertas_tsunami"),
        F.sum(F.coalesce(F.col("felt"),
               F.lit(0)))                         .alias("total_sentidos"),
        F.round(F.avg(F.coalesce(F.col("sig"),
               F.lit(0))),            2)          .alias("sig_medio")
    )
    .orderBy("geo_region", F.desc("mag_media"))
)

(
    df_summary
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_SUMMARY)
)
print(f"✅ gold.summary salvo: {df_summary.count():,} registros")

# COMMAND ----------

print("🔨 Construindo gold.heatmap_region...")

df_heatmap = (
    df_silver
    .groupBy("geo_region", "depth_class", "magnitude_class")
    .agg(
        F.count("*")                         .alias("total"),
        F.round(F.avg("magnitude"),     2)   .alias("mag_media"),
        F.round(F.avg("depth_km"),      2)   .alias("prof_media"),
        F.sum(F.col("tsunami_warning")
               .cast("int"))                 .alias("tsunamis")
    )
    .orderBy("geo_region", "depth_class", F.desc("mag_media"))
)

(
    df_heatmap
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_HEATMAP)
)
print(f"✅ gold.heatmap_region salvo: {df_heatmap.count():,} registros")

# COMMAND ----------

print("🔨 Construindo gold.hourly_activity...")

df_hourly = (
    df_silver
    .groupBy("event_hour", "magnitude_class")
    .agg(
        F.count("*")                         .alias("total_eventos"),
        F.round(F.avg("magnitude"),     2)   .alias("mag_media"),
        F.sum(F.col("tsunami_warning")
               .cast("int"))                 .alias("tsunamis")
    )
    .orderBy("event_hour", "magnitude_class")
)

(
    df_hourly
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_HOURLY)
)
print(f"✅ gold.hourly_activity salvo: {df_hourly.count():,} registros")

# COMMAND ----------

print("=" * 60)
print("  VALIDAÇÃO FINAL — CAMADA GOLD")
print("=" * 60)

print("\n🔴 [1/4] Top 10 eventos mais críticos:")
display(spark.sql("""
    SELECT event_id, magnitude, magnitude_class,
           place, geo_region, depth_km, depth_class,
           tsunami_warning, risk_score, risk_level, event_time
    FROM earthquake_pipeline.gold.alerts
    ORDER BY risk_score DESC
    LIMIT 10
"""))

print("\n🌍 [2/4] Sumário por região:")
display(spark.sql("""
    SELECT geo_region,
           SUM(total_eventos)      AS total_eventos,
           ROUND(AVG(mag_media),2) AS mag_media,
           MAX(mag_maxima)         AS mag_maxima,
           SUM(alertas_tsunami)    AS total_tsunamis
    FROM earthquake_pipeline.gold.summary
    GROUP BY geo_region
    ORDER BY total_eventos DESC
"""))

print("\n🕐 [3/4] Horários de maior atividade sísmica:")
display(spark.sql("""
    SELECT event_hour,
           SUM(total_eventos) AS total,
           ROUND(AVG(mag_media), 2) AS mag_media
    FROM earthquake_pipeline.gold.hourly_activity
    GROUP BY event_hour
    ORDER BY total DESC
    LIMIT 10
"""))

print("\n⚠️  [4/4] KPIs gerais:")
display(spark.sql("""
    SELECT
        COUNT(*)                                        AS total_eventos,
        ROUND(AVG(magnitude), 2)                        AS mag_media_global,
        MAX(magnitude)                                  AS mag_maxima,
        SUM(CASE WHEN tsunami_warning THEN 1 ELSE 0 END) AS total_tsunamis,
        SUM(CASE WHEN risk_level = '🔴 CRITICAL'
                 THEN 1 ELSE 0 END)                     AS eventos_criticos,
        COUNT(DISTINCT geo_region)                      AS regioes_afetadas
    FROM earthquake_pipeline.gold.alerts
"""))

print("CAMADA GOLD CONCLUÍDA! ✅")