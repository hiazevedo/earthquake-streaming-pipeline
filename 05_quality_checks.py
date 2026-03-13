# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

resultados = []

def check(camada, tabela, descricao, passou, detalhe=""):
    status = "✅ PASSOU" if passou else "❌ FALHOU"
    resultados.append((camada, tabela, descricao, status, detalhe))
    print(f"  {status} | {camada}.{tabela} | {descricao} {detalhe}")

print(f"{'=' * 60}")
print(f"  QUALITY CHECKS — earthquake-streaming-pipeline")
print(f"  Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'=' * 60}")

# COMMAND ----------

print("\n── BRONZE ───────────────────────────────────────────────")

df_b = spark.table("earthquake_pipeline.bronze.earthquakes")
bronze_count = df_b.count()

check("bronze", "earthquakes", "Volume mínimo (> 100 registros)",
      bronze_count > 100, f"→ {bronze_count:,} registros")

colunas_bronze = set(df_b.columns)
obrigatorias   = {"event_id", "magnitude", "place", "event_time",
                  "latitude", "longitude", "depth_km", "_source_file"}
check("bronze", "earthquakes", "Colunas obrigatórias presentes",
      obrigatorias.issubset(colunas_bronze),
      f"→ {len(colunas_bronze)} colunas")

nulos_id  = df_b.filter(F.col("event_id").isNull()).count()
check("bronze", "earthquakes", "Sem nulos em event_id",
      nulos_id == 0, f"→ {nulos_id} nulos")

nulos_mag = df_b.filter(F.col("magnitude").isNull()).count()
check("bronze", "earthquakes", "Sem nulos em magnitude",
      nulos_mag == 0, f"→ {nulos_mag} nulos")

mag_range = df_b.agg(F.min("magnitude"), F.max("magnitude")).collect()[0]
check("bronze", "earthquakes", "Magnitudes em range válido (-2 a 10)",
      mag_range[0] >= -2 and mag_range[1] <= 10,
      f"→ min: {mag_range[0]} | max: {mag_range[1]}")

source_files = df_b.select("_source_file").distinct().count()
check("bronze", "earthquakes", "Múltiplos arquivos ingeridos (> 1)",
      source_files > 1, f"→ {source_files} arquivos")

# COMMAND ----------

print("\n── SILVER ───────────────────────────────────────────────")

df_s = spark.table("earthquake_pipeline.silver.earthquakes")
silver_count = df_s.count()

check("silver", "earthquakes", "Silver ≤ Bronze (deduplicação aplicada)",
      silver_count <= bronze_count,
      f"→ Silver: {silver_count:,} | Bronze: {bronze_count:,}")

for col in ["event_id", "magnitude", "event_time", "latitude",
            "longitude", "geo_region", "magnitude_class"]:
    n = df_s.filter(F.col(col).isNull()).count()
    check("silver", "earthquakes", f"Sem nulos em '{col}'",
          n == 0, f"→ {n} nulos")

dupes = df_s.count() - df_s.dropDuplicates(["event_id"]).count()
check("silver", "earthquakes", "Sem duplicatas por event_id",
      dupes == 0, f"→ {dupes} duplicatas")

from pyspark.sql.types import BooleanType, DoubleType
schema_map = {f.name: type(f.dataType) for f in df_s.schema.fields}
check("silver", "earthquakes", "tsunami_warning é BooleanType",
      schema_map.get("tsunami_warning") == BooleanType)
check("silver", "earthquakes", "magnitude é DoubleType",
      schema_map.get("magnitude") == DoubleType)

classes_validas = {"Great","Major","Strong","Moderate","Light","Minor","Micro"}
classes_encontradas = {r[0] for r in df_s.select("magnitude_class").distinct().collect()}
check("silver", "earthquakes", "magnitude_class com valores válidos",
      classes_encontradas.issubset(classes_validas),
      f"→ {classes_encontradas}")

regioes = df_s.select("geo_region").distinct().count()
check("silver", "earthquakes", "Cobertura global (≥ 3 regiões)",
      regioes >= 3, f"→ {regioes} regiões")

lat_range = df_s.agg(F.min("latitude"), F.max("latitude")).collect()[0]
check("silver", "earthquakes", "Latitudes no range válido (-90 a 90)",
      lat_range[0] >= -90 and lat_range[1] <= 90,
      f"→ min: {lat_range[0]} | max: {lat_range[1]}")

# COMMAND ----------

print("\n── GOLD ─────────────────────────────────────────────────")

tabelas_gold = ["alerts", "summary", "heatmap_region", "hourly_activity"]
for tabela in tabelas_gold:
    try:
        n = spark.table(f"earthquake_pipeline.gold.{tabela}").count()
        check("gold", tabela, "Tabela existe e tem dados",
              n > 0, f"→ {n:,} registros")
    except Exception as e:
        check("gold", tabela, "Tabela existe e tem dados",
              False, f"→ ERRO: {str(e)}")

df_alerts = spark.table("earthquake_pipeline.gold.alerts")

risk_levels = {r[0] for r in df_alerts.select("risk_level").distinct().collect()}
check("gold", "alerts", "risk_level com valores válidos",
      len(risk_levels) > 0, f"→ {risk_levels}")

risk_range = df_alerts.agg(
    F.min("risk_score"), F.max("risk_score")).collect()[0]
check("gold", "alerts", "risk_score entre 0 e 100",
      risk_range[0] >= 0 and risk_range[1] <= 100,
      f"→ min: {risk_range[0]} | max: {risk_range[1]}")

tsunamis_gold   = df_alerts.filter(F.col("tsunami_warning") == True).count()
tsunamis_silver = df_s.filter(F.col("tsunami_warning") == True).count()
check("gold", "alerts", "Contagem de tsunamis consistente com Silver",
      tsunamis_gold == tsunamis_silver,
      f"→ Gold: {tsunamis_gold} | Silver: {tsunamis_silver}")

horas = spark.table("earthquake_pipeline.gold.hourly_activity") \
             .select("event_hour").distinct().count()
check("gold", "hourly_activity", "Cobertura de horas do dia (≥ 20 horas)",
      horas >= 20, f"→ {horas} horas distintas")

regioes_summary = spark.table("earthquake_pipeline.gold.summary") \
                       .select("geo_region").distinct().count()
check("gold", "summary", "Cobertura de regiões (≥ 3)",
      regioes_summary >= 3, f"→ {regioes_summary} regiões")

# COMMAND ----------

print(f"\n{'=' * 60}")
print("  RELATÓRIO FINAL DE QUALIDADE")
print(f"{'=' * 60}")

df_report = spark.createDataFrame(
    resultados,
    ["camada", "tabela", "check", "status", "detalhe"]
)
display(df_report)

total_checks  = len(resultados)
passou_checks = sum(1 for r in resultados if "PASSOU" in r[3])
falhou_checks = total_checks - passou_checks

print(f"""
Total de checks : {total_checks:<5}
    ✅ Passou       : {passou_checks:<5}
    ❌ Falhou       : {falhou_checks:<5}
    Taxa de sucesso : {(passou_checks/total_checks*100):.1f}%
""")