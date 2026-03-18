# Databricks notebook source
# DBTITLE 1,Imports
import requests
import json
import time
from datetime import datetime, timedelta, timezone

# COMMAND ----------

# DBTITLE 1,configurações
RAW_JSON_PATH = "/Volumes/earthquake_pipeline/bronze/raw_json"

print("✅ Imports carregados")
print(f"📁 Destino dos JSONs: {RAW_JSON_PATH}")

# COMMAND ----------

# DBTITLE 1,Função de coleta da API USGS
def fetch_earthquakes(
    min_magnitude: float = 1.0,
    hours_back: int       = 24,
    limit: int            = 500
) -> dict:
    """
    Coleta eventos sísmicos da API pública USGS.

    Parâmetros:
        min_magnitude : magnitude mínima dos eventos
        hours_back    : quantas horas para trás buscar
        limit         : máximo de eventos por chamada
    """
    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)

    params = {
        "format":     "geojson",
        "starttime":  start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime":    end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": min_magnitude,
        "limit":      limit,
        "orderby":    "time"
    }

    url      = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()

print("✅ Função fetch_earthquakes definida")

# COMMAND ----------

# DBTITLE 1,Testar a API e inspecionar resposta
print("🌍 Testando conexão com a API USGS...")

data = fetch_earthquakes(min_magnitude=2.5, hours_back=24)

total_eventos = len(data["features"])
metadata      = data["metadata"]

print(f"\n📊 Resposta da API:")
print(f"   Status    : {metadata.get('status', 'N/A')}")
print(f"   Total     : {metadata.get('count', 0)} eventos")
print(f"   Gerado em : {datetime.fromtimestamp(metadata['generated']/1000, tz=timezone.utc)}")

print(f"\n🔍 Exemplo do primeiro evento:")
if total_eventos > 0:
    evento    = data["features"][0]
    props     = evento["properties"]
    coords    = evento["geometry"]["coordinates"]
    print(f"   ID         : {evento['id']}")
    print(f"   Magnitude  : {props['mag']}")
    print(f"   Local      : {props['place']}")
    print(f"   Profund.   : {coords[2]} km")
    print(f"   Tsunami    : {'⚠️ SIM' if props['tsunami'] else 'Não'}")
    print(f"   Alerta     : {props.get('alert', 'N/A')}")
    print(f"   Timestamp  : {datetime.fromtimestamp(props['time']/1000, tz=timezone.utc)}")

# COMMAND ----------

# DBTITLE 1,Função para salvar JSON no Volume
def save_batch_to_volume(data: dict, path: str) -> str:
    """
    Salva um batch de eventos como arquivo JSON no Volume.
    Nome do arquivo inclui timestamp para garantir unicidade.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = f"earthquakes_{timestamp}.json"
    filepath  = f"{path}/{filename}"

    # Extrair apenas os features (eventos) e adicionar metadados de coleta
    batch = {
        "collected_at" : datetime.now(timezone.utc).isoformat(),
        "total_events" : len(data["features"]),
        "source"       : "USGS Earthquake Hazards Program",
        "features"     : data["features"]
    }

    # Salvar via dbutils
    json_str = json.dumps(batch, ensure_ascii=False)
    dbutils.fs.put(filepath, json_str, overwrite=True)

    return filename

print("✅ Função save_batch_to_volume definida")

# COMMAND ----------

# DBTITLE 1,Coletar múltiplos batches simulando streaming
# Coleta dados de diferentes janelas de tempo para simular histórico

print("🚀 Iniciando coleta de múltiplos batches...\n")

batches_config = [
    {"min_magnitude": 1.0, "hours_back": 6,   "label": "Últimas 6h  — M≥1.0"},
    {"min_magnitude": 1.0, "hours_back": 12,  "label": "Últimas 12h — M≥1.0"},
    {"min_magnitude": 1.0, "hours_back": 24,  "label": "Últimas 24h — M≥1.0"},
    {"min_magnitude": 2.5, "hours_back": 72,  "label": "Últimos 3d  — M≥2.5"},
    {"min_magnitude": 4.0, "hours_back": 168, "label": "Última semana — M≥4.0"},
]

total_coletado = 0
arquivos_salvos = []

for config in batches_config:
    try:
        print(f"📡 Coletando: {config['label']}...")
        data     = fetch_earthquakes(
            min_magnitude = config["min_magnitude"],
            hours_back    = config["hours_back"]
        )
        eventos  = len(data["features"])
        filename = save_batch_to_volume(data, RAW_JSON_PATH)

        total_coletado += eventos
        arquivos_salvos.append(filename)
        print(f"   ✅ {eventos} eventos → {filename}")

        time.sleep(2)  # respeitar rate limit da API

    except Exception as e:
        print(f"   ❌ Erro: {str(e)}")

print(f""" COLETA CONCLUÍDA! ✅
Batches coletados : {len(arquivos_salvos):<5}
Total de eventos  : {total_coletado:<5}
""")

# COMMAND ----------

# DBTITLE 1,Validar arquivos salvos no Volume
print("📁 Arquivos salvos no Volume:\n")

files = dbutils.fs.ls(RAW_JSON_PATH)
total_bytes = 0

for f in files:
    total_bytes += f.size
    print(f"   📄 {f.name:<45} {f.size:>10,} bytes")

print(f"\n   Total: {len(files)} arquivos | {total_bytes:,} bytes")
print("\n✅ Volume pronto para o Auto Loader!")
print("   Próximo passo: 02_bronze_stream.py")