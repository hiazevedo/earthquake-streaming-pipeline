# Earthquake Streaming Pipeline

> Pipeline de streaming de dados sísmicos em tempo real com Auto Loader e Structured Streaming no Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-0194E2?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Sobre o projeto

Pipeline de streaming que coleta dados sísmicos em tempo real da API do **USGS Earthquake Hazards Program**, processa via **Auto Loader + Structured Streaming** e gera alertas de risco com score calculado em tempo quase-real. Os dados fluem automaticamente do Volume de arquivos JSON até tabelas Delta analíticas com deduplicação e enriquecimento geográfico.

---

## Arquitetura

```
API USGS (GeoJSON)
      │
      ▼  coleta periódica (micro-batches)
┌─────────────────┐
│  UC Volume      │  JSON files
│  /raw_json      │  Novos arquivos a cada ciclo
└────────┬────────┘
         │  Auto Loader (cloudFiles)
         ▼
┌─────────────────┐
│    BRONZE       │  Schema explícito + explode features array
│   earthquakes   │  _source_file, _ingested_at
└────────┬────────┘
         │  readStream + withWatermark
         ▼
┌─────────────────┐
│    SILVER       │  dropDuplicates(event_id)
│   earthquakes   │  magnitude_class, depth_class
└────────┬────────┘  alert_level, tsunami_warning
         │           geo_region (7 regiões)
         ▼
┌─────────────────────────────────────────┐
│                 GOLD                    │
│  alerts       │ summary │ heatmap       │
│  risk_score   │ by_region│ hourly       │
└─────────────────────────────────────────┘
```

---

## Sistema de alertas — Risk Score

```python
risk_score = (magnitude / 10.0 * 60)      # peso 60% — magnitude
           + (tsunami_warning * 30)        # peso 30% — alerta tsunami
           + (sig / 1000.0 * 10)          # peso 10% — significância USGS

# Classificação
risk_score >= 70  →  🔴 CRITICAL
risk_score >= 50  →  🟠 HIGH
risk_score >= 30  →  🟡 MEDIUM
risk_score <  30  →  🟢 LOW
```

---

## Tabelas Gold

| Tabela | Descrição |
|--------|-----------|
| `gold.alerts` | Todos os eventos com risk_score calculado |
| `gold.summary` | Agregação por geo_region × magnitude_class |
| `gold.heatmap_region` | geo_region × depth_class × magnitude_class |
| `gold.hourly_activity` | Atividade sísmica por hora do dia |

---

## Estrutura do projeto

```
earthquake-streaming-pipeline/
├── databricks.yml               # Databricks Asset Bundle — Job de ingestão (1h)
├── 00_setup.py                  # Catalog, schemas, volumes, checkpoints
├── 01_api_ingestion.py          # Coleta da API USGS → Volume JSON
├── 02_bronze_stream.py          # Auto Loader → Delta Bronze
├── 03_silver_stream.py          # Streaming Bronze → Silver + enriquecimento
├── 04_gold_alerts.py            # Risk Score + 4 tabelas Gold
├── 05_quality_checks.py         # 29 checks automatizados ✅ 100%
├── 06_dashboard.py              # Notebook de dashboard Databricks SQL
└── 07_streaming_continuous.py   # Loop de micro-batches (5 min/ciclo)
```

---

## Databricks Job

O pipeline está configurado como **Databricks Job** via Asset Bundle (`databricks.yml`), agendado para rodar a cada hora:

```
api_ingestion → bronze_stream → silver_stream → gold_alerts → quality_checks
```

Para fazer o deploy:

```bash
databricks bundle deploy
databricks bundle run earthquake_pipeline
```

---

## Quality Checks

**29/29 checks passando — 100%**

| Camada | Checks | Destaques |
|--------|--------|-----------|
| Bronze | 6 | Volume, schema, source file |
| Silver | 13 | BooleanType, 7 regiões, lat range, deduplicação |
| Gold | 10 | Consistência tsunami Gold=Silver, 24 horas distintas |

---

## Enriquecimento geográfico

```python
# 7 regiões definidas por lat/lon bounding boxes
"North America"       # lat 15–72, lon -168 a -52
"South America"       # lat -56–15, lon -82 a -34
"Europe"              # lat 35–72, lon -25 a 45
"Asia"                # lat 0–55,  lon 52 a 150
"Oceania"             # lat -50–0, lon 110 a 180
"Africa & Middle East"
"Pacific / Other"     # demais regiões oceânicas
```

---

## Insights dos dados coletados

- **North America** lidera em volume (229 eventos), mas **Pacific/Other** concentra 7 dos 8 alertas de tsunami
- **Todos os alertas de tsunami ocorrem em terremotos rasos (< 70km)** — profundidade superficial é o principal fator de risco
- **Hora 10 UTC** é o pico de atividade sísmica (27 eventos)
- **Hora 17 UTC** tem a maior magnitude média (M4.0)
- **Alaska** domina o ranking de criticidade — 7 dos 10 eventos mais perigosos

---

## Streaming contínuo (Micro-batch)

O pipeline simula streaming real com ciclos de 5 minutos, adaptado para o Databricks Free Edition:

```python
# Configuração do loop contínuo
INTERVALO_SEG = 300   # coleta a cada 5 minutos
MAX_CICLOS    = 12    # roda por 1 hora (12 × 5min)

# Cada ciclo executa:
# 1. fetch_and_save()    — coleta API USGS
# 2. run_bronze_stream() — Auto Loader processa novos JSONs
# 3. run_silver_stream() — deduplica e enriquece
# 4. run_gold()          — atualiza alertas e risk scores
```

> **Nota de produção:** Em ambiente produtivo, o job seria orquestrado via Databricks Workflows com cluster sempre ativo, eliminando a necessidade do loop manual.

---

## Stack técnica

| Tecnologia | Uso |
|------------|-----|
| **Databricks Free Edition** | Ambiente Serverless AWS |
| **Unity Catalog** | Governança + Volumes para checkpoints |
| **Auto Loader** | Ingestão incremental de JSONs (cloudFiles) |
| **Structured Streaming** | Processamento contínuo com watermark |
| **Delta Lake** | ACID transactions + time travel |
| **Databricks Asset Bundles** | Orquestração do pipeline como código |
| **USGS Earthquake API** | Fonte de dados pública (sem autenticação) |
| **Matplotlib / Seaborn** | 6 gráficos analíticos tema dark |

---

## Como reproduzir

### Pré-requisitos
- Conta no [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Acesso à internet para a API USGS (gratuita, sem autenticação)
- Databricks CLI instalado e configurado

### Passo a passo

```bash
# 1. Clone o repositório
git clone https://github.com/hiazevedo/earthquake-streaming-pipeline.git
cd earthquake-streaming-pipeline

# 2. Deploy via Asset Bundle
databricks bundle deploy

# 3. Execute o job
databricks bundle run earthquake_pipeline
```

Ou execute os notebooks manualmente na ordem:

```
00_setup.py                 # Cria catalog earthquake_pipeline
01_api_ingestion.py         # Coleta batches iniciais da API USGS
02_bronze_stream.py         # Processa JSONs com Auto Loader
03_silver_stream.py         # Enriquece e deduplica
04_gold_alerts.py           # Gera risk scores e alertas
05_quality_checks.py        # Valida pipeline (29/29)
06_dashboard.py             # Dashboard Databricks SQL
07_streaming_continuous.py  # Inicia loop contínuo (opcional)
```

### Unity Catalog

```
Catalog : earthquake_pipeline
Schemas : bronze | silver | gold | checkpoints
Volumes : /Volumes/earthquake_pipeline/bronze/raw_json
          /Volumes/earthquake_pipeline/checkpoints/streaming
```

---

## Decisões técnicas

**Por que `trigger(availableNow=True)` e não `trigger(continuous=...)`?**
O Databricks Free Edition não mantém clusters ativos indefinidamente. O `availableNow=True` processa todos os dados pendentes e encerra o stream — compatível com Serverless e sem consumir créditos desnecessariamente.

**Por que schema explícito no Auto Loader?**
A inferência automática de schema em dados GeoJSON aninhados é instável e pode falhar com novos campos da API. O schema explícito garante estabilidade e controle total sobre os tipos de dados.

**Por que watermark de 24 horas?**
A API USGS pode ter pequenos atrasos no reporte de eventos. O watermark de 24h garante que eventos tardios ainda sejam capturados antes de serem descartados pela deduplicação.

---

## Portfólio

Este projeto faz parte do [Databricks Data Engineering Portfolio](https://github.com/hiazevedo/databricks-portfolio), uma série de projetos práticos cobrindo o ciclo completo de engenharia de dados com Databricks.

| # | Projeto | Tema |
|---|---------|------|
| 1 | [fuel-price-pipeline-br](https://github.com/hiazevedo/fuel-price-pipeline-br) | Batch · Medallion · ANP |
| 2 | **earthquake-streaming-pipeline** ← você está aqui | Streaming · Auto Loader · USGS |
| 3 | [earthquake-ml-pipeline](https://github.com/hiazevedo/earthquake-ml-pipeline) | ML · MLflow · Spark ML |
| 4 | [weather-dlt-pipeline](https://github.com/hiazevedo/weather-dlt-pipeline) | DLT · Workflows · Open-Meteo |
| 5 | [weather-ml-rain-forecast](https://github.com/hiazevedo/weather-ml-rain-forecast) | ML Avançado · Previsão de Chuva |
