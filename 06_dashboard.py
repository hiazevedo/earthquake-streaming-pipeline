# Databricks notebook source
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import seaborn as sns
import pandas as pd
import numpy as np

# COMMAND ----------

plt.rcParams.update({
    "figure.facecolor":  "#0d1117",
    "axes.facecolor":    "#161b22",
    "axes.edgecolor":    "#30363d",
    "axes.labelcolor":   "#c9d1d9",
    "axes.titlecolor":   "#ffffff",
    "xtick.color":       "#8b949e",
    "ytick.color":       "#8b949e",
    "text.color":        "#c9d1d9",
    "grid.color":        "#21262d",
    "grid.linestyle":    "--",
    "grid.alpha":        0.5,
    "font.family":       "monospace",
})

COLORS = ["#58a6ff","#3fb950","#f78166","#d2a8ff",
          "#ffa657","#79c0ff","#56d364","#ff7b72"]

print("✅ Ambiente configurado")

# COMMAND ----------

# Gráfico 1: Distribuição de magnitude (histograma)

df_g1 = spark.sql("""
    SELECT magnitude, magnitude_class, tsunami_warning
    FROM earthquake_pipeline.silver.earthquakes
""").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 5))

# Histograma de magnitude
axes[0].hist(df_g1["magnitude"], bins=30, color="#58a6ff",
             edgecolor="#0d1117", linewidth=0.5, alpha=0.85)
axes[0].axvline(df_g1["magnitude"].mean(), color="#ffa657",
                linestyle="--", linewidth=2,
                label=f"Média: {df_g1['magnitude'].mean():.2f}")
axes[0].axvline(2.5, color="#f78166", linestyle=":",
                linewidth=1.5, label="Threshold M2.5")
axes[0].set_title("Distribuição de Magnitude dos Terremotos",
                  fontsize=13, fontweight="bold")
axes[0].set_xlabel("Magnitude")
axes[0].set_ylabel("Frequência")
axes[0].legend(fontsize=9, framealpha=0.2)
axes[0].grid(True, axis="y")

# Pizza por classe
class_counts = df_g1["magnitude_class"].value_counts()
order = ["Strong","Moderate","Light","Minor","Micro"]
order = [c for c in order if c in class_counts.index]
sizes  = [class_counts[c] for c in order]
colors = COLORS[:len(order)]

wedges, texts, autotexts = axes[1].pie(
    sizes, labels=order, colors=colors,
    autopct="%1.1f%%", startangle=90,
    textprops={"color": "#c9d1d9", "fontsize": 9},
    wedgeprops={"edgecolor": "#0d1117", "linewidth": 1.5}
)
for at in autotexts:
    at.set_color("#ffffff")
    at.set_fontsize(8)
axes[1].set_title("Proporção por Classe de Magnitude",
                  fontsize=13, fontweight="bold")

plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 2: Eventos por região + alertas de tsunami

df_g2 = spark.sql("""
    SELECT geo_region,
           SUM(total_eventos)   AS total,
           SUM(alertas_tsunami) AS tsunamis
    FROM earthquake_pipeline.gold.summary
    GROUP BY geo_region
    ORDER BY total DESC
""").toPandas()

x     = np.arange(len(df_g2))
width = 0.6

fig, ax1 = plt.subplots(figsize=(14, 6))
ax2 = ax1.twinx()

bars = ax1.bar(x, df_g2["total"], width,
               color="#58a6ff", edgecolor="#0d1117",
               linewidth=0.5, label="Total Eventos")
ax2.plot(x, df_g2["tsunamis"], color="#f78166",
         marker="D", linewidth=2.5, markersize=8,
         label="Alertas Tsunami", zorder=5)

for bar, val in zip(bars, df_g2["total"]):
    ax1.text(bar.get_x() + bar.get_width()/2,
             bar.get_height() + 1,
             str(val), ha="center", va="bottom",
             fontsize=9, color="#ffffff")

ax1.set_title("Total de Eventos Sísmicos e Alertas de Tsunami por Região",
              fontsize=13, fontweight="bold", pad=15)
ax1.set_xlabel("Região", fontsize=11)
ax1.set_ylabel("Total de Eventos", fontsize=11)
ax2.set_ylabel("Alertas de Tsunami", fontsize=11, color="#f78166")
ax1.set_xticks(x)
ax1.set_xticklabels(df_g2["geo_region"], rotation=20, ha="right", fontsize=9)
ax2.tick_params(axis="y", colors="#f78166")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2,
           fontsize=9, framealpha=0.2, loc="upper right")
ax1.grid(True, axis="y")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 3: Magnitude vs Profundidade (scatter)

df_g3 = spark.sql("""
    SELECT magnitude, depth_km, magnitude_class,
           tsunami_warning, geo_region
    FROM earthquake_pipeline.silver.earthquakes
    WHERE depth_km IS NOT NULL
""").toPandas()

color_map = {
    "Strong":   "#f78166",
    "Moderate": "#ffa657",
    "Light":    "#58a6ff",
    "Minor":    "#3fb950",
    "Micro":    "#8b949e"
}

fig, ax = plt.subplots(figsize=(14, 7))

for cls, grp in df_g3.groupby("magnitude_class"):
    color = color_map.get(cls, "#ffffff")
    ax.scatter(grp["depth_km"], grp["magnitude"],
               c=color, label=cls, alpha=0.65,
               s=grp["magnitude"] * 12,
               edgecolors="#0d1117", linewidth=0.3)

# Destacar tsunamis
tsunamis = df_g3[df_g3["tsunami_warning"] == True]
ax.scatter(tsunamis["depth_km"], tsunamis["magnitude"],
           c="#ff0000", marker="*", s=300, zorder=10,
           label="⚠️ Tsunami Warning", edgecolors="#ffffff",
           linewidth=0.5)

ax.axhline(5.0, color="#ffa657", linestyle="--",
           linewidth=1, alpha=0.7, label="M5.0 (Moderate threshold)")
ax.axhline(6.0, color="#f78166", linestyle="--",
           linewidth=1, alpha=0.7, label="M6.0 (Strong threshold)")
ax.axvline(70,  color="#d2a8ff", linestyle=":",
           linewidth=1, alpha=0.5, label="70km (Shallow/Intermediate)")
ax.axvline(300, color="#79c0ff", linestyle=":",
           linewidth=1, alpha=0.5, label="300km (Intermediate/Deep)")

ax.set_title("Magnitude vs Profundidade — Scatter Plot com Alertas de Tsunami",
             fontsize=13, fontweight="bold", pad=15)
ax.set_xlabel("Profundidade (km)", fontsize=11)
ax.set_ylabel("Magnitude", fontsize=11)
ax.legend(fontsize=8, framealpha=0.2,
          loc="upper right", ncol=2)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()


# COMMAND ----------

# Gráfico 4: Atividade sísmica por hora do dia

df_g4 = spark.sql("""
    SELECT event_hour,
           SUM(total_eventos) AS total,
           ROUND(AVG(mag_media), 2) AS mag_media
    FROM earthquake_pipeline.gold.hourly_activity
    GROUP BY event_hour
    ORDER BY event_hour
""").toPandas()

fig, ax1 = plt.subplots(figsize=(16, 5))
ax2 = ax1.twinx()

bar_colors = ["#f78166" if h in [6,7,8,17,18] else "#58a6ff"
              for h in df_g4["event_hour"]]

bars = ax1.bar(df_g4["event_hour"], df_g4["total"],
               color=bar_colors, edgecolor="#0d1117",
               linewidth=0.3, width=0.7)
ax2.plot(df_g4["event_hour"], df_g4["mag_media"],
         color="#ffa657", marker="o", linewidth=2,
         markersize=5, label="Magnitude Média")

ax1.set_title("Atividade Sísmica por Hora do Dia (UTC)",
              fontsize=13, fontweight="bold", pad=15)
ax1.set_xlabel("Hora (UTC)", fontsize=11)
ax1.set_ylabel("Total de Eventos", fontsize=11)
ax2.set_ylabel("Magnitude Média", fontsize=11, color="#ffa657")
ax2.tick_params(axis="y", colors="#ffa657")
ax1.set_xticks(range(0, 24))
ax1.grid(True, axis="y")

p1 = mpatches.Patch(color="#58a6ff", label="Atividade normal")
p2 = mpatches.Patch(color="#f78166", label="Pico de atividade")
ax1.legend(handles=[p1, p2], fontsize=9,
           framealpha=0.2, loc="upper left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 5: Heatmap região x profundidade

df_g5 = spark.sql("""
    SELECT geo_region, depth_class,
           SUM(total) AS total_eventos
    FROM earthquake_pipeline.gold.heatmap_region
    GROUP BY geo_region, depth_class
""").toPandas()

pivot = df_g5.pivot(index="geo_region",
                    columns="depth_class",
                    values="total_eventos").fillna(0)

order_cols = [c for c in ["Shallow","Intermediate","Deep"]
              if c in pivot.columns]
pivot = pivot[order_cols]

fig, ax = plt.subplots(figsize=(12, 6))
sns.heatmap(pivot, cmap="YlOrRd", annot=True, fmt=".0f",
            linewidths=0.5, linecolor="#0d1117", ax=ax,
            cbar_kws={"label": "Total de Eventos"})

ax.set_title("Heatmap — Eventos por Região e Profundidade",
             fontsize=13, fontweight="bold", pad=15)
ax.set_xlabel("Classe de Profundidade", fontsize=11)
ax.set_ylabel("Região Geográfica", fontsize=11)
ax.tick_params(axis="x", rotation=0)
ax.tick_params(axis="y", rotation=0)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 6: Top 10 eventos mais críticos

df_g6 = spark.sql("""
    SELECT event_id, magnitude, place,
           geo_region, depth_km, risk_score,
           risk_level, tsunami_warning
    FROM earthquake_pipeline.gold.alerts
    ORDER BY risk_score DESC
    LIMIT 10
""").toPandas()

df_g6["label"] = df_g6["place"].str[:35]
risk_colors = {
    "🔴 CRITICAL": "#f78166",
    "🟠 HIGH":     "#ffa657",
    "🟡 MEDIUM":   "#f0e68c",
    "🟢 LOW":      "#3fb950"
}
bar_colors = [risk_colors.get(r, "#8b949e")
              for r in df_g6["risk_level"]]

fig, ax = plt.subplots(figsize=(14, 7))
bars = ax.barh(df_g6["label"], df_g6["risk_score"],
               color=bar_colors, edgecolor="#0d1117",
               linewidth=0.5)

for bar, row in zip(bars, df_g6.itertuples()):
    tsunami = " 🌊" if row.tsunami_warning else ""
    ax.text(bar.get_width() + 0.3,
            bar.get_y() + bar.get_height()/2,
            f"M{row.magnitude} | {row.risk_score}{tsunami}",
            va="center", fontsize=8, color="#ffffff")

ax.set_title("Top 10 Eventos Mais Críticos — Risk Score",
             fontsize=13, fontweight="bold", pad=15)
ax.set_xlabel("Risk Score (0–100)", fontsize=11)
ax.set_xlim(0, 105)
ax.invert_yaxis()
ax.grid(True, axis="x")

patches = [mpatches.Patch(color=c, label=l)
           for l, c in risk_colors.items()]
ax.legend(handles=patches, fontsize=9,
          framealpha=0.2, loc="lower right")
plt.tight_layout()
plt.show()