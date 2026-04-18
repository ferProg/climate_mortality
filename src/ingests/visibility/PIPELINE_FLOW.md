# Climate Mortality Pipeline — Flujo Completo



## Visibility Pipeline — Desglose (Block 1 + 3)

### 3 Scripts Principales

#### Script 1: **ingest_visibility_raw.py**
```
Fuente: [AEMET API / Local files]
  ↓
Descarga + Parse CSV
  ├─ Columnas: fecha, isla, visibilidad (km), estacion_id
  ├─ Validación básica: tipos de dato, rangos (0-100+ km)
  └─ Handleo errores: URLs muertas, datos faltantes
  ↓
Output: raw_visibility.csv
```

#### Script 2: **visibility_cleaning.py**
```
Input: raw_visibility.csv
  ↓
Transformaciones:
  ├─ Convertir tipos: fecha → datetime, visibilidad → float
  ├─ Validar rangos: visibilidad ∈ [0, 150]
  ├─ Imputar missings: forward fill o media por isla
  ├─ Detectar outliers: >3σ respecto media móvil
  └─ Crear flag: "calima_proxy" (visibility < 5km = TRUE)
  ↓
Output: clean_visibility.csv
```

#### Script 3: **visibility_calima_proxy.py**
```
Input: clean_visibility.csv
  ↓
Lógica de proxy:
  ├─ Calima directa: visibility < 5 km → calima_event = 1
  ├─ Pre-calima: visibility 5-10 km → calima_warning = 1
  ├─ Normal: visibility > 10 km → calima = 0
  └─ Crear columnas adicionales:
     ├─ consecutive_calima_days
     ├─ visibility_ma7 (media móvil 7 días)
     └─ visibility_change_rate (variación día a día)
  ↓
Output: visibility_features.csv → Input a MERGE (Step 3)
```

---

## Cómo Encaja Visibility en el Pipeline

```
Step 1: INGESTION
├─ raw_aemet.csv (temperatura, presión, humedad)
├─ raw_noaa.csv (contaminación: PM2.5, O3)
├─ raw_visibility.csv ← Script 1: ingest_visibility_raw
└─ raw_deaths.csv (muertes por día + isla)

Step 2: CLEANING
├─ clean_aemet.csv
├─ clean_noaa.csv
├─ clean_visibility.csv ← Script 2: visibility_cleaning
└─ clean_deaths.csv

Step 3: MERGE + FEATURES
├─ merged_weather.csv (aemet + noaa + visibility)
│  └─ Visibility features: Script 3: visibility_calima_proxy
└─ clean_deaths.csv (sin merge aún)

Step 5: QA VALIDATION
├─ Verifica merged_weather:
│  ├─ Missings en visibility
│  ├─ Correlación visibility ↔ deaths (debe ser negativa)
│  ├─ Outliers en calima_proxy
│  └─ Temporal continuity (no saltos de fechas)

Step 6: FINAL MERGE
├─ merged_weather (con visibility features)
└─ clean_deaths
   → final_dataset.csv
```

---

## Diferencias: Visibility vs. AEMET Station Data

| Aspecto | AEMET (Temperatura, Presión) | Visibility |
|---|---|---|
| **Fuente** | AEMET API (nacional) | AEMET API (mismo) |
| **Frecuencia** | Diaria (00:00 UTC) | Diaria (00:00 UTC) |
| **Cobertura** | 1 estación por isla | Múltiples estaciones (según isla) |
| **Valores típicos** | T: -10 a +50°C, P: 980-1050 hPa | Visibility: 0.1 a 100+ km |
| **Transformación** | Normalización, rolling avg | Calima proxy (binaria), consecutive days |
| **Feature engineering** | Media móvil 7d, lag 1d, lag 7d | Mismo + calima_event flag + visibility_change_rate |
| **Conexión causal** | AEMET → (posiblemente) → Muertes | Visibility (calima proxy) → **FUERTE** → Muertes |

---

## Diagrama: run_island_pipeline.py (Block 3)

```
run_island_pipeline(island_name, start_date, end_date, force_refresh=False)
  │
  ├─ [1] Ejecuta: ingest_visibility_raw(island)
  │   └─ Output: raw_visibility.csv
  │
  ├─ [2] Ejecuta: visibility_cleaning(raw_visibility)
  │   └─ Output: clean_visibility.csv
  │
  ├─ [3] Ejecuta: visibility_calima_proxy(clean_visibility)
  │   └─ Output: visibility_features.csv
  │
  ├─ [4] Ejecuta: [AEMET + NOAA ya descargados]
  │   └─ Merge: aemet + noaa + visibility → merged_weather.csv
  │
  ├─ [5] Ejecuta: qa_validation(merged_weather, clean_deaths)
  │   └─ Output: qa_report.json
  │
  ├─ [6] Ejecuta: final_merge(merged_weather, clean_deaths, qa_report)
  │   └─ Output: final_dataset.csv + final_dataset.parquet
  │
  └─ [7] Retorna: paths a todos los outputs + qa_report
```

**Parámetros:**
- `island_name`: "Tenerife", "Gran Canaria", etc.
- `start_date`, `end_date`: YYYY-MM-DD
- `force_refresh`: Si True → redescarga todo. Si False → usa cache.

---
## step1_load_isd_airports.py

NOAA ISD yearly files (1 por año)
  ↓
Extrae: dt_utc, visibility, air_temperature, dew_point
        + quality codes (para validar)
  ↓
Output: data/interim/visibility/step1_yearly/
         └─ isd_manifest_YYYY_YYYY.parquet
         
Si falta año → None
---
## step2_filter_13utc_and_build_daily.py

Input: isd_manifest_YYYY_YYYY.parquet (from step1)
  ↓
Filtros:
  - dt_utc entre 12:00 ± 90 min (config: TARGET_HOUR_UTC=12, TIME_TOL_MIN=90)
  - vis_qc != 9 (datos inválidos)
  - vis_m >= 0 (guardrail físico)
  ↓
Transformaciones:
  - Calcula RH% (desde temp + dewpoint)
  - Marca vis_is_capped_10km (si vis_m = 9999 o 10000)
  - Por cada station/date → guarda **closest obs to 12 UTC**
  ↓
Output: isd_daily_{start_date}_{end_date}.parquet
         (1 fila per station/date)
         
---
## step3_build_dust_day_flag_island.py

Input: isd_daily_{start_date}_{end_date}.parquet (from step2)
  ↓
Lógica de detección (per station/day):
  
  CONFIRMED (calima casi segura):
    - vis < 10 km  (strong dust)
    - O: vis 10-20 km + RH < 70%  (moderate dust + dry air)
  
  POSSIBLE (calima posible):
    - vis falta + RH < 30%  (aire muy seco)
  ↓
Agregación por date (toda la isla):
  - confirmed_airports: N estaciones con confirmed flag
  - possible_airports: N estaciones con possible flag
  - airports_obs: total estaciones con datos
  - vis_min_m: visibilidad mínima
  - rh_min_pct: humedad mínima
  ↓
Output: data/interim/visibility/step3_daily/
         low_vis_proxy_daily_{island}.parquet
         (1 fila per date, isla entera)

---
## step4_aggregate_weekly_island.py

Input: low_vis_proxy_daily_{island}.parquet (from step3)
  ↓
Agregación por semana (Mon week_start UTC):
  
  COUNTS (suma):
    - low_vis_confirmed_days_week: N días confirmed en la semana
    - low_vis_possible_days_week: N días possible en la semana
    - low_vis_any_days_week: N días any en la semana
  
  FLAGS (max, i.e., "ocurrió al menos 1 vez?"):
    - low_vis_confirmed_any_week: 1 si ≥1 día confirmed
    - low_vis_possible_any_week: 1 si ≥1 día possible
    - low_vis_any_week: 1 si ≥1 día any
  
  EXTREMOS (min/max):
    - confirmed_airports_max_week: máximo N estaciones confirmed
    - vis_min_m_week: visibilidad mínima semanal
    - rh_min_pct_week: humedad mínima semanal
  ↓
Output: data/interim/visibility/step4_weekly/
         visibility_weekly_{island}.parquet
         (1 fila per week_start, per island)
---
## Por qué 4 scripts en lugar de 1:

Reutilización: Step1 (descarga NOAA) es caro (network). Si falla step3, no redescarga.
Debugging: Si step3 produce flags raros, sabes dónde mirar (step3, no 4000 líneas).
Escalabilidad: Cada step es independiente. Puedes paralelizar islands sin conflictos.
Mantenimiento: Cambiar threshold de calima (step3) no afecta lógica de descarga (step1).
Caching: Puedes cachear step1/step2 outputs y solo rerun step3/step4 si cambias reglas.
