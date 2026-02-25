'''
Función: Toma los avisos CAP ya parseados y construye flags/variables de exposición para análisis (ej. nivel máximo semanal, indicador “yellow week”, etc.).
Salida típica: dataset semanal con dust_tfe_level_max_week, dust_tfe_is_yellow_week y derivados.
'''
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

IN_PARSED = DATA_RAW / "aemet_alerts_cap_parsed.csv"

# Salidas (Tenerife-only)
OUT_DAILY = DATA_PROCESSED / "aemet_calima_alerts_tenerife_daily.csv"
OUT_WEEKLY = DATA_PROCESSED / "aemet_calima_alerts_tenerife_weekly.csv"


def dust_level_score(event: str) -> int:
    e = (event or "").lower()
    if "rojo" in e or "extreme" in e:
        return 4
    if "naranja" in e or "severe" in e:
        return 3
    if "amarillo" in e or "moderate" in e:
        return 2
    if "verde" in e or "minor" in e:
        return 1
    return 0


def main():
    print("[START] reading parsed CSV:", IN_PARSED)

    alerts = pd.read_csv(
        IN_PARSED,
        usecols=["identifier", "areaDesc", "event", "sent", "onset", "expires"],
        dtype={
            "identifier": "string",
            "areaDesc": "string",
            "event": "string",
            "sent": "string",
            "onset": "string",
            "expires": "string",
        },
        low_memory=False,
    )

    # Dedup ES/EN (mismo identifier)
    alerts["sent_dt"] = pd.to_datetime(alerts["sent"], errors="coerce", utc=True).dt.tz_convert(None)
    alerts = alerts.sort_values(["identifier", "sent_dt"]).drop_duplicates(subset=["identifier"], keep="last")

    # Parse datetimes
    alerts["onset_dt"] = pd.to_datetime(alerts["onset"], errors="coerce", utc=True).dt.tz_convert(None)
    alerts["expires_dt"] = pd.to_datetime(alerts["expires"], errors="coerce", utc=True).dt.tz_convert(None)

    # Dust event
    ev = alerts["event"].fillna("").str.lower()
    alerts["is_dust_event"] = (
        ev.str.contains("polvo en suspensión", na=False)
        | ev.str.contains("dust warning", na=False)
    ).astype(int)

    # Tenerife-only areas
    area_blob = alerts["areaDesc"].fillna("").str.lower()
    alerts["is_tenerife"] = (
        area_blob.str.contains("tenerife", na=False)
        | area_blob.str.contains("norte de tenerife", na=False)
        | area_blob.str.contains("área metropolitana de tenerife", na=False)
        | area_blob.str.contains("area metropolitana de tenerife", na=False)
        | area_blob.str.contains("este, sur y oeste de tenerife", na=False)
    ).astype(int)

    # Severity score
    alerts["dust_level"] = alerts["event"].apply(dust_level_score)

    # Filter: dust + Tenerife
    dust_tfe = alerts[(alerts["is_dust_event"] == 1) & (alerts["is_tenerife"] == 1)].copy()
    dust_tfe = dust_tfe[dust_tfe["onset_dt"].notna() & dust_tfe["expires_dt"].notna()].copy()

    print("[SANITY] unique alerts:", len(alerts))
    print("[SANITY] dust events:", int(alerts["is_dust_event"].sum()))
    print("[SANITY] tenerife-ish:", int(alerts["is_tenerife"].sum()))
    print("[SANITY] dust_tfe:", len(dust_tfe))
    print("[SANITY] dust_tfe areaDesc sample:", dust_tfe["areaDesc"].dropna().head(5).tolist())

    # Build full daily calendar (CAP archive start)
    STUDY_START = pd.Timestamp("2018-06-18")
    STUDY_END = pd.Timestamp("2025-12-31")
    day_index = pd.date_range(STUDY_START, STUDY_END, freq="D")

    daily = pd.DataFrame({"day": day_index})
    daily["aemet_dust_tfe_alert_day"] = 0
    daily["dust_tfe_level_max_day"] = 0
    daily["dust_tfe_lvl_ge2_day"] = (daily["dust_tfe_level_max_day"] >= 2).astype(int)
    daily["dust_tfe_lvl_ge3_day"] = (daily["dust_tfe_level_max_day"] >= 3).astype(int)


    for _, r in dust_tfe.iterrows():
        a0 = r["onset_dt"].floor("D")
        b0 = r["expires_dt"].floor("D")
        if b0 < a0:
            continue

        m = (daily["day"] >= a0) & (daily["day"] <= b0)
        if not m.any():
            continue

        daily.loc[m, "aemet_dust_tfe_alert_day"] = 1
        daily.loc[m, "dust_tfe_level_max_day"] = daily.loc[m, "dust_tfe_level_max_day"].clip(
            lower=int(r["dust_level"])
        )

    daily["week_start"] = (daily["day"] - pd.to_timedelta(daily["day"].dt.weekday, unit="D")).dt.normalize()

    weekly = (
        daily.groupby("week_start", as_index=False)
        .agg(
            aemet_dust_tfe_alert_days=("aemet_dust_tfe_alert_day", "sum"),
            dust_tfe_level_max_week=("dust_tfe_level_max_day", "max"),
            dust_tfe_days_lvl_ge2=("dust_tfe_lvl_ge2_day", "sum"),
            dust_tfe_days_lvl_ge3=("dust_tfe_lvl_ge3_day", "sum"),
            n_days=("day", "count"),
        )
    )

    weekly["aemet_dust_tfe_alert_any"] = (weekly["aemet_dust_tfe_alert_days"] > 0).astype(int)
    weekly["dust_tfe_any_lvl_ge2"] = (weekly["dust_tfe_days_lvl_ge2"] > 0).astype(int)
    weekly["dust_tfe_any_lvl_ge3"] = (weekly["dust_tfe_days_lvl_ge3"] > 0).astype(int)
    weekly["coverage"] = weekly["n_days"] / 7


    # Keep full weeks only
    weekly = weekly[weekly["n_days"] == 7].copy()

    daily.to_csv(OUT_DAILY, index=False)
    weekly.to_csv(OUT_WEEKLY, index=False)

    print("[OUT] daily:", OUT_DAILY, daily.shape)
    print("[OUT] weekly:", OUT_WEEKLY, weekly.shape)
    print("[CHECK] daily counts:\n", daily["aemet_dust_tfe_alert_day"].value_counts())
    print("[CHECK] weekly any counts:\n", weekly["aemet_dust_tfe_alert_any"].value_counts())
    print("[CHECK] weekly any>=2 counts:\n", weekly["dust_tfe_any_lvl_ge2"].value_counts())
    print("[CHECK] weekly any>=3 counts:\n", weekly["dust_tfe_any_lvl_ge3"].value_counts())
    print("[CHECK] weekly max level counts:\n", weekly["dust_tfe_level_max_week"].value_counts().sort_index())


if __name__ == "__main__":
    main()
