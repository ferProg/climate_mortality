# build_calima_proxy_regional.py
# Aggregates island-level calima proxies into a regional score
# weighted by population (2020 census)
# Output: data/processed/calima/calima_proxy_regional_2004_2025.parquet

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED = PROJECT_ROOT / "data" / "processed"

ISLANDS = {
    "tenerife":      ("tfe",  0.43),
    "gran_canaria":  ("gcan", 0.39),
    "lanzarote":     ("lzt",  0.07),
    "fuerteventura": ("ftv",  0.06),
    "la_palma":      ("lpa",  0.04),
    "gomera":        ("gom",  0.01),
}

frames = []
for island, (code, weight) in ISLANDS.items():
    path = PROCESSED / island / "calima" / f"calima_proxy_weekly_{code}_2004_2025.parquet"
    if not path.exists():
        print(f"❌ Missing: {path}")
        continue
    df = pd.read_parquet(path)[["week_start", "calima_proxy_score"]]
    df["week_start"] = pd.to_datetime(df["week_start"])
    df["weighted_score"] = df["calima_proxy_score"] * weight
    df["weight"] = weight
    df["island"] = island
    frames.append(df)
    print(f"✓ Loaded {island} ({len(df)} weeks)")

all_df = pd.concat(frames, ignore_index=True)

# Weighted mean per week
regional = (
    all_df.groupby("week_start")
    .apply(lambda x: (x["weighted_score"].sum() / x["weight"].sum()))
    .reset_index()
)
regional.columns = ["week_start", "calima_proxy_score_regional"]

# Assign regional levels
regional["calima_proxy_level_regional"] = "no_calima"
regional.loc[regional["calima_proxy_score_regional"] >= 0.25, "calima_proxy_level_regional"] = "possible"
regional.loc[regional["calima_proxy_score_regional"] >= 0.50, "calima_proxy_level_regional"] = "probable"
regional.loc[regional["calima_proxy_score_regional"] >= 0.75, "calima_proxy_level_regional"] = "intense"

# Save
out_dir = PROCESSED / "calima"
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / "calima_proxy_regional_2004_2025.parquet"
regional.to_parquet(out_path, index=False)

print(f"\nShape     : {regional.shape}")
print(f"Date range: {regional['week_start'].min().date()} → {regional['week_start'].max().date()}")
print(f"\nLevel distribution:")
print(regional["calima_proxy_level_regional"].value_counts())
print(f"\nScore stats:")
print(regional["calima_proxy_score_regional"].describe())
print(f"\n✓ Saved: {out_path}")