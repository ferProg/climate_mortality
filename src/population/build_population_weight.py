"""
build_population_weights.py
Reads ISTAC official population data (Canary Islands, 2009–2025)
and exports a clean annual parquet for use in mortality rate calculations.

Source: ISTAC Padrón Municipal — Cifras Oficiales de Población 1996–2025
File: data/raw/population/ISTAC_2009_2025.xlsx
Output: data/processed/population_canarias_2009_2025.parquet
"""

import pandas as pd
from pathlib import Path

# --- Paths ---
ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "data/raw/population/ISTAC_2009_2025.xlsx"
OUTPUT = ROOT / "data/processed/population/population_canarias_2009_2025.parquet"

# --- Load ---
pop_raw = pd.read_excel(INPUT, header=None)

# Canary Islands Total is row index 8
# Years 2025→2009, Population columns at positions 2, 6, 10... (every 4 cols)
years = list(range(2025, 2008, -1))



print(pop_raw.shape)
canarias_row = pop_raw.iloc[13]

pop_cols = [2 + i * 4 for i in range(len(years))]


population = {year: int(canarias_row.iloc[col]) 
              for year, col in zip(years, pop_cols)}

# --- Build clean DataFrame ---
df = (pd.DataFrame(list(population.items()), columns=['year', 'population'])
        .sort_values('year')
        .reset_index(drop=True))

# --- Validate ---
assert len(df) == 17, f"Expected 17 years, got {len(df)}"
assert df['population'].min() > 2_000_000, "Sanity check failed: population too low"
print(df.to_string(index=False))
print(f"\n✅ Exported → {OUTPUT}")

# --- Export ---
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUTPUT, index=False)
