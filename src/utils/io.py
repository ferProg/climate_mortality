from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.utils.text import safe_slug

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)



def read_any(fp: Path, *, sep: str = ";") -> pd.DataFrame:
    fp = Path(fp)
    if fp.suffix.lower() == ".parquet":
        return pd.read_parquet(fp)
    if fp.suffix.lower() == ".csv":
        return pd.read_csv(fp, sep=sep)
    raise ValueError(f"Unsupported file type: {fp}")

def save_parquet(df: pd.DataFrame, fp: Path) -> Path:
    fp = Path(fp)
    ensure_dir(fp.parent)
    df.to_parquet(fp, index=False)
    return fp