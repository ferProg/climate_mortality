"""
d25_nb_utils.py
Reusable helpers for the Diciembre25 climate-mortality EDA notebooks.
Keep this file light, dependency-free, and stable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd


def find_project_root(start: Path) -> Path:
    """
    Walk upward until we find a directory that looks like a project root.
    Heuristic: contains a 'data' folder OR a 'src' folder.
    """
    p = start.resolve()
    for _ in range(10):
        if (p / "data").exists() or (p / "src").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start.resolve()


def section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def glance(df: pd.DataFrame, label: str | None = None, n: int = 5) -> None:
    """Quick first look with optional label."""
    if label:
        print(f"\n--- {label} ---")
    print("shape:", df.shape)
    print("\ndtypes:\n", df.dtypes)
    print("\nhead:")
    display(df.head(n))  # notebook display


def missing_table(df: pd.DataFrame) -> pd.DataFrame:
    miss = df.isna().sum().sort_values(ascending=False)
    pct = (miss / len(df)).round(4)
    return pd.DataFrame({"missing": miss, "missing_pct": pct})


def duplicate_count(df: pd.DataFrame, subset: Optional[Sequence[str]] = None) -> int:
    return int(df.duplicated(subset=subset).sum())


def checks(
    df: pd.DataFrame,
    required: Sequence[str] = ("week_start", "deaths_week"),
    key: Sequence[str] = ("week_start",),
    dt: Optional[str] = "week_start",
) -> pd.DataFrame:
    """
    Flexible QA checks:
    - required: columns that must exist
    - key: subset for duplicate checks
    - dt: datetime column to coerce/check
    """
    results = []

    missing_cols = [c for c in required if c not in df.columns]
    results.append({
        "name": "required_cols_present",
        "ok": len(missing_cols) == 0,
        "detail": "missing=" + ",".join(missing_cols) if missing_cols else "ok"
    })

    if dt and dt in df.columns:
        coerced = pd.to_datetime(df[dt], errors="coerce")
        n_bad = int(coerced.isna().sum())
        results.append({
            "name": f"datetime_parse_{dt}",
            "ok": n_bad == 0,
            "detail": f"bad={n_bad}"
        })
    else:
        results.append({"name": "datetime_parse", "ok": True, "detail": "skipped"})

    if key and all(k in df.columns for k in key):
        dups = int(df.duplicated(subset=list(key)).sum())
        results.append({
            "name": "duplicates_on_key",
            "ok": dups == 0,
            "detail": f"dups={dups}"
        })
    else:
        results.append({"name": "duplicates_on_key", "ok": True, "detail": "skipped"})

    miss_cells = int(df.isna().sum().sum())
    results.append({
        "name": "total_missing_cells",
        "ok": True,
        "detail": f"missing_cells={miss_cells}"
    })

    if "deaths_week" in df.columns:
        neg = int((df["deaths_week"] < 0).sum())
        results.append({
            "name": "deaths_nonnegative",
            "ok": neg == 0,
            "detail": f"neg={neg}"
        })

    return pd.DataFrame(results)


def num_summary(
    df: pd.DataFrame,
    cols: Optional[Sequence[str]] = None,
    include: str = "number",
) -> pd.DataFrame:
    """
    Numeric summary with richer defaults.
    - If cols is None: summarizes all numeric columns.
    - If cols provided: summarizes only those that exist.
    """
    if cols is None:
        num = df.select_dtypes(include=[include])
        if num.shape[1] == 0:
            return pd.DataFrame()
        return num.describe().T

    keep = [c for c in cols if c in df.columns]
    if not keep:
        return pd.DataFrame()
    return df[keep].describe().T


def set_island_paths(root: Path, island_code: str) -> dict[str, Path]:
    """
    Standardize your project paths per island. Update once, reuse everywhere.
    """
    return {
        "MASTER_DIR": root / "data" / "master" / island_code,
        "EDA_DIR": root / "data" / "processed" / "eda_ready" / island_code,
        "REPORTS_DIR": root / "reports" / island_code,
    }


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path

def autosave_fig(
    fig_or_path,
    out_dir: Path | None = None,
    filename: str | None = None,
    dpi: int = 150,
    bbox_inches: str = "tight",
    close: bool = False,
) -> Path:
    """
    Save a matplotlib figure consistently.

    Supported call styles:
    1) autosave_fig(fig, out_dir, "plot.png")
    2) autosave_fig(out_dir / "plot.png")                # uses current active figure
    3) autosave_fig(fig, out_dir / "plot.png")
    """
    from pathlib import Path
    import matplotlib.pyplot as plt

    # Case 1: first arg is a figure-like object
    if hasattr(fig_or_path, "savefig"):
        fig = fig_or_path

        if out_dir is None:
            raise TypeError("When passing a figure, you must provide out_dir or a full filepath.")

        out_dir = Path(out_dir)

        # autosave_fig(fig, full_filepath)
        if filename is None and out_dir.suffix:
            out_fp = out_dir
            out_fp.parent.mkdir(parents=True, exist_ok=True)
        else:
            if filename is None:
                raise TypeError("filename is required when out_dir is a directory.")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_fp = out_dir / filename

    else:
        # Case 2: first arg is a full filepath, use current figure
        fig = plt.gcf()
        out_fp = Path(fig_or_path)
        out_fp.parent.mkdir(parents=True, exist_ok=True)

    fig.savefig(out_fp, dpi=dpi, bbox_inches=bbox_inches)
    print(f"Saved figure -> {out_fp}")

    if close:
        plt.close(fig)

    return out_fp


def save_table(
    df: pd.DataFrame,
    out_dir: Path,
    filename: str | None = None,
    index: bool = False,
) -> Path:
    """
    Save a dataframe as CSV consistently.

    Supported call styles:
    1) save_table(df, out_dir, "file.csv")
    2) save_table(df, out_dir / "file.csv")
    """
    out_dir = Path(out_dir)

    if filename is None:
        out_fp = out_dir
        out_fp.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_fp = out_dir / filename

    df.to_csv(out_fp, index=index)
    print(f"Saved table -> {out_fp}")
    return out_fp