import pandas as pd

def validate_weekly_dataset(
    df: pd.DataFrame,
    required_cols: set[str] | None = None,
    week_col: str = "week_start",
    make_copy: bool = True,
) -> pd.DataFrame:
    """
    Validate a weekly dataset:
    - required columns exist
    - week_start is datetime
    - no duplicate weeks
    Returns a validated (optionally copied) DataFrame.
    """
    if required_cols is None:
        required_cols = {week_col, "deaths_week"}

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {sorted(missing)}")

    out = df.copy() if make_copy else df

    # Ensure datetime
    try:
        out[week_col] = pd.to_datetime(out[week_col], errors="raise")
    except Exception as e:
        raise ValueError(f"Column '{week_col}' could not be parsed as datetime") from e

    # Duplicates
    if out[week_col].duplicated().any():
        dups = out.loc[out[week_col].duplicated(), week_col].sort_values().unique()
        raise ValueError(f"Duplicate weeks found in '{week_col}': {list(dups)[:10]}")

    return out