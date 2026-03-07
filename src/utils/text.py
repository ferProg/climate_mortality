def safe_slug(s: str) -> str:
    return s.strip().lower().replace(" ", "_")