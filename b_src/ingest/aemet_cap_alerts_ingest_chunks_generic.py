# b_src/ingest/aemet_cap_alerts_ingest_chunks_generic.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import argparse
import os
import time
import re
import io
import tarfile
import gzip
import hashlib
import requests
import pandas as pd
import xml.etree.ElementTree as ET


# -------------------------
# Project paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
B_DATA = PROJECT_ROOT / "b_data"
B_LOGS = PROJECT_ROOT / "b_logs"

BASE = "https://opendata.aemet.es/opendata/api"


# -------------------------
# Island suffix + area keywords
# -------------------------
ISLAND_SUFFIX = {
    "tenerife": "tfe",
    "gran_canaria": "gc",
    "grancanaria": "gc",
    "lanzarote": "lz",
    "fuerteventura": "fvt",
    "la_palma": "lp",
    "lapalma": "lp",
    "la_gomera": "lg",
    "lagomera": "lg",
    "el_hierro": "eh",
    "elhierro": "eh",
}

DEFAULT_AREA_KEYWORDS = {
    # These are deliberately “loose” because CAP areaDesc can vary a lot.
    # You can override with --area-keywords if needed.
    "tenerife": ["tenerife", "tfe", "santa cruz", "sta cruz", "la laguna"],
    "gran_canaria": ["gran canaria", "las palmas", "gc"],
    "lanzarote": ["lanzarote", "arrecife"],
    "fuerteventura": ["fuerteventura", "puerto del rosario"],
    "la_palma": ["la palma", "santa cruz de la palma"],
    "la_gomera": ["la gomera", "san sebastián", "san sebastian"],
    "el_hierro": ["el hierro", "valverde"],
}


# -------------------------
# Keyword detection
# -------------------------
DUST_KEYWORDS = [
    "polvo en suspensión",
    "polvo en suspension",
    "calima",
    "intrusión sahar",
    "intrusion sahar",
    "intrusión sahariana",
    "intrusion sahariana",
    "sahar",
    "dust",
]

HEAT_KEYWORDS = [
    "altas temperaturas",
    "temperaturas máximas",
    "temperaturas maximas",
    "temperatura máxima",
    "temperatura maxima",
    "ola de calor",
    "heat warning",
    "high temperature",
]


# -------------------------
# CLI
# -------------------------
def safe_slug(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AEMET CAP archive ingest with chunk saving (generic by island).")
    p.add_argument("--island", required=True, help="e.g. tenerife, gran_canaria, lanzarote ...")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--step-days", type=int, default=7, help="Query window (days). Default 7")
    p.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between calls. Default 0.2")
    p.add_argument("--resume", action="store_true", help="Skip chunks already saved to disk")
    p.add_argument("--out-format", choices=["parquet", "csv"], default="parquet")
    p.add_argument("--also-csv", action="store_true", help="If out-format=parquet, also write CSV copies")
    p.add_argument("--area-keywords", nargs="*", default=None, help="Override areaDesc keywords for this island")
    p.add_argument("--debug", action="store_true", help="More verbose logging")
    return p.parse_args()


# -------------------------
# Logging
# -------------------------
def log(msg: str, log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# -------------------------
# HTTP retry + safe JSON
# -------------------------
def fetch_response_retry(
    url: str,
    api_key: str | None = None,
    max_tries: int = 10,
    base_sleep: float = 2.0,
    accept_json: bool = False,
):
    """
    /opendata/api/... -> uses api_key
    /opendata/sh/...  -> no api_key
    Retries:
      - HTTP 429 and 5xx
      - transient network errors
    """
    use_key = (api_key is not None) and ("/opendata/api/" in url)

    headers = {"user-agent": "Mozilla/5.0"}
    if accept_json:
        headers["accept"] = "application/json"
    if use_key:
        headers["api_key"] = api_key

    for i in range(max_tries):
        try:
            r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(base_sleep * (2 ** i), 120))
                continue

            if r.status_code >= 400:
                r.raise_for_status()

            return r

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError):
            time.sleep(min(base_sleep * (2 ** i), 120))
            continue

    raise RuntimeError(f"Too many retries for {url}")


def safe_json_response(r: requests.Response) -> dict:
    """
    Some AEMET endpoints occasionally return non-JSON (HTML) with HTTP 200.
    We treat that as a transient failure with a useful snippet.
    """
    try:
        return r.json()
    except Exception:
        ct = (r.headers.get("Content-Type") or "").lower()
        snippet = (r.text or "")[:300].replace("\n", "\\n")
        raise RuntimeError(f"Non-JSON response: status={r.status_code} content-type={ct} snippet={snippet}")


# -------------------------
# XML parsing (robust namespaces)
# -------------------------
def parse_cap_xml(xml_text: str) -> list[dict]:
    def strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    def find_first_text(elem: ET.Element, local_name: str) -> str:
        for ch in elem.iter():
            if strip_ns(ch.tag) == local_name:
                return (ch.text or "").strip()
        return ""

    def find_all(elem: ET.Element, local_name: str) -> list[ET.Element]:
        return [ch for ch in elem.iter() if strip_ns(ch.tag) == local_name]

    rows: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return rows

    alert_fields = {
        "identifier": find_first_text(root, "identifier"),
        "sender": find_first_text(root, "sender"),
        "sent": find_first_text(root, "sent"),
        "status": find_first_text(root, "status"),
        "msgType": find_first_text(root, "msgType"),
        "scope": find_first_text(root, "scope"),
    }

    infos = find_all(root, "info")
    if not infos:
        rows.append({**alert_fields,
                    "event": "", "urgency": "", "severity": "", "certainty": "",
                    "onset": "", "expires": "", "headline": "", "description": "", "areaDesc": ""})
        return rows

    for info in infos:
        area_descs = []
        for area in find_all(info, "area"):
            ad = find_first_text(area, "areaDesc")
            if ad:
                area_descs.append(ad)

        rows.append({
            **alert_fields,
            "event": find_first_text(info, "event"),
            "urgency": find_first_text(info, "urgency"),
            "severity": find_first_text(info, "severity"),
            "certainty": find_first_text(info, "certainty"),
            "onset": find_first_text(info, "onset"),
            "expires": find_first_text(info, "expires"),
            "headline": find_first_text(info, "headline"),
            "description": find_first_text(info, "description"),
            "areaDesc": " | ".join(area_descs),
        })

    return rows


def _bytes_to_text(b: bytes) -> str | None:
    for enc in ("utf-8", "iso-8859-15", "latin-1"):
        try:
            return b.decode(enc)
        except Exception:
            pass
    return None


def _maybe_gunzip(b: bytes) -> bytes:
    if len(b) >= 2 and b[:2] == b"\x1f\x8b":
        return gzip.decompress(b)
    return b


def _clean_cap_xml_text(txt: str) -> str | None:
    """
    Extract only the CAP XML document from a buffer that may include TAR padding/junk.
    """
    if not txt:
        return None
    lo = txt.lower()
    start = lo.find("<?xml")
    if start == -1:
        start = lo.find("<alert")
    if start == -1:
        return None
    end = lo.rfind("</alert>")
    if end == -1:
        return None
    end += len("</alert>")
    return txt[start:end]


def parse_cap_from_tar_payload(payload: bytes, debug: bool = False) -> list[dict]:
    """
    OUTER TAR (from /sh/...) contains members (often *.gz)
    Each member -> maybe gunzip -> INNER TAR
    Inner TAR contains XML CAP files.
    """
    rows: list[dict] = []
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as outer:
        members = [m for m in outer.getmembers() if m.isfile()]
        if debug:
            print(f"DEBUG outer tar members: {len(members)}")
            if members:
                print("DEBUG first outer member:", members[0].name)

        for m in members:
            f = outer.extractfile(m)
            if f is None:
                continue
            b = f.read()
            if not b:
                continue

            inner_blob = _maybe_gunzip(b)

            # Try INNER TAR
            try:
                with tarfile.open(fileobj=io.BytesIO(inner_blob), mode="r:*") as inner:
                    inner_members = [im for im in inner.getmembers() if im.isfile()]
                    for im in inner_members:
                        g = inner.extractfile(im)
                        if g is None:
                            continue
                        xml_bytes = g.read()
                        if not xml_bytes:
                            continue

                        txt = _bytes_to_text(xml_bytes)
                        if txt is None:
                            continue
                        txt2 = _clean_cap_xml_text(txt)
                        if not txt2:
                            continue

                        parsed = parse_cap_xml(txt2)
                        if not parsed:
                            continue

                        for r in parsed:
                            r["cap_file"] = f"{m.name}::{im.name}"
                        rows.extend(parsed)

            except tarfile.ReadError:
                # Not an inner TAR: maybe inner_blob is XML directly
                txt = _bytes_to_text(inner_blob)
                if txt is None:
                    continue
                txt2 = _clean_cap_xml_text(txt)
                if not txt2:
                    continue
                parsed = parse_cap_xml(txt2)
                if not parsed:
                    continue
                for r in parsed:
                    r["cap_file"] = m.name
                rows.extend(parsed)

    return rows


# -------------------------
# Ranges
# -------------------------
def iter_ranges(start: datetime, end: datetime, step_days: int):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=step_days) - timedelta(seconds=1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(seconds=1)


def chunk_hash(fechaini: str, fechafin: str) -> str:
    return hashlib.md5(f"{fechaini}|{fechafin}".encode("utf-8")).hexdigest()[:12]


def write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, encoding="utf-8")


# -------------------------
# Level scoring (dust & heat)
# -------------------------
def _severity_score(severity: str) -> int:
    s = (severity or "").strip().lower()
    if s == "extreme":
        return 4
    if s == "severe":
        return 3
    if s == "moderate":
        return 2
    if s == "minor":
        return 1
    return 0


def _color_score_from_text(text: str) -> int:
    t = (text or "").lower()
    if "rojo" in t or " red " in f" {t} ":
        return 4
    if "naranja" in t or " orange " in f" {t} ":
        return 3
    if "amarillo" in t or " yellow " in f" {t} ":
        return 2
    if "verde" in t or " green " in f" {t} ":
        return 1
    return 0


def build_daily_weekly_flags(alerts: pd.DataFrame, study_start: pd.Timestamp, study_end: pd.Timestamp,
                            area_keywords: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    alerts = alerts.copy()

    # Datetimes
    alerts["onset_dt"] = pd.to_datetime(alerts.get("onset"), errors="coerce", utc=True).dt.tz_convert(None)
    alerts["expires_dt"] = pd.to_datetime(alerts.get("expires"), errors="coerce", utc=True).dt.tz_convert(None)

    # Dedup by identifier (keep latest sent)
    if "identifier" in alerts.columns:
        alerts["identifier"] = alerts["identifier"].astype(str)

    if "sent" in alerts.columns and "identifier" in alerts.columns:
        alerts["sent_dt"] = pd.to_datetime(alerts["sent"], errors="coerce", utc=True).dt.tz_convert(None)
        alerts = alerts.sort_values(["identifier", "sent_dt"]).drop_duplicates(subset=["identifier"], keep="last")
        alerts = alerts.drop(columns=["sent_dt"], errors="ignore")
    elif "identifier" in alerts.columns:
        alerts = alerts.drop_duplicates(subset=["identifier"], keep="last")

    # Area match
    area_blob = alerts.get("areaDesc", "").fillna("").astype(str).str.lower()
    area_keywords = [k.lower() for k in (area_keywords or [])]
    area_re = re.compile("|".join(map(re.escape, area_keywords))) if area_keywords else None
    alerts["is_area_match"] = area_blob.str.contains(area_re, na=False).astype(int) if area_re else 0

    # Combined text
    blob = (
        alerts.get("event", "").fillna("").astype(str) + " " +
        alerts.get("headline", "").fillna("").astype(str) + " " +
        alerts.get("description", "").fillna("").astype(str)
    ).str.lower()

    dust_re = re.compile("|".join(map(re.escape, [k.lower() for k in DUST_KEYWORDS])))
    heat_re = re.compile("|".join(map(re.escape, [k.lower() for k in HEAT_KEYWORDS])))

    alerts["is_dust_event"] = blob.str.contains(dust_re, na=False).astype(int)
    alerts["is_heat_event"] = blob.str.contains(heat_re, na=False).astype(int)

    sev_score = alerts.get("severity", "").fillna("").astype(str).apply(_severity_score)
    text_score = blob.apply(_color_score_from_text)
    alerts["level_score"] = pd.concat([sev_score, text_score], axis=1).max(axis=1).fillna(0).astype(int)

    base = alerts[(alerts["is_area_match"] == 1)].copy()
    base = base[base["onset_dt"].notna() & base["expires_dt"].notna()].copy()

    dust = base[base["is_dust_event"] == 1].copy()
    heat = base[base["is_heat_event"] == 1].copy()

    day_index = pd.date_range(study_start, study_end, freq="D")
    daily = pd.DataFrame({"day": day_index})

    daily["aemet_dust_alert_day"] = 0
    daily["dust_level_max_day"] = 0
    daily["aemet_heat_alert_day"] = 0
    daily["heat_level_max_day"] = 0

    def apply_events(df_events: pd.DataFrame, flag_col: str, level_col: str):
        if df_events.empty:
            return
        for _, r in df_events.iterrows():
            a0 = r["onset_dt"].floor("D")
            b0 = r["expires_dt"].floor("D")
            m = (daily["day"] >= a0) & (daily["day"] <= b0)
            daily.loc[m, flag_col] = 1
            daily.loc[m, level_col] = daily.loc[m, level_col].clip(lower=int(r["level_score"]))

    apply_events(dust, "aemet_dust_alert_day", "dust_level_max_day")
    apply_events(heat, "aemet_heat_alert_day", "heat_level_max_day")

    # ISO week start Monday
    daily["week_start"] = (daily["day"] - pd.to_timedelta(daily["day"].dt.weekday, unit="D")).dt.normalize()

    weekly = daily.groupby("week_start", as_index=False).agg(
        n_days=("day", "count"),
        aemet_dust_alert_days=("aemet_dust_alert_day", "sum"),
        dust_level_max_week=("dust_level_max_day", "max"),
        aemet_heat_alert_days=("aemet_heat_alert_day", "sum"),
        heat_level_max_week=("heat_level_max_day", "max"),
    )
    weekly["coverage"] = weekly["n_days"] / 7.0

    weekly["aemet_dust_alert_any"] = (weekly["aemet_dust_alert_days"] > 0).astype(int)
    weekly["dust_yellow_plus_week"] = (weekly["dust_level_max_week"] >= 2).astype(int)

    weekly["aemet_heat_alert_any"] = (weekly["aemet_heat_alert_days"] > 0).astype(int)
    weekly["heat_yellow_plus_week"] = (weekly["heat_level_max_week"] >= 2).astype(int)

    weekly = weekly[weekly["n_days"] == 7].copy()

    return alerts, daily, weekly


# -------------------------
# Main
# -------------------------
def main():
    args = parse_args()

    api_key = os.environ.get("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError("Missing env var AEMET_API_KEY. PowerShell: $env:AEMET_API_KEY='...'; then run.")

    island_slug = safe_slug(args.island)
    suffix = ISLAND_SUFFIX.get(island_slug)
    if not suffix:
        raise ValueError(f"island '{island_slug}' not in ISLAND_SUFFIX map. Add it.")

    # output dirs
    raw_dir = B_DATA / "raw" / island_slug
    proc_dir = B_DATA / "processed" / island_slug
    chunk_dir = raw_dir / "cap_chunks" / suffix
    chunk_dir.mkdir(parents=True, exist_ok=True)

    range_tag = f"{args.start}_{args.end}"
    log_file = (B_LOGS / island_slug) / f"aemet_cap_chunks_{suffix}_{range_tag}.log"

    parsed_out = raw_dir / f"aemet_alerts_cap_parsed_{suffix}_{range_tag}.{ 'parquet' if args.out_format=='parquet' else 'csv'}"
    daily_out = proc_dir / f"aemet_alerts_flags_daily_{suffix}_{range_tag}.{ 'parquet' if args.out_format=='parquet' else 'csv'}"
    weekly_out = proc_dir / f"aemet_alerts_flags_weekly_{suffix}_{range_tag}.{ 'parquet' if args.out_format=='parquet' else 'csv'}"

    # area keywords
    if args.area_keywords:
        area_keywords = args.area_keywords
    else:
        area_keywords = DEFAULT_AREA_KEYWORDS.get(island_slug, [island_slug, "canarias"])

    start_dt = datetime.fromisoformat(args.start)
    end_dt = datetime.fromisoformat(args.end) + timedelta(hours=23, minutes=59, seconds=59)

    log(f"START island={island_slug} suffix={suffix} range={args.start}..{args.end} step_days={args.step_days}", log_file)
    log(f"chunk_dir={chunk_dir}", log_file)
    log(f"area_keywords={area_keywords}", log_file)
    log(f"out_format={args.out_format} also_csv={args.also_csv} resume={args.resume}", log_file)

    calls = 0
    saved_chunks = 0
    skipped_chunks = 0
    no_data_chunks = 0
    non_json_meta = 0

    for a, b in iter_ranges(start_dt, end_dt, step_days=args.step_days):
        fechaini = a.strftime("%Y-%m-%dT%H:%M:%SUTC")
        fechafin = b.strftime("%Y-%m-%dT%H:%M:%SUTC")
        key = chunk_hash(fechaini, fechafin)

        chunk_path = chunk_dir / f"cap_{suffix}_{range_tag}_{key}.{ 'parquet' if args.out_format=='parquet' else 'csv'}"

        if args.resume and chunk_path.exists():
            skipped_chunks += 1
            continue

        api_url = f"{BASE}/avisos_cap/archivo/fechaini/{fechaini}/fechafin/{fechafin}"

        # meta fetch (must be JSON)
        try:
            resp = fetch_response_retry(api_url, api_key=api_key, accept_json=True)
            meta = safe_json_response(resp)
        except Exception as e:
            non_json_meta += 1
            log(f"[META NON-JSON] {fechaini}->{fechafin} err={e}", log_file)
            time.sleep(args.sleep)
            continue

        calls += 1

        if isinstance(meta, dict) and meta.get("estado") == 404:
            no_data_chunks += 1
            if args.debug:
                log(f"[{calls}] {fechaini}->{fechafin}: no data", log_file)
            continue

        datos_url = meta.get("datos") if isinstance(meta, dict) else None
        if not datos_url:
            log(f"[{calls}] unexpected meta (no datos). keys={list(meta.keys()) if isinstance(meta, dict) else type(meta)}", log_file)
            continue

        sh_resp = fetch_response_retry(datos_url, api_key=None, accept_json=False)
        payload = sh_resp.content

        parsed_rows = parse_cap_from_tar_payload(payload, debug=args.debug)

        if not parsed_rows:
            if args.debug:
                ctype = (sh_resp.headers.get("Content-Type") or "").lower()
                log(f"[{calls}] parsed_rows=0 ctype={ctype} bytes={len(payload)}", log_file)
            time.sleep(args.sleep)
            continue

        df_chunk = pd.DataFrame(parsed_rows)
        df_chunk["sh_url"] = datos_url
        df_chunk["range_start"] = fechaini
        df_chunk["range_end"] = fechafin
        df_chunk["chunk_key"] = key

        if "identifier" in df_chunk.columns:
            df_chunk["identifier"] = df_chunk["identifier"].astype(str)
            df_chunk = df_chunk.drop_duplicates(subset=["identifier"], keep="last")

        write_df(df_chunk, chunk_path, args.out_format)
        if args.also_csv and args.out_format == "parquet":
            df_chunk.to_csv(chunk_path.with_suffix(".csv"), index=False, encoding="utf-8")

        saved_chunks += 1
        log(f"[{calls}] saved chunk rows={len(df_chunk)} -> {chunk_path.name}", log_file)

        time.sleep(args.sleep)

    log(f"DOWNLOAD DONE calls={calls} saved_chunks={saved_chunks} skipped_chunks={skipped_chunks} "
        f"no_data_chunks={no_data_chunks} meta_non_json={non_json_meta}", log_file)

    # consolidate only this run's range tag
    chunk_files = sorted(chunk_dir.glob(f"cap_{suffix}_{range_tag}_*.{ 'parquet' if args.out_format=='parquet' else 'csv'}"))
    if not chunk_files:
        log("No chunk files found for consolidation. Exiting.", log_file)
        return

    log(f"CONSOLIDATE {len(chunk_files)} chunk file(s)", log_file)

    parts = []
    for p in chunk_files:
        try:
            parts.append(pd.read_parquet(p) if args.out_format == "parquet" else pd.read_csv(p))
        except Exception as e:
            log(f"[WARN] failed reading chunk {p.name}: {e}", log_file)

    if not parts:
        log("No readable chunks. Exiting.", log_file)
        return

    alerts = pd.concat(parts, ignore_index=True)

    # global dedup (identifier + latest sent)
    if "identifier" in alerts.columns:
        alerts["identifier"] = alerts["identifier"].astype(str)

    if "sent" in alerts.columns and "identifier" in alerts.columns:
        alerts["sent_dt"] = pd.to_datetime(alerts["sent"], errors="coerce", utc=True).dt.tz_convert(None)
        alerts = alerts.sort_values(["identifier", "sent_dt"]).drop_duplicates(subset=["identifier"], keep="last")
        alerts = alerts.drop(columns=["sent_dt"], errors="ignore")
    elif "identifier" in alerts.columns:
        alerts = alerts.drop_duplicates(subset=["identifier"], keep="last")

    write_df(alerts, parsed_out, args.out_format)
    if args.also_csv and args.out_format == "parquet":
        alerts.to_csv(parsed_out.with_suffix(".csv"), index=False, encoding="utf-8")

    # build daily/weekly flags (dust + heat)
    study_start = pd.Timestamp(args.start)
    study_end = pd.Timestamp(args.end)
    alerts2, daily, weekly = build_daily_weekly_flags(alerts, study_start, study_end, area_keywords)

    write_df(daily, daily_out, args.out_format)
    write_df(weekly, weekly_out, args.out_format)

    if args.also_csv and args.out_format == "parquet":
        daily.to_csv(daily_out.with_suffix(".csv"), index=False, encoding="utf-8")
        weekly.to_csv(weekly_out.with_suffix(".csv"), index=False, encoding="utf-8")

    dust_matched = int(((alerts2["is_dust_event"] == 1) & (alerts2["is_area_match"] == 1)).sum()) if len(alerts2) else 0
    heat_matched = int(((alerts2["is_heat_event"] == 1) & (alerts2["is_area_match"] == 1)).sum()) if len(alerts2) else 0

    log(f"parsed_alert_rows={len(alerts)} dust+area matched={dust_matched} heat+area matched={heat_matched}", log_file)
    log(f"daily rows={len(daily)} weekly rows={len(weekly)}", log_file)
    log(f"saved parsed={parsed_out}", log_file)
    log(f"saved daily={daily_out}", log_file)
    log(f"saved weekly={weekly_out}", log_file)
    log("DONE", log_file)


if __name__ == "__main__":
    main()