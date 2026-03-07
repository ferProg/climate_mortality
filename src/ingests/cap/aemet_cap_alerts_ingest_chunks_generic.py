from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import os
import random
import re
import tarfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import xml.etree.ElementTree as ET

from src.utils.io import ensure_dir
from src.utils.logging import setup_logging

import logging
LOGGER = logging.getLogger(__name__)

BASE = "https://opendata.aemet.es/opendata/api"


# -------------------------
# Area keywords (loose matching)
# -------------------------
DEFAULT_AREA_KEYWORDS = {
    "tenerife": ["tenerife", "tfe", "santa cruz", "sta cruz", "la laguna", "candelaria", "arona", "granadilla"],
    "gran_canaria": ["gran canaria", "las palmas", "telde", "gáldar", "galdar", "aguimes", "agüimes"],
    "lanzarote": ["lanzarote", "arrecife", "teguise", "tinajo", "yaiza", "tías", "tias"],
    "fuerteventura": ["fuerteventura", "puerto del rosario", "pájara", "pajara", "antigua", "tuineje"],
    "la_palma": ["la palma", "santa cruz de la palma", "los llanos", "breña", "brena", "tazacorte"],
    "la_gomera": ["la gomera", "san sebastián", "san sebastian", "valle gran rey", "hermigua"],
    "el_hierro": ["el hierro", "hierro", "valverde", "frontera", "el pinar"],
}

# Canonical islands used by extractor
CANONICAL_ISLANDS = [
    "tenerife",
    "gran_canaria",
    "la_palma",
    "lanzarote",
    "fuerteventura",
    "la_gomera",
    "el_hierro",
]


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
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="AEMET CAP archive ingest for Canary Islands (chunked), partitioned by year_onset=YYYY."
    )
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--step-days", type=int, default=7, help="Query window size in days (default 7)")
    p.add_argument("--sleep", type=float, default=0.5, help="Base sleep seconds between successful chunk calls (default 0.5)")
    p.add_argument("--resume", action="store_true", help="Skip partitions already written for a chunk_key")
    p.add_argument("--debug", action="store_true", help="More verbose logging")
    return p.parse_args()


# -------------------------
# HTTP retry + safe JSON
# -------------------------
def fetch_response_retry(
    url: str,
    api_key: str | None = None,
    max_tries: int = 10,
    min_interval_s: float = 20.0,
    base_sleep: float = 10.0,
    accept_json: bool = False,
) -> requests.Response:
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

    last_err: Exception | None = None

    for i in range(1, max_tries + 1):
        try:
            _rate_limit(min_interval_s)
            r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)

            if r.status_code == 429:
                # Respect Retry-After if present
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    sleep_s = int(ra)
                else:
                    sleep_s = min(60 * i + random.uniform(0, 30.0), 600)
                LOGGER.warning("429 Too Many Requests. Sleeping %.2fs (attempt %s/%s)", sleep_s, i, max_tries)
                time.sleep(sleep_s)
                continue

            if r.status_code in (500, 502, 503, 504):
                sleep_s = min(base_sleep * (2 ** (i - 1)) + random.uniform(0, 1.0), 120)
                LOGGER.warning("Server %s. Sleeping %.2fs (attempt %s/%s)", r.status_code, sleep_s, i, max_tries)
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                r.raise_for_status()

            return r

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.HTTPError) as e:
            last_err = e
            sleep_s = min(base_sleep * (2 ** (i - 1)) + random.uniform(0, 1.0), 120)
            LOGGER.warning("Request error: %s | sleeping %.2fs (attempt %s/%s)", e, sleep_s, i, max_tries)
            time.sleep(sleep_s)

    raise RuntimeError(f"Too many retries for {url}. Last error: {last_err}")


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
            LOGGER.info("DEBUG outer tar members: %s", len(members))
            if members:
                LOGGER.info("DEBUG first outer member: %s", members[0].name)

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
# Helpers
# -------------------------

# --- GLOBAL rate limiter (one request every N seconds) ---
_LAST_CALL_TS = 0.0

def _rate_limit(min_interval_s: float) -> None:
    global _LAST_CALL_TS
    now = time.time()
    wait = (_LAST_CALL_TS + min_interval_s) - now
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL_TS = time.time()
def iter_ranges(start: datetime, end: datetime, step_days: int):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=step_days) - timedelta(seconds=1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(seconds=1)


def chunk_hash(fechaini: str, fechafin: str) -> str:
    return hashlib.md5(f"{fechaini}|{fechafin}".encode("utf-8")).hexdigest()[:12]


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


def add_canarias_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      - onset_dt/expires_dt (UTC)
      - year_onset
      - has_<island> flags
      - is_dust_event, is_heat_event, level_score
    """
    out = df.copy()

    out["onset_dt"] = pd.to_datetime(out.get("onset"), errors="coerce", utc=True)
    out["expires_dt"] = pd.to_datetime(out.get("expires"), errors="coerce", utc=True)
    out["year_onset"] = out["onset_dt"].dt.year

    # Text blobs
    area_blob = out.get("areaDesc", "").fillna("").astype(str).str.lower()
    main_blob = (
        out.get("event", "").fillna("").astype(str) + " " +
        out.get("headline", "").fillna("").astype(str) + " " +
        out.get("description", "").fillna("").astype(str)
    ).str.lower()

    dust_re = re.compile("|".join(map(re.escape, [k.lower() for k in DUST_KEYWORDS])))
    heat_re = re.compile("|".join(map(re.escape, [k.lower() for k in HEAT_KEYWORDS])))

    out["is_dust_event"] = main_blob.str.contains(dust_re, na=False).astype(int)
    out["is_heat_event"] = main_blob.str.contains(heat_re, na=False).astype(int)

    sev_score = out.get("severity", "").fillna("").astype(str).apply(_severity_score)
    text_score = main_blob.apply(_color_score_from_text)
    out["level_score"] = pd.concat([sev_score, text_score], axis=1).max(axis=1).fillna(0).astype(int)

    # has_<island> flags from areaDesc
    for isl in CANONICAL_ISLANDS:
        kws = DEFAULT_AREA_KEYWORDS.get(isl, [isl])
        if not kws:
            out[f"has_{isl}"] = 0
            continue
        area_re = re.compile("|".join(map(re.escape, [k.lower() for k in kws])))
        out[f"has_{isl}"] = area_blob.str.contains(area_re, na=False).astype(int)

    return out


# -------------------------
# Main
# -------------------------
def main() -> None:
    args = parse_args()

    api_key = os.environ.get("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError("Missing env var AEMET_API_KEY. PowerShell: $env:AEMET_API_KEY='...'; then run.")

    project_root = Path(__file__).resolve().parents[3]  # src/ingests/cap -> project root
    data_root = project_root / "data"
    out_root = data_root / "interim" / "cap" / "canarias"
    ensure_dir(out_root)

    # logs
    logs_dir = project_root / "logs" / "cap" / "canarias"
    ensure_dir(logs_dir)
    log_fp = logs_dir / f"aemet_cap_canarias_{args.start}_{args.end}.log"
    setup_logging(log_fp)

    start_dt = datetime.fromisoformat(args.start)
    end_dt = datetime.fromisoformat(args.end) + timedelta(hours=23, minutes=59, seconds=59)

    LOGGER.info("START canarias CAP ingest | %s..%s | step_days=%s resume=%s", args.start, args.end, args.step_days, args.resume)
    LOGGER.info("Output dir: %s", out_root)

    calls = 0
    saved_parts = 0
    skipped_parts = 0
    no_data_chunks = 0
    meta_non_json = 0
    parsed_empty = 0

    for a, b in iter_ranges(start_dt, end_dt, step_days=args.step_days):
        fechaini = a.strftime("%Y-%m-%dT%H:%M:%SUTC")
        fechafin = b.strftime("%Y-%m-%dT%H:%M:%SUTC")
        key = chunk_hash(fechaini, fechafin)

        api_url = f"{BASE}/avisos_cap/archivo/fechaini/{fechaini}/fechafin/{fechafin}"

        # meta fetch (must be JSON)
        try:
            resp = fetch_response_retry(api_url, api_key=api_key, accept_json=True)
            meta = safe_json_response(resp)
        except Exception as e:
            meta_non_json += 1
            LOGGER.warning("[META NON-JSON] %s -> %s | err=%s", fechaini, fechafin, e)
            time.sleep(args.sleep)
            continue

        calls += 1

        if isinstance(meta, dict) and meta.get("estado") == 404:
            no_data_chunks += 1
            if args.debug:
                LOGGER.info("[%s] %s -> %s : no data (404)", calls, fechaini, fechafin)
            time.sleep(args.sleep)
            continue

        datos_url = meta.get("datos") if isinstance(meta, dict) else None
        if not datos_url:
            LOGGER.warning("[%s] unexpected meta (no datos). meta=%s", calls, meta)
            time.sleep(args.sleep)
            continue

        sh_resp = fetch_response_retry(datos_url, api_key=None, accept_json=False)
        payload = sh_resp.content

        parsed_rows = parse_cap_from_tar_payload(payload, debug=args.debug)
        if not parsed_rows:
            parsed_empty += 1
            if args.debug:
                LOGGER.info("[%s] parsed_rows=0 bytes=%s url=%s", calls, len(payload), datos_url)
            time.sleep(args.sleep)
            continue

        df_chunk = pd.DataFrame(parsed_rows)
        df_chunk["sh_url"] = datos_url
        df_chunk["range_start"] = fechaini
        df_chunk["range_end"] = fechafin
        df_chunk["chunk_key"] = key

        # dedup within chunk by identifier (keep last)
        if "identifier" in df_chunk.columns:
            df_chunk["identifier"] = df_chunk["identifier"].astype(str)
            df_chunk = df_chunk.drop_duplicates(subset=["identifier"], keep="last")

        # Add flags + year partition
        df_chunk = add_canarias_flags(df_chunk)

        # If onset_dt missing, we can't partition; drop those
        df_chunk = df_chunk.dropna(subset=["onset_dt"]).copy()
        if df_chunk.empty:
            parsed_empty += 1
            time.sleep(args.sleep)
            continue

        # Write per year_onset partition; deterministic filename per chunk key
        for year_onset, df_y in df_chunk.groupby("year_onset"):
            if pd.isna(year_onset):
                continue
            year_onset = int(year_onset)
            year_dir = out_root / f"year_onset={year_onset}"
            ensure_dir(year_dir)

            out_fp = year_dir / f"part_{key}.parquet"
            if args.resume and out_fp.exists():
                skipped_parts += 1
                continue

            df_y.to_parquet(out_fp, index=False)
            saved_parts += 1

            if args.debug:
                LOGGER.info("[%s] saved %s rows -> %s", calls, len(df_y), out_fp)

        time.sleep(args.sleep)

    LOGGER.info(
        "DONE calls=%s saved_parts=%s skipped_parts=%s no_data_chunks=%s meta_non_json=%s parsed_empty=%s",
        calls, saved_parts, skipped_parts, no_data_chunks, meta_non_json, parsed_empty
    )
    LOGGER.info("Output dataset: %s", out_root)
    LOGGER.info("Log: %s", log_fp)


if __name__ == "__main__":
    main()