'''Función: Descarga/ingesta los datos de avisos meteorológicos AEMET (CAP, Common Alerting Protocol) para Tenerife, los normaliza y los guarda como dataset “raw/interim” con fechas consistentes.
Salida típica: ficheros intermedios con avisos por día/zona/nivel (según tu parsing).'''

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import os
import time
import re
import io
import tarfile
import gzip
import requests
import pandas as pd
import xml.etree.ElementTree as ET


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
LOGS = PROJECT_ROOT / "logs"

OUT_ALERTS_PARSED = DATA_RAW / "aemet_alerts_cap_parsed.csv"
OUT_DAILY = DATA_PROCESSED / "aemet_calima_alerts_daily.csv"
OUT_WEEKLY = DATA_PROCESSED / "aemet_calima_alerts_weekly.csv"
LOG_FILE = LOGS / "01_ingest_aemet_alerts_log.txt"

BASE = "https://opendata.aemet.es/opendata/api"

STUDY_START = pd.Timestamp("2018-06-18")
STUDY_END   = pd.Timestamp("2025-12-31")



# Ajustaremos keywords cuando veamos eventos reales
AREA_KEYWORDS = [
    "canarias", "tenerife", "la palma", "gran canaria", "lanzarote",
    "fuerteventura", "la gomera", "el hierro"
]
DUST_KEYWORDS = ["calima", "polvo", "suspensi", "intrusión sahariana", "sahar"]


def fetch_response_retry(url: str, api_key: str | None = None, max_tries: int = 10, base_sleep: float = 2.0):
    """
    /opendata/api/... -> requiere api_key
    /opendata/sh/...  -> NO requiere api_key
    Reintenta:
      - HTTP 429 y 5xx
      - errores de red (ConnectionError/Timeout/ProtocolError/etc.)
    """
    use_key = (api_key is not None) and ("/opendata/api/" in url)

    for i in range(max_tries):
        try:
            if use_key:
                r = requests.get(url, params={"api_key": api_key}, timeout=60, allow_redirects=True)
            else:
                r = requests.get(url, timeout=60, allow_redirects=True)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(base_sleep * (2 ** i), 120))
                continue

            if r.status_code >= 400:
                r.raise_for_status()

            return r

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            # network-level transient
            time.sleep(min(base_sleep * (2 ** i), 120))
            continue

    raise RuntimeError(f"Too many retries for {url}")



# ---------- robust XML parsing (namespaces ok) ----------
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
        # fallback: at least keep alert-level record
        rows.append(
            {
                **alert_fields,
                "event": "",
                "urgency": "",
                "severity": "",
                "certainty": "",
                "onset": "",
                "expires": "",
                "headline": "",
                "description": "",
                "areaDesc": "",
            }
        )
        return rows

    for info in infos:
        area_descs = []
        for area in find_all(info, "area"):
            ad = find_first_text(area, "areaDesc")
            if ad:
                area_descs.append(ad)

        rows.append(
            {
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
            }
        )

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


def parse_cap_from_tar_payload(payload: bytes, debug: bool = True) -> list[dict]:
    """
    OUTER TAR (from /sh/...) contains members: *.gz
    Each *.gz -> gunzip -> INNER TAR
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


def normalize_dt(s: str) -> pd.Timestamp:
    if not s:
        return pd.NaT
    return pd.to_datetime(s, errors="coerce", utc=True).tz_convert(None)


def iter_ranges(start: datetime, end: datetime, step_days: int = 2):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=step_days) - timedelta(seconds=1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(seconds=1)


def build_daily_weekly_flags(alerts: pd.DataFrame):
    alerts = alerts.copy()

    # 1) Parse datetimes
    alerts["onset_dt"] = pd.to_datetime(alerts["onset"], errors="coerce", utc=True).dt.tz_convert(None)
    alerts["expires_dt"] = pd.to_datetime(alerts["expires"], errors="coerce", utc=True).dt.tz_convert(None)

    # 2) Dedup ES/EN using identifier (keep latest 'sent' if present)
    if "sent" in alerts.columns:
        alerts["sent_dt"] = pd.to_datetime(alerts["sent"], errors="coerce", utc=True).dt.tz_convert(None)
        alerts = alerts.sort_values(["identifier", "sent_dt"]).drop_duplicates(subset=["identifier"], keep="last")
    else:
        alerts = alerts.drop_duplicates(subset=["identifier"], keep="last")

    # 3) Canarias filter (areaDesc)
    area_blob = alerts["areaDesc"].fillna("").str.lower()
    area_re = re.compile("|".join(map(re.escape, [k.lower() for k in AREA_KEYWORDS])))
    alerts["is_canarias"] = area_blob.str.contains(area_re, na=False).astype(int)

    # 4) Dust event filter (robust)
    ev = alerts["event"].fillna("").str.lower()
    alerts["is_dust_event"] = (
        ev.str.contains("polvo en suspensión", na=False)
        | ev.str.contains("dust warning", na=False)
    ).astype(int)

    # 5) Dust severity score (optional, useful)
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

    alerts["dust_level"] = alerts["event"].apply(dust_level_score)

    dust_can = alerts[(alerts["is_dust_event"] == 1) & (alerts["is_canarias"] == 1)].copy()
    dust_can = dust_can[dust_can["onset_dt"].notna() & dust_can["expires_dt"].notna()].copy()

    # 6) Build daily/weekly
    if dust_can.empty:
        daily = pd.DataFrame(columns=["day", "aemet_dust_alert_day", "dust_level_max_day"])
        weekly = pd.DataFrame(columns=["week_start", "aemet_dust_alert_days", "aemet_dust_alert_any",
                                       "dust_level_max_week", "n_days", "coverage"])
        return alerts, daily, weekly

    # daily index based on min/max of dust alerts (later you can clip to your study window)
    day_index = pd.date_range(STUDY_START, STUDY_END, freq="D")


    daily = pd.DataFrame({"day": day_index})
    daily["aemet_dust_alert_day"] = 0
    daily["dust_level_max_day"] = 0

    for _, r in dust_can.iterrows():
        a0 = r["onset_dt"].floor("D")
        b0 = r["expires_dt"].floor("D")
        m = (daily["day"] >= a0) & (daily["day"] <= b0)
        daily.loc[m, "aemet_dust_alert_day"] = 1
        daily.loc[m, "dust_level_max_day"] = daily.loc[m, "dust_level_max_day"].clip(lower=r["dust_level"])

    daily["week_start"] = (daily["day"] - pd.to_timedelta(daily["day"].dt.weekday, unit="D")).dt.normalize()

    weekly = (daily.groupby("week_start", as_index=False)
              .agg(aemet_dust_alert_days=("aemet_dust_alert_day", "sum"),
                   dust_level_max_week=("dust_level_max_day", "max"),
                   n_days=("day", "count")))

    weekly["aemet_dust_alert_any"] = (weekly["aemet_dust_alert_days"] > 0).astype(int)
    weekly["coverage"] = weekly["n_days"] / 7
    weekly = weekly[weekly["n_days"] == 7].copy()


    return alerts, daily, weekly

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    api_key = os.environ.get("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError("Missing env var AEMET_API_KEY. PowerShell: $env:AEMET_API_KEY='...'; then run.")

    # TEST (2 días). Luego lo cambias a 2018-06-18 -> 2025-12-31
    #start = datetime(2024, 2, 1, 0, 0, 0)
    #end = datetime(2024, 2, 2, 23, 59, 59)
    start = datetime(2018, 6, 18, 0, 0, 0)
    end = datetime(2025, 12, 31, 23, 59, 59)

    all_rows: list[dict] = []
    calls = 0

    already = set()
    if OUT_ALERTS_PARSED.exists():
        try:
            tmp = pd.read_csv(OUT_ALERTS_PARSED, usecols=["identifier"])
            already = set(tmp["identifier"].dropna().astype(str).unique())
            print(f"[RESUME] existing identifiers: {len(already)}")
        except Exception:
            already = set()


    for a, b in iter_ranges(start, end, step_days=2):
        fechaini = a.strftime("%Y-%m-%dT%H:%M:%SUTC")
        fechafin = b.strftime("%Y-%m-%dT%H:%M:%SUTC")

        api_url = f"{BASE}/avisos_cap/archivo/fechaini/{fechaini}/fechafin/{fechafin}"
        meta = fetch_response_retry(api_url, api_key=api_key).json()
        calls += 1

        if isinstance(meta, dict) and meta.get("estado") == 404:
            print(f"[{calls}] {fechaini}->{fechafin}: no data")
            continue

        datos_url = meta.get("datos")
        if not datos_url:
            print(f"[{calls}] unexpected meta keys: {list(meta.keys())}")
            continue

        sh_resp = fetch_response_retry(datos_url, api_key=None)
        payload = sh_resp.content
        ctype = (sh_resp.headers.get("Content-Type") or "").lower()
        print(f"[{calls}] sh content-type={ctype} bytes={len(payload)} first8={payload[:8]!r}")

        parsed_rows = parse_cap_from_tar_payload(payload, debug=True)
        if parsed_rows:
            df_chunk = pd.DataFrame(parsed_rows)

            # evita duplicados si estás reanudando
            if "identifier" in df_chunk.columns and already:
                df_chunk["identifier"] = df_chunk["identifier"].astype(str)
                df_chunk = df_chunk[~df_chunk["identifier"].isin(already)].copy()

            if not df_chunk.empty:
                header = not OUT_ALERTS_PARSED.exists()
                df_chunk.to_csv(OUT_ALERTS_PARSED, mode="a", header=header, index=False)
                if "identifier" in df_chunk.columns:
                    already.update(df_chunk["identifier"].astype(str).unique())


        for row in parsed_rows:
            row["sh_url"] = datos_url
            row["range_start"] = fechaini
            row["range_end"] = fechafin
            all_rows.append(row)

        time.sleep(0.6)

    alerts = pd.read_csv(OUT_ALERTS_PARSED)
    if alerts.empty:
        print("No alerts downloaded/parsed (empty DataFrame).")
        LOG_FILE.write_text("No alerts downloaded/parsed (empty DataFrame).\n", encoding="utf-8")
        return

    alerts.to_csv(OUT_ALERTS_PARSED, index=False)

    alerts2, daily, weekly = build_daily_weekly_flags(alerts)

    # clip to study window (example)
    study_start = pd.Timestamp("2018-06-18")
    study_end = pd.Timestamp("2025-12-31")
    daily = daily[(daily["day"] >= study_start) & (daily["day"] <= study_end)].copy()
    weekly = weekly[(weekly["week_start"] >= study_start) & (weekly["week_start"] <= study_end)].copy()

    daily.to_csv(OUT_DAILY, index=False)
    weekly.to_csv(OUT_WEEKLY, index=False)

    log_text = (
        "01_ingest_aemet_alerts.py\n"
        f"RANGE: {start} -> {end}\n"
        f"calls={calls}\n"
        f"parsed rows={len(alerts)} saved={OUT_ALERTS_PARSED}\n"
        f"dust+canarias matched rows={int(((alerts2['is_dust_event']==1) & (alerts2['is_canarias']==1)).sum())}\n"
        f"daily rows={len(daily)} saved={OUT_DAILY}\n"
        f"weekly rows={len(weekly)} saved={OUT_WEEKLY}\n"
    )
    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)


if __name__ == "__main__":
    main()
