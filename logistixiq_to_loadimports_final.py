#!/usr/bin/env python3
"""
LogistixIQ Carrier Export API -> Voldhaul Load Collector importer

What this script does
---------------------
1. Calls LogistixIQ export API for a date window.
2. Downloads the JSON export file from export_info.download_url.
3. Maps each source record into the exact top-level JSON shape expected by
   the load-collector /api/loads/push endpoint.
4. Pushes the payload as multipart/form-data with:
      - payload  (required)
      - bolimage (optional)
5. The target Laravel service stores the decoded JSON into loadimports.payload_json
   and extracts jobname from the top-level payload JSON.

Safety / operations
-------------------
- SQLite dedupe by source_key + source_hash
- local failure queue with retry support
- replay mode from LOCAL_EXPORT_JSON
- optional payload sample saving
- source request spacing + rolling 24h quota guard
- optional loop mode polling
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import mimetypes
import os
from pathlib import Path
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


# ---------------------------------------------------------------------------
# dotenv helpers
# ---------------------------------------------------------------------------


def load_simple_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


load_simple_dotenv()


# ---------------------------------------------------------------------------
# env/config helpers
# ---------------------------------------------------------------------------


def env_first(names: Iterable[str], default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


class Config:
    # Source: LogistixIQ
    LOGISTIXIQ_EXPORT_URL = env_first(
        ["LOGISTIXIQ_EXPORT_URL", "SOURCE_URL"],
        "https://fhll67llek.execute-api.us-east-1.amazonaws.com/prod/carrier-export",
    )
    LOGISTIXIQ_API_KEY = env_first(["LOGISTIXIQ_API_KEY", "SOURCE_API_KEY"], "")
    LOGISTIXIQ_CARRIER_ID = env_first(["LOGISTIXIQ_CARRIER_ID", "SOURCE_CARRIER_ID"], "464")

    # Target: Voldhaul Load Collector
    LOAD_COLLECTOR_PUSH_URL = env_first(
        ["LOAD_COLLECTOR_PUSH_URL", "TARGET_PUSH_URL"],
        "https://api.sandbox.voldhaul.com/api/load-collector/api/loads/push",
    )
    LOAD_COLLECTOR_JOBS_URL = env_first(
        ["LOAD_COLLECTOR_JOBS_URL", "TARGET_JOBS_URL"],
        "https://api.sandbox.voldhaul.com/api/load-collector/api/importjobs/list",
    )
    LOAD_COLLECTOR_BEARER_TOKEN = env_first(
        ["LOAD_COLLECTOR_BEARER_TOKEN", "TARGET_BEARER_TOKEN"],
        "",
    )

    # Logging / local state
    SQLITE_PATH = os.getenv("SQLITE_PATH", "./logistixiq_importer.db")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "./logistixiq_importer.log")
    LOG_FILE_MAX_BYTES = env_int("LOG_FILE_MAX_BYTES", 5 * 1024 * 1024)
    LOG_FILE_BACKUP_COUNT = env_int("LOG_FILE_BACKUP_COUNT", 3)

    # Polling / windows
    INITIAL_LOOKBACK_MINUTES = env_int("INITIAL_LOOKBACK_MINUTES", 1440)
    OVERLAP_MINUTES = env_int("OVERLAP_MINUTES", 60)
    FORCE_WINDOW_START = os.getenv("FORCE_WINDOW_START", "")
    FORCE_WINDOW_END = os.getenv("FORCE_WINDOW_END", "")
    LOOP = env_bool("LOOP", False)
    POLL_SECONDS = env_int("POLL_SECONDS", 900)

    # Source safety
    EXPORT_REQUEST_MIN_INTERVAL_SECONDS = env_int("EXPORT_REQUEST_MIN_INTERVAL_SECONDS", 2)
    EXPORT_REQUEST_MAX_PER_24H = env_int("EXPORT_REQUEST_MAX_PER_24H", 95)

    # Retry / timeouts
    SOURCE_TIMEOUT_SECONDS = env_int("SOURCE_TIMEOUT_SECONDS", 60)
    DOWNLOAD_TIMEOUT_SECONDS = env_int("DOWNLOAD_TIMEOUT_SECONDS", 120)
    TARGET_TIMEOUT_SECONDS = env_int("TARGET_TIMEOUT_SECONDS", 60)
    SOURCE_MAX_RETRIES = env_int("SOURCE_MAX_RETRIES", 4)
    TARGET_MAX_RETRIES = env_int("TARGET_MAX_RETRIES", 4)

    # Testing / replay
    DRY_RUN = env_bool("DRY_RUN", False)
    LOCAL_EXPORT_JSON = os.getenv("LOCAL_EXPORT_JSON", "")
    ONLY_FIRST_N_RECORDS = env_int("ONLY_FIRST_N_RECORDS", 0)
    MAX_PUSHES_PER_RUN = env_int("MAX_PUSHES_PER_RUN", 0)

    SAVE_EXPORT_JSON = env_bool("SAVE_EXPORT_JSON", True)
    EXPORT_ARCHIVE_DIR = os.getenv("EXPORT_ARCHIVE_DIR", "./export_archive")
    SAVE_PAYLOAD_SAMPLES = env_bool("SAVE_PAYLOAD_SAMPLES", True)
    PAYLOAD_SAMPLES_DIR = env_first(["PAYLOAD_SAMPLES_DIR", "PAYLOAD_SAMPLE_DIR"], "./payload_samples")

    # Optional helpers
    ATTACH_IMAGE = env_bool("ATTACH_IMAGE", True)
    VALIDATE_JOBNAMES = env_bool("VALIDATE_JOBNAMES", False)
    LOCAL_IMAGE_DIRS = [
        x.strip()
        for x in os.getenv("LOCAL_IMAGE_DIRS", r"C:\logistixiq-test\images").split(",")
        if x.strip()
    ]
    SCRAPER_SOURCE_NAME = os.getenv("SCRAPER_SOURCE_NAME", "Logistixiq")
    DEFAULT_CARRIER = os.getenv("DEFAULT_CARRIER", "WYO Trucking LLC")


logger = logging.getLogger("logistixiq_importer")


# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else getattr(logging, Config.LOG_LEVEL, logging.INFO)
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level)
    logger.addHandler(stream_handler)

    if Config.LOG_FILE_PATH:
        log_path = Path(Config.LOG_FILE_PATH)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=Config.LOG_FILE_MAX_BYTES,
            backupCount=Config.LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    logger.propagate = False


# ---------------------------------------------------------------------------
# generic utils
# ---------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(timezone.utc)



def iso_now() -> str:
    return utcnow().isoformat(timespec="seconds")



def ensure_dir(path_str: str) -> Path:
    path = Path(path_str)
    path.mkdir(parents=True, exist_ok=True)
    return path



def write_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")



def normalize_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()



def to_list(value: Any) -> List[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    return [value]



def parse_any_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None

    s = str(value).strip()
    if not s:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %I:%M:%S %p",
        "%Y-%m-%d %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%y %I:%M %p",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None



def fmt_source_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")



def iso_or_blank(value: Any) -> str:
    dt = parse_any_datetime(value)
    return dt.isoformat() if dt else ""



def yyyy_mm_dd_or_blank(value: Any) -> str:
    dt = parse_any_datetime(value)
    return dt.strftime("%Y-%m-%d") if dt else ""



def pretty_status_time(value: Any) -> str:
    dt = parse_any_datetime(value)
    if dt:
        return dt.strftime("%Y-%m-%d %I:%M:%S %p")
    return normalize_str(value)



def first_non_empty(item: Dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        if key in item:
            value = normalize_str(item.get(key))
            if value:
                return value
    return default



def first_non_empty_raw(item: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in item and item.get(key) not in (None, "", []):
            return item.get(key)
    return None



def maybe_money(value: Any) -> str:
    s = normalize_str(value)
    if not s:
        return ""
    if s.startswith("$"):
        return s
    try:
        amount = float(s.replace(",", "").replace("$", ""))
        return f"${amount:g}"
    except Exception:
        return s



def stable_hash(item: Dict[str, Any]) -> str:
    encoded = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()



def choose_source_key(record: Dict[str, Any], payload: Dict[str, Any]) -> str:
    direct = first_non_empty(
        record,
        "order_id",
        "loadnumber",
        "load_number",
        "ticket_no",
        "sand_ticket_no",
        "bol",
        "invoice_id",
    )
    if direct:
        return direct

    joined = "|".join(
        [
            normalize_str(payload.get("jobname")),
            normalize_str(payload.get("ticket_no")),
            normalize_str(payload.get("driver")),
            normalize_str(payload.get("truck")),
            normalize_str(payload.get("status_time")),
        ]
    )
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]



def validate_required_config() -> None:
    missing: List[str] = []
    if not Config.LOGISTIXIQ_API_KEY and not Config.LOCAL_EXPORT_JSON:
        missing.append("LOGISTIXIQ_API_KEY")
    if not Config.LOGISTIXIQ_CARRIER_ID and not Config.LOCAL_EXPORT_JSON:
        missing.append("LOGISTIXIQ_CARRIER_ID")
    if not Config.LOAD_COLLECTOR_BEARER_TOKEN:
        missing.append("LOAD_COLLECTOR_BEARER_TOKEN")

    if missing:
        raise RuntimeError("Missing required config: " + ", ".join(missing))


# ---------------------------------------------------------------------------
# sqlite cache / state
# ---------------------------------------------------------------------------


class Cache:
    def __init__(self, path: str) -> None:
        self.path = path
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS imported_records (
                    source_key TEXT PRIMARY KEY,
                    source_hash TEXT NOT NULL,
                    source_payload_json TEXT NOT NULL,
                    pushed_payload_json TEXT NOT NULL,
                    target_id TEXT,
                    last_seen_at TEXT NOT NULL,
                    last_pushed_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS export_request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS failed_pushes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    source_payload_json TEXT NOT NULL,
                    pushed_payload_json TEXT NOT NULL,
                    last_error TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    first_failed_at TEXT NOT NULL,
                    last_failed_at TEXT NOT NULL,
                    next_retry_at TEXT NOT NULL,
                    is_resolved INTEGER NOT NULL DEFAULT 0
                )
                """
            )

    def get_record(self, source_key: str) -> Optional[sqlite3.Row]:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM imported_records WHERE source_key = ?",
                (source_key,),
            ).fetchone()

    def upsert_record(
        self,
        source_key: str,
        source_hash: str,
        source_payload_json: str,
        pushed_payload_json: str,
        target_id: Optional[str],
    ) -> None:
        now = iso_now()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO imported_records (
                    source_key, source_hash, source_payload_json,
                    pushed_payload_json, target_id, last_seen_at, last_pushed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    source_hash=excluded.source_hash,
                    source_payload_json=excluded.source_payload_json,
                    pushed_payload_json=excluded.pushed_payload_json,
                    target_id=excluded.target_id,
                    last_seen_at=excluded.last_seen_at,
                    last_pushed_at=excluded.last_pushed_at
                """,
                (
                    source_key,
                    source_hash,
                    source_payload_json,
                    pushed_payload_json,
                    target_id or "",
                    now,
                    now,
                ),
            )

    def touch_seen(self, source_key: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE imported_records SET last_seen_at = ? WHERE source_key = ?",
                (iso_now(), source_key),
            )

    def get_state(self, key: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT state_value FROM run_state WHERE state_key = ?",
                (key,),
            ).fetchone()
            return row["state_value"] if row else None

    def set_state(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO run_state (state_key, state_value)
                VALUES (?, ?)
                ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value
                """,
                (key, value),
            )

    def log_export_request(self, requested_at: datetime) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO export_request_log (requested_at) VALUES (?)",
                (requested_at.isoformat(),),
            )

    def count_export_requests_last_24h(self, now: datetime) -> int:
        since = (now - timedelta(hours=24)).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM export_request_log WHERE requested_at >= ?",
                (since,),
            ).fetchone()
            return int(row["c"])

    def last_export_request_at(self) -> Optional[datetime]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT requested_at FROM export_request_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            return parse_any_datetime(row["requested_at"])

    def queue_failed_push(
        self,
        source_key: str,
        source_hash: str,
        source_payload_json: str,
        pushed_payload_json: str,
        error_message: str,
        attempts: Optional[int] = None,
    ) -> None:
        now = utcnow()
        with self._conn() as conn:
            existing = conn.execute(
                """
                SELECT id, attempts FROM failed_pushes
                WHERE source_key = ? AND is_resolved = 0
                ORDER BY id DESC
                LIMIT 1
                """,
                (source_key,),
            ).fetchone()

            if existing:
                new_attempts = attempts if attempts is not None else int(existing["attempts"]) + 1
                next_retry_at = (
                    now + timedelta(minutes=min(60, max(1, 2 ** min(new_attempts, 5))))
                ).isoformat()
                conn.execute(
                    """
                    UPDATE failed_pushes
                    SET source_hash = ?,
                        source_payload_json = ?,
                        pushed_payload_json = ?,
                        last_error = ?,
                        attempts = ?,
                        last_failed_at = ?,
                        next_retry_at = ?,
                        is_resolved = 0
                    WHERE id = ?
                    """,
                    (
                        source_hash,
                        source_payload_json,
                        pushed_payload_json,
                        error_message,
                        new_attempts,
                        now.isoformat(),
                        next_retry_at,
                        int(existing["id"]),
                    ),
                )
            else:
                first_attempts = attempts if attempts is not None else 1
                next_retry_at = (now + timedelta(minutes=2)).isoformat()
                conn.execute(
                    """
                    INSERT INTO failed_pushes (
                        source_key, source_hash, source_payload_json, pushed_payload_json,
                        last_error, attempts, first_failed_at, last_failed_at, next_retry_at, is_resolved
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        source_key,
                        source_hash,
                        source_payload_json,
                        pushed_payload_json,
                        error_message,
                        first_attempts,
                        now.isoformat(),
                        now.isoformat(),
                        next_retry_at,
                    ),
                )

    def resolve_failed_push(self, source_key: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE failed_pushes SET is_resolved = 1 WHERE source_key = ? AND is_resolved = 0",
                (source_key,),
            )

    def get_retryable_failed_pushes(self, limit: int = 100) -> List[sqlite3.Row]:
        now = utcnow().isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM failed_pushes
                WHERE is_resolved = 0
                  AND next_retry_at <= ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            return list(rows)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def should_retry_status(status_code: int) -> bool:
    return status_code in {408, 409, 425, 429, 500, 502, 503, 504}



def request_json_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    max_retries: int,
    timeout: int,
    **kwargs: Any,
) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            if should_retry_status(response.status_code) and attempt < max_retries:
                wait = min(30, 2 ** (attempt - 1))
                logger.warning(
                    "%s %s returned %s, retrying in %ss (attempt %s/%s)",
                    method,
                    url,
                    response.status_code,
                    wait,
                    attempt,
                    max_retries,
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            last_exc = exc
            response = getattr(exc, "response", None)
            status_code = response.status_code if response is not None else None
            retryable = status_code is None or should_retry_status(status_code)
            if attempt < max_retries and retryable:
                wait = min(30, 2 ** (attempt - 1))
                logger.warning(
                    "%s %s failed (%s), retrying in %ss (attempt %s/%s)",
                    method,
                    url,
                    exc,
                    wait,
                    attempt,
                    max_retries,
                )
                time.sleep(wait)
                continue
            raise
        except ValueError as exc:
            last_exc = exc
            break

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Failed {method} {url}")


# ---------------------------------------------------------------------------
# source API
# ---------------------------------------------------------------------------


class SourceAPI:
    def __init__(self, cache: Cache) -> None:
        self.cache = cache
        self.session = requests.Session()
        if Config.LOGISTIXIQ_API_KEY:
            self.session.headers.update({"x-api-key": Config.LOGISTIXIQ_API_KEY})

    def _rate_limit(self) -> None:
        now = utcnow()
        used = self.cache.count_export_requests_last_24h(now)
        if used >= Config.EXPORT_REQUEST_MAX_PER_24H:
            raise RuntimeError(
                f"Daily export request limit reached: {used}/{Config.EXPORT_REQUEST_MAX_PER_24H} in last 24h"
            )

        last = self.cache.last_export_request_at()
        if last is not None:
            elapsed = (now - last).total_seconds()
            if elapsed < Config.EXPORT_REQUEST_MIN_INTERVAL_SECONDS:
                sleep_for = Config.EXPORT_REQUEST_MIN_INTERVAL_SECONDS - elapsed
                logger.info("Sleeping %.1fs before next export request", sleep_for)
                time.sleep(sleep_for)

    def request_export(self, start_dt: datetime, end_dt: datetime) -> Dict[str, Any]:
        if not Config.LOGISTIXIQ_API_KEY:
            raise RuntimeError("Missing LOGISTIXIQ_API_KEY")

        self._rate_limit()
        params = {
            "start_datetime": fmt_source_datetime(start_dt),
            "end_datetime": fmt_source_datetime(end_dt),
            "CarrierID": Config.LOGISTIXIQ_CARRIER_ID,
        }
        logger.info("Requesting export %s", params)
        data = request_json_with_retry(
            self.session,
            "GET",
            Config.LOGISTIXIQ_EXPORT_URL,
            params=params,
            max_retries=Config.SOURCE_MAX_RETRIES,
            timeout=Config.SOURCE_TIMEOUT_SECONDS,
        )
        self.cache.log_export_request(utcnow())

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected export response type: {type(data).__name__}")
        if data.get("success") is False:
            raise RuntimeError(f"Export error: {data}")
        export_info = data.get("export_info")
        if not isinstance(export_info, dict):
            raise RuntimeError(f"Missing export_info in response: {data}")
        download_url = normalize_str(export_info.get("download_url"))
        if not download_url:
            raise RuntimeError(f"Missing download_url in export_info: {data}")
        return data

    def download_export_json(self, download_url: str) -> Dict[str, Any]:
        logger.info("Downloading export file %s", download_url)
        data = request_json_with_retry(
            requests.Session(),
            "GET",
            download_url,
            max_retries=Config.SOURCE_MAX_RETRIES,
            timeout=Config.DOWNLOAD_TIMEOUT_SECONDS,
        )
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected download JSON type: {type(data).__name__}")
        return data

    def load_records_from_local_export(self, path_str: str) -> List[Dict[str, Any]]:
        path = Path(path_str)
        logger.info("Loading local export JSON %s", path)
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise RuntimeError("LOCAL_EXPORT_JSON must point to a JSON object with a records array")
        records = data.get("records")
        if not isinstance(records, list):
            raise RuntimeError("LOCAL_EXPORT_JSON file does not contain a records list")
        return [x for x in records if isinstance(x, dict)]


# ---------------------------------------------------------------------------
# target API
# ---------------------------------------------------------------------------


class TargetAPI:
    def __init__(self) -> None:
        self.session = requests.Session()
        if Config.LOAD_COLLECTOR_BEARER_TOKEN:
            self.session.headers.update(
                {"Authorization": f"Bearer {Config.LOAD_COLLECTOR_BEARER_TOKEN}"}
            )

    def list_jobs(self) -> List[str]:
        if not Config.LOAD_COLLECTOR_BEARER_TOKEN:
            raise RuntimeError("Missing LOAD_COLLECTOR_BEARER_TOKEN")
        data = request_json_with_retry(
            self.session,
            "GET",
            Config.LOAD_COLLECTOR_JOBS_URL,
            max_retries=Config.TARGET_MAX_RETRIES,
            timeout=Config.TARGET_TIMEOUT_SECONDS,
        )
        items = data.get("items", []) if isinstance(data, dict) else []
        return [
            normalize_str(item.get("jobname"))
            for item in items
            if isinstance(item, dict) and normalize_str(item.get("jobname"))
        ]

    def fetch_image(self, image_url: str) -> Optional[Tuple[str, bytes, str]]:
        try:
            response = requests.get(image_url, timeout=Config.DOWNLOAD_TIMEOUT_SECONDS)
            response.raise_for_status()
            filename = image_url.rstrip("/").split("/")[-1] or "image"
            content_type = response.headers.get("content-type", "application/octet-stream")
            return filename, response.content, content_type
        except Exception as exc:
            logger.warning("Image download failed for %s: %s", image_url, exc)
            return None

    def resolve_local_pod_image(self, payload: Dict[str, Any]) -> Optional[Tuple[str, bytes, str]]:
        pod_images = [normalize_str(x) for x in to_list(payload.get("pod_images")) if normalize_str(x)]
        if not pod_images:
            return None

        filename = pod_images[0]
        for folder_str in Config.LOCAL_IMAGE_DIRS:
            folder = Path(folder_str)
            candidate = folder / filename
            if candidate.exists() and candidate.is_file():
                content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
                return candidate.name, candidate.read_bytes(), content_type
        return None

    def push_load(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not Config.LOAD_COLLECTOR_BEARER_TOKEN:
            raise RuntimeError("Missing LOAD_COLLECTOR_BEARER_TOKEN")

        payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        files: Dict[str, Tuple[str, Any, str]] = {
            "payload": (
                "load.json",
                payload_bytes,
                "application/json",
            )
        }

        bolimage: Optional[Tuple[str, bytes, str]] = None
        image_url = normalize_str(payload.get("image_download_link"))

        if Config.ATTACH_IMAGE:
            if image_url:
                bolimage = self.fetch_image(image_url)
            if bolimage is None:
                bolimage = self.resolve_local_pod_image(payload)

        if bolimage is not None:
            logger.info("Attaching bolimage %s", bolimage[0])
            files["bolimage"] = bolimage
        else:
            logger.info(
                "No bolimage found for ticket_no=%s loadnumber=%s; image columns will remain NULL",
                normalize_str(payload.get("ticket_no")),
                normalize_str(payload.get("loadnumber")),
            )

        last_exc: Optional[Exception] = None
        for attempt in range(1, Config.TARGET_MAX_RETRIES + 1):
            try:
                response = self.session.post(
                    Config.LOAD_COLLECTOR_PUSH_URL,
                    files=files,
                    timeout=Config.TARGET_TIMEOUT_SECONDS,
                )
                if should_retry_status(response.status_code) and attempt < Config.TARGET_MAX_RETRIES:
                    wait = min(30, 2 ** (attempt - 1))
                    logger.warning(
                        "POST %s returned %s, retrying in %ss (attempt %s/%s)",
                        Config.LOAD_COLLECTOR_PUSH_URL,
                        response.status_code,
                        wait,
                        attempt,
                        Config.TARGET_MAX_RETRIES,
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                try:
                    return response.json()
                except Exception:
                    return {"ok": True, "raw_response": response.text}
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
                response = getattr(exc, "response", None)
                status_code = response.status_code if response is not None else None
                retryable = status_code is None or should_retry_status(status_code)
                if attempt < Config.TARGET_MAX_RETRIES and retryable:
                    wait = min(30, 2 ** (attempt - 1))
                    logger.warning(
                        "Push failed (%s), retrying in %ss (attempt %s/%s)",
                        exc,
                        wait,
                        attempt,
                        Config.TARGET_MAX_RETRIES,
                    )
                    time.sleep(wait)
                    continue
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Push failed")


# ---------------------------------------------------------------------------
# mapping
# ---------------------------------------------------------------------------


def build_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the exact JSON shape that should land in loadimports.payload_json.

    Target shape example required by the user:
    {
      "po": "PFS-500054",
      "eta": null,
      "notes": "Completed order from Logistixiq",
      ...
    }
    """

    order_id = first_non_empty(record, "order_id")
    sand_ticket_no = first_non_empty(record, "sand_ticket_no")
    driver_name = first_non_empty(record, "driver_name")
    loading_site_name = first_non_empty(record, "loading_site_name")
    well_site_name = first_non_empty(record, "well_site_name")
    stage = first_non_empty(record, "stage")
    truck = first_non_empty(record, "truck")
    carrier_name = first_non_empty(record, "carrier_name", default=Config.DEFAULT_CARRIER) or Config.DEFAULT_CARRIER
    po_value = first_non_empty(record, "po", "po_reference")
    sand_type_name = first_non_empty(record, "sand_type_name")
    weight = first_non_empty(record, "weight")
    ton = first_non_empty(record, "ton")
    mileage = first_non_empty(record, "mileage")

    load_arrival = first_non_empty_raw(record, "load_arrival")
    load_depart = first_non_empty_raw(record, "load_depart")
    well_arrival = first_non_empty_raw(record, "well_arrival")
    well_depart = first_non_empty_raw(record, "well_depart")
    order_accepted_at = first_non_empty_raw(record, "order_accepted_at")
    eta = first_non_empty_raw(record, "eta")

    image_url = first_non_empty(
        record,
        "image_download_link",
        "ticket_image_url",
        "pod_image_url",
    )

    pod_images = [
        normalize_str(x)
        for x in to_list(first_non_empty_raw(record, "pod_images"))
        if normalize_str(x)
    ]

    if not pod_images and image_url:
        pod_images = [image_url.rstrip("/").split("/")[-1]]

    status_time_source = first_non_empty_raw(
        record,
        "driver_time_well_departure",
        "well_depart",
        "sand_ticket_uploaded_at",
    )
    delivery_time_source = first_non_empty_raw(
        record,
        "well_depart",
        "driver_time_well_departure",
    )
    invoice_date_source = first_non_empty_raw(
        record,
        "invoice_date",
        "sand_ticket_uploaded_at",
        "well_depart",
    )

    payload = {
        "po": po_value,
        "eta": pretty_status_time(eta) if eta not in (None, "") else None,
        "notes": "Completed order from Logistixiq",
        "stage": stage,
        "state": "DELIVERED",
        "truck": truck,
        "amount": maybe_money(first_non_empty(record, "amount")),
        "driver": driver_name,
        "status": "Order Completed",
        "weight": weight,
        "carrier": carrier_name,
        "jobname": well_site_name,
        "product": sand_type_name,
        "terminal": loading_site_name,
        "box_units": "",
        "ticket_no": sand_ticket_no,
        "timestamp": datetime.now().isoformat(),
        "loadnumber": order_id,
        "pod_images": pod_images,
        "box_numbers": "",
        "status_time": pretty_status_time(status_time_source),
        "gross_weight": "",
        "invoice_date": yyyy_mm_dd_or_blank(invoice_date_source),
        "total_weight": weight,
        "trip_mileage": mileage,
        "wait_at_well": "",
        "ScraperSource": Config.SCRAPER_SOURCE_NAME,
        "delivery_time": pretty_status_time(delivery_time_source),
        "wait_at_loader": "",
        "approved_mileage": "",
        "current_location": loading_site_name,
        "datetime_ordered": "",
        "deadhead_mileage": "",
        "reconcile_status": "Confirmed",
        "datetime_accepted": iso_or_blank(order_accepted_at),
        "datetime_assigned": "",
        "eta_after_transit": "",
        "datetime_delivered": iso_or_blank(well_depart),
        "eta_after_accepted": "",
        "datetime_in_transit": "",
        "image_download_link": image_url,
        "datetime_at_terminal": iso_or_blank(load_arrival),
        "datetime_at_destination": iso_or_blank(well_arrival),
        "timestamp_departure_origin": pretty_status_time(load_depart),
        "delivery_time_departure_destination": pretty_status_time(well_depart),
    }

    if not payload["weight"] and ton:
        payload["weight"] = ton
        payload["total_weight"] = ton

    return payload


# ---------------------------------------------------------------------------
# importer
# ---------------------------------------------------------------------------


@dataclass
class RunStats:
    export_requests: int = 0
    records_seen: int = 0
    skipped_unchanged: int = 0
    pushed: int = 0
    failed: int = 0
    saved_payload_samples: int = 0
    retried_failed: int = 0


class Importer:
    def __init__(self) -> None:
        validate_required_config()
        self.cache = Cache(Config.SQLITE_PATH)
        self.source = SourceAPI(self.cache)
        self.target = TargetAPI()
        self.valid_jobs: Optional[set[str]] = None

    def load_valid_jobs(self) -> None:
        if not Config.VALIDATE_JOBNAMES:
            return
        try:
            jobs = self.target.list_jobs()
            self.valid_jobs = set(jobs)
            logger.info("Loaded %d jobnames from target", len(jobs))
        except Exception as exc:
            self.valid_jobs = None
            logger.warning("Could not load jobname list: %s", exc)

    def get_window(self) -> Tuple[datetime, datetime]:
        if Config.FORCE_WINDOW_START and Config.FORCE_WINDOW_END:
            start_dt = parse_any_datetime(Config.FORCE_WINDOW_START)
            end_dt = parse_any_datetime(Config.FORCE_WINDOW_END)
            if not start_dt or not end_dt:
                raise RuntimeError("FORCE_WINDOW_START and FORCE_WINDOW_END must be valid datetimes")
            return start_dt, end_dt

        now = utcnow()
        last_end = self.cache.get_state("last_successful_window_end")
        if last_end:
            parsed = parse_any_datetime(last_end)
            if parsed:
                return parsed - timedelta(minutes=Config.OVERLAP_MINUTES), now
        return now - timedelta(minutes=Config.INITIAL_LOOKBACK_MINUTES), now

    def split_into_source_chunks(
        self,
        start_dt: datetime,
        end_dt: datetime,
        max_days: int = 31,
    ) -> List[Tuple[datetime, datetime]]:
        chunks: List[Tuple[datetime, datetime]] = []
        cursor = start_dt
        max_span = timedelta(days=max_days)
        while cursor < end_dt:
            chunk_end = min(cursor + max_span, end_dt)
            chunks.append((cursor, chunk_end))
            cursor = chunk_end
        return chunks

    def save_export_json_if_enabled(self, export_json: Dict[str, Any]) -> Optional[Path]:
        if not Config.SAVE_EXPORT_JSON:
            return None
        folder = ensure_dir(Config.EXPORT_ARCHIVE_DIR)
        filename = f"export_{utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        path = folder / filename
        write_json_file(path, export_json)
        logger.info("Saved export JSON to %s", path)
        return path

    def save_payload_sample_if_enabled(self, source_key: str, payload: Dict[str, Any]) -> bool:
        if not Config.SAVE_PAYLOAD_SAMPLES:
            return False
        folder = ensure_dir(Config.PAYLOAD_SAMPLES_DIR)
        safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in source_key)
        path = folder / f"{safe_name}.json"
        write_json_file(path, payload)
        return True

    def load_records(self, stats: RunStats) -> List[Dict[str, Any]]:
        if Config.LOCAL_EXPORT_JSON:
            records = self.source.load_records_from_local_export(Config.LOCAL_EXPORT_JSON)
            logger.info("Loaded %d record(s) from LOCAL_EXPORT_JSON", len(records))
            return records

        start_dt, end_dt = self.get_window()
        logger.info("Polling window %s -> %s", start_dt.isoformat(), end_dt.isoformat())

        chunks = self.split_into_source_chunks(start_dt, end_dt)
        logger.info("Window split into %d export request(s)", len(chunks))

        all_records: List[Dict[str, Any]] = []
        for chunk_start, chunk_end in chunks:
            export_response = self.source.request_export(chunk_start, chunk_end)
            stats.export_requests += 1
            export_info = export_response["export_info"]
            download_url = normalize_str(export_info.get("download_url"))
            export_json = self.source.download_export_json(download_url)
            self.save_export_json_if_enabled(export_json)
            records = export_json.get("records")
            if not isinstance(records, list):
                raise RuntimeError("Downloaded export JSON does not contain a records list")
            typed_records = [x for x in records if isinstance(x, dict)]
            logger.info(
                "Chunk %s -> %s returned %d record(s)",
                chunk_start.isoformat(),
                chunk_end.isoformat(),
                len(typed_records),
            )
            all_records.extend(typed_records)

        self.cache.set_state("last_successful_window_end", end_dt.isoformat())
        return all_records

    def maybe_limit_records_for_testing(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if Config.ONLY_FIRST_N_RECORDS > 0:
            limited = records[: Config.ONLY_FIRST_N_RECORDS]
            logger.info(
                "ONLY_FIRST_N_RECORDS active: %d -> %d record(s)",
                len(records),
                len(limited),
            )
            return limited
        return records

    def push_or_queue_failure(
        self,
        source_key: str,
        source_hash: str,
        source_payload_json: str,
        pushed_payload_json: str,
        payload: Dict[str, Any],
        stats: RunStats,
    ) -> None:
        try:
            if Config.DRY_RUN:
                result = {"ok": True, "id": None, "dry_run": True}
                logger.info("DRY RUN push %s", json.dumps(payload, ensure_ascii=False))
            else:
                result = self.target.push_load(payload)

            target_id = normalize_str(result.get("id")) if isinstance(result, dict) else ""
            self.cache.upsert_record(
                source_key=source_key,
                source_hash=source_hash,
                source_payload_json=source_payload_json,
                pushed_payload_json=pushed_payload_json,
                target_id=target_id,
            )
            self.cache.resolve_failed_push(source_key)
            stats.pushed += 1
            logger.info("Pushed source_key=%s target_id=%s", source_key, target_id)
        except Exception as exc:
            stats.failed += 1
            self.cache.queue_failed_push(
                source_key=source_key,
                source_hash=source_hash,
                source_payload_json=source_payload_json,
                pushed_payload_json=pushed_payload_json,
                error_message=str(exc),
            )
            logger.exception("Push failed for source_key=%s: %s", source_key, exc)

    def process_records(self, records: List[Dict[str, Any]], stats: RunStats) -> None:
        push_cap_remaining = Config.MAX_PUSHES_PER_RUN if Config.MAX_PUSHES_PER_RUN > 0 else None

        for record in records:
            payload = build_payload(record)
            source_key = choose_source_key(record, payload)
            source_hash = stable_hash(record)
            existing = self.cache.get_record(source_key)

            if self.valid_jobs is not None:
                jobname = normalize_str(payload.get("jobname"))
                if jobname and jobname not in self.valid_jobs:
                    logger.warning("jobname not present in import job list: %s", jobname)

            if self.save_payload_sample_if_enabled(source_key, payload):
                stats.saved_payload_samples += 1

            if existing and existing["source_hash"] == source_hash:
                self.cache.touch_seen(source_key)
                stats.skipped_unchanged += 1
                continue

            if push_cap_remaining is not None and push_cap_remaining <= 0:
                logger.info("MAX_PUSHES_PER_RUN reached; stopping further pushes for this run")
                break

            source_payload_json = json.dumps(record, ensure_ascii=False, sort_keys=True)
            pushed_payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            self.push_or_queue_failure(
                source_key=source_key,
                source_hash=source_hash,
                source_payload_json=source_payload_json,
                pushed_payload_json=pushed_payload_json,
                payload=payload,
                stats=stats,
            )
            if push_cap_remaining is not None:
                push_cap_remaining -= 1

    def process_once(self) -> RunStats:
        stats = RunStats()
        self.load_valid_jobs()
        records = self.load_records(stats)
        records = self.maybe_limit_records_for_testing(records)
        stats.records_seen = len(records)
        self.process_records(records, stats)
        return stats

    def retry_failed(self) -> RunStats:
        stats = RunStats()
        rows = self.cache.get_retryable_failed_pushes(limit=100)
        logger.info("Retrying %d failed queue item(s)", len(rows))

        push_cap_remaining = Config.MAX_PUSHES_PER_RUN if Config.MAX_PUSHES_PER_RUN > 0 else None

        for row in rows:
            if push_cap_remaining is not None and push_cap_remaining <= 0:
                logger.info("MAX_PUSHES_PER_RUN reached during failed queue retry")
                break

            source_key = normalize_str(row["source_key"])
            source_hash = normalize_str(row["source_hash"])
            source_payload_json = normalize_str(row["source_payload_json"])
            pushed_payload_json = normalize_str(row["pushed_payload_json"])
            payload = json.loads(pushed_payload_json)

            stats.retried_failed += 1
            try:
                if Config.DRY_RUN:
                    result = {"ok": True, "id": None, "dry_run": True}
                    logger.info("DRY RUN retry push %s", source_key)
                else:
                    result = self.target.push_load(payload)
                target_id = normalize_str(result.get("id")) if isinstance(result, dict) else ""
                self.cache.upsert_record(
                    source_key=source_key,
                    source_hash=source_hash,
                    source_payload_json=source_payload_json,
                    pushed_payload_json=pushed_payload_json,
                    target_id=target_id,
                )
                self.cache.resolve_failed_push(source_key)
                stats.pushed += 1
                logger.info("Retried successfully source_key=%s target_id=%s", source_key, target_id)
            except Exception as exc:
                stats.failed += 1
                attempts = int(row["attempts"]) + 1
                self.cache.queue_failed_push(
                    source_key=source_key,
                    source_hash=source_hash,
                    source_payload_json=source_payload_json,
                    pushed_payload_json=pushed_payload_json,
                    error_message=str(exc),
                    attempts=attempts,
                )
                logger.exception("Retry failed for source_key=%s: %s", source_key, exc)

            if push_cap_remaining is not None:
                push_cap_remaining -= 1

        return stats


# ---------------------------------------------------------------------------
# cli
# ---------------------------------------------------------------------------


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry queued failed pushes only; do not call the source export API",
    )
    return parser.parse_args(argv)



def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    setup_logging(debug=args.debug)

    try:
        importer = Importer()

        if args.retry_failed:
            stats = importer.retry_failed()
            logger.info(
                "Retry complete | retried_failed=%s pushed=%s failed=%s",
                stats.retried_failed,
                stats.pushed,
                stats.failed,
            )
            return 0

        loop_mode = Config.LOOP and not args.once
        if loop_mode:
            logger.info("Loop mode enabled; polling every %s seconds", Config.POLL_SECONDS)
            while True:
                stats = importer.process_once()
                logger.info(
                    "Run complete | export_requests=%s records_seen=%s skipped=%s pushed=%s failed=%s saved_payload_samples=%s",
                    stats.export_requests,
                    stats.records_seen,
                    stats.skipped_unchanged,
                    stats.pushed,
                    stats.failed,
                    stats.saved_payload_samples,
                )
                time.sleep(Config.POLL_SECONDS)
        else:
            stats = importer.process_once()
            logger.info(
                "Run complete | export_requests=%s records_seen=%s skipped=%s pushed=%s failed=%s saved_payload_samples=%s",
                stats.export_requests,
                stats.records_seen,
                stats.skipped_unchanged,
                stats.pushed,
                stats.failed,
                stats.saved_payload_samples,
            )
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
