from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from kia_models_parser import DEFAULT_OUTPUT_PATH, scrape_once


LOGGER = logging.getLogger("kia_models_service")

DEFAULT_PORT = 8000
DEFAULT_REFRESH_INTERVAL_SECONDS = 60 * 60
DEFAULT_SCRAPE_TIMEOUT_SECONDS = 30

IS_RENDER = os.getenv("RENDER") == "true"
DEFAULT_CACHE_PATH = Path("/tmp/kia_models.json") if IS_RENDER else DEFAULT_OUTPUT_PATH

CACHE_PATH = Path(os.getenv("KIA_MODELS_OUTPUT", str(DEFAULT_CACHE_PATH)))
SEED_PATH = Path(os.getenv("KIA_MODELS_SEED_PATH", str(DEFAULT_OUTPUT_PATH)))
REFRESH_INTERVAL_SECONDS = int(os.getenv("REFRESH_INTERVAL_SECONDS", str(DEFAULT_REFRESH_INTERVAL_SECONDS)))
SCRAPE_TIMEOUT_SECONDS = int(os.getenv("SCRAPE_TIMEOUT_SECONDS", str(DEFAULT_SCRAPE_TIMEOUT_SECONDS)))

_state_lock = threading.RLock()
_refresh_condition = threading.Condition(_state_lock)
_cache: dict[str, object] | None = None
_last_success_epoch = 0.0
_last_error = ""
_refreshing = False

MODEL_ALIASES = {
    "kia sportage": "sportage",
    "sportage": "sportage",
    "new kia sportage": "new_sportage",
    "new sportage": "new_sportage",
    "новый sportage": "new_sportage",
    "новый kia sportage": "new_sportage",
    "kia ceed": "ceed",
    "ceed": "ceed",
    "kia ceed sw": "ceed_sw",
    "ceed sw": "ceed_sw",
    "kia cerato": "cerato",
    "cerato": "cerato",
    "kia k5": "newk5",
    "k5": "newk5",
    "kia k8": "k8",
    "k8": "k8",
    "new kia k8": "k8",
    "new k8": "k8",
    "новый kia k8": "k8",
    "новый k8": "k8",
    "kia k9": "k9",
    "k9": "k9",
    "kia ev9": "ev9",
    "ev9": "ev9",
    "kia sorento": "sorento",
    "sorento": "sorento",
    "kia soul": "soul",
    "soul": "soul",
    "kia carnival": "carnivalnew",
    "carnival": "carnivalnew",
    "kia seltos": "seltos",
    "seltos": "seltos",
    "kia soluto": "soluto",
    "soluto": "soluto",
    "new kia soluto": "soluto",
    "new soluto": "soluto",
    "новый kia soluto": "soluto",
    "новый soluto": "soluto",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json_file(path: Path) -> dict[str, object] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.exception("Could not read cache file %s", path)
        return None


def load_initial_cache() -> None:
    global _cache, _last_success_epoch

    payload = read_json_file(CACHE_PATH) or read_json_file(SEED_PATH)
    if not payload:
        return

    with _state_lock:
        _cache = payload
        _last_success_epoch = time.time()

    LOGGER.info("Loaded initial cache with %s models", payload.get("count"))


def cache_age_seconds() -> float | None:
    with _state_lock:
        if not _cache or not _last_success_epoch:
            return None
        return max(0.0, time.time() - _last_success_epoch)


def is_cache_stale() -> bool:
    age = cache_age_seconds()
    return age is None or age >= REFRESH_INTERVAL_SECONDS


def normalize_query(value: str) -> str:
    return " ".join(value.strip().lower().split())


def resolve_model_from_cache(raw_name: str) -> dict[str, object] | None:
    normalized = normalize_query(raw_name)
    target_slug = MODEL_ALIASES.get(normalized, normalized.replace(" ", "_"))

    with _state_lock:
        models = list((_cache or {}).get("models", []))

    for model in models:
        slug = str(model.get("slug", "")).lower()
        name = normalize_query(str(model.get("name", "")))
        if slug == target_slug or name == normalized:
            return model

    for model in models:
        slug = str(model.get("slug", "")).lower()
        name = normalize_query(str(model.get("name", "")))
        if normalized and (normalized in name or normalized in slug):
            return model

    return None


def build_model_message(model: dict[str, object]) -> str:
    lines = [
        f"Kia {model.get('name', '')}".strip(),
        f"Цена: {model.get('price') or 'пока недоступна'}",
        f"Прайс-лист: {model.get('price_list_url') or 'пока недоступен'}",
        f"Брошюра: {model.get('brochure_url') or 'пока недоступна'}",
        f"Ссылка на модель: {model.get('model_url') or 'пока недоступна'}",
    ]
    return "\n".join(lines)


def build_model_response(model: dict[str, object]) -> dict[str, object]:
    response = {
        "status": "success",
        "name": model.get("name", ""),
        "slug": model.get("slug", ""),
        "price": model.get("price", ""),
        "previous_price": model.get("previous_price", ""),
        "model_url": model.get("model_url", ""),
        "options_url": model.get("options_url", ""),
        "price_list_url": model.get("price_list_url", ""),
        "brochure_url": model.get("brochure_url", ""),
        "errors": model.get("errors", []),
        "message": build_model_message(model),
        "model": model,
    }
    return response


def refresh_cache() -> None:
    global _cache, _last_error, _last_success_epoch, _refreshing

    with _refresh_condition:
        if _refreshing:
            while _refreshing:
                _refresh_condition.wait(timeout=1)
            return
        _refreshing = True

    try:
        LOGGER.info("Refreshing Kia models cache")
        payload = scrape_once(CACHE_PATH, SCRAPE_TIMEOUT_SECONDS)
        payload["served_by"] = "render-web-service"
        payload["refreshed_at_utc"] = utc_now()

        with _state_lock:
            _cache = payload
            _last_success_epoch = time.time()
            _last_error = ""

        LOGGER.info("Refreshed Kia models cache with %s models", payload.get("count"))
    except Exception as exc:
        LOGGER.exception("Kia models refresh failed")
        with _state_lock:
            _last_error = str(exc)
    finally:
        with _refresh_condition:
            _refreshing = False
            _refresh_condition.notify_all()


def trigger_refresh_if_needed(force: bool = False) -> bool:
    if not force and not is_cache_stale():
        return False

    with _state_lock:
        if _refreshing:
            return False

    thread = threading.Thread(target=refresh_cache, name="kia-models-refresh", daemon=True)
    thread.start()
    return True


def refresh_loop() -> None:
    while True:
        trigger_refresh_if_needed()
        time.sleep(30)


def status_payload() -> dict[str, object]:
    with _state_lock:
        count = _cache.get("count") if _cache else 0
        fetched_at = _cache.get("fetched_at_utc") if _cache else ""
        return {
            "ok": bool(_cache) and not _last_error,
            "has_cache": bool(_cache),
            "count": count,
            "fetched_at_utc": fetched_at,
            "cache_age_seconds": cache_age_seconds(),
            "refresh_interval_seconds": REFRESH_INTERVAL_SECONDS,
            "refreshing": _refreshing,
            "last_error": _last_error,
        }


class KiaModelsHandler(BaseHTTPRequestHandler):
    server_version = "KiaModelsHTTP/1.0"

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_cors_headers()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)
        route = parsed_url.path.rstrip("/") or "/"
        query = parse_qs(parsed_url.query)

        if route in {"/", "/healthz"}:
            self.write_json(status_payload())
            return

        if route in {"/models", "/api/models", "/kia_models.json"}:
            wait_for_refresh = query.get("refresh") == ["1"] or query.get("wait") == ["1"]
            if wait_for_refresh:
                refresh_cache()
            else:
                trigger_refresh_if_needed()

            with _state_lock:
                payload = _cache
                last_error = _last_error

            if payload:
                self.write_json(payload)
            else:
                self.write_json(
                    {
                        "error": "Kia models cache is not ready yet",
                        "refreshing": True,
                        "last_error": last_error,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
            return

        if route in {"/model", "/api/model"}:
            model_name = query.get("name", [""])[0]
            if query.get("refresh") == ["1"]:
                refresh_cache()
            else:
                trigger_refresh_if_needed()

            if not model_name:
                self.write_json({"error": "Missing required query parameter: name"}, status=HTTPStatus.BAD_REQUEST)
                return

            model = resolve_model_from_cache(model_name)
            if model:
                self.write_json(build_model_response(model))
            else:
                self.write_json(
                    {
                        "status": "error",
                        "error": "Model not found",
                        "model_name_normalized": normalize_query(model_name),
                    },
                    status=HTTPStatus.NOT_FOUND,
                )
            return

        if route == "/refresh":
            wait_for_refresh = query.get("wait") == ["1"]
            if wait_for_refresh:
                refresh_cache()
                self.write_json(status_payload())
            else:
                started = trigger_refresh_if_needed(force=True)
                response = status_payload()
                response["refresh_started"] = started
                self.write_json(response, status=HTTPStatus.ACCEPTED)
            return

        self.write_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed_url = urlparse(self.path)
        route = parsed_url.path.rstrip("/") or "/"

        if route in {"/model", "/api/model"}:
            body = self.read_json_body()
            model_name = str(body.get("model_name") or body.get("name") or "")
            refresh = bool(body.get("refresh"))

            if refresh:
                refresh_cache()
            else:
                trigger_refresh_if_needed()

            if not model_name:
                self.write_json({"error": "Missing required JSON field: model_name"}, status=HTTPStatus.BAD_REQUEST)
                return

            model = resolve_model_from_cache(model_name)
            if model:
                self.write_json(build_model_response(model))
            else:
                self.write_json(
                    {
                        "status": "error",
                        "error": "Model not found",
                        "model_name_normalized": normalize_query(model_name),
                    },
                    status=HTTPStatus.NOT_FOUND,
                )
            return

        self.write_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length <= 0:
            return {}

        try:
            raw_body = self.rfile.read(content_length)
            body = json.loads(raw_body.decode("utf-8"))
            return body if isinstance(body, dict) else {}
        except Exception:
            LOGGER.exception("Could not parse JSON request body")
            return {}

    def log_message(self, format: str, *args: object) -> None:
        LOGGER.info("%s - %s", self.address_string(), format % args)

    def send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")

    def write_json(self, payload: dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_initial_cache()
    trigger_refresh_if_needed(force=True)

    refresher = threading.Thread(target=refresh_loop, name="kia-models-refresh-loop", daemon=True)
    refresher.start()

    port = int(os.getenv("PORT", str(DEFAULT_PORT)))
    server = ThreadingHTTPServer(("0.0.0.0", port), KiaModelsHandler)
    LOGGER.info("Serving Kia models API on port %s", port)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
