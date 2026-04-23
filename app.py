from __future__ import annotations

import json
import logging
import os
import hashlib
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from kia_models_parser import DEFAULT_OUTPUT_PATH, scrape_once, write_json_atomic


LOGGER = logging.getLogger("kia_models_service")

DEFAULT_PORT = 8000
DEFAULT_REFRESH_INTERVAL_SECONDS = 60 * 60
DEFAULT_SCRAPE_TIMEOUT_SECONDS = 30

IS_RENDER = os.getenv("RENDER") == "true"
DEFAULT_CACHE_PATH = Path("/tmp/kia_models.json") if IS_RENDER else DEFAULT_OUTPUT_PATH

CACHE_PATH = Path(os.getenv("KIA_MODELS_OUTPUT", str(DEFAULT_CACHE_PATH)))
SOURCE_CACHE_PATH = Path(os.getenv("KIA_MODELS_SOURCE_OUTPUT", str(CACHE_PATH.with_name("kia_models_source.json"))))
SEED_PATH = Path(os.getenv("KIA_MODELS_SEED_PATH", str(DEFAULT_OUTPUT_PATH)))
OVERRIDES_PATH = Path(os.getenv("KIA_MODEL_OVERRIDES_PATH", str(CACHE_PATH.with_name("kia_model_overrides.json"))))
PENDING_CHANGES_PATH = Path(os.getenv("KIA_PENDING_CHANGES_PATH", str(CACHE_PATH.with_name("kia_pending_changes.json"))))
REFRESH_INTERVAL_SECONDS = int(os.getenv("REFRESH_INTERVAL_SECONDS", str(DEFAULT_REFRESH_INTERVAL_SECONDS)))
SCRAPE_TIMEOUT_SECONDS = int(os.getenv("SCRAPE_TIMEOUT_SECONDS", str(DEFAULT_SCRAPE_TIMEOUT_SECONDS)))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
KIA_LOGS_CHAT_ID = os.getenv("KIA_LOGS_CHAT_ID", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
CHANGE_FIELDS = {"price", "price_list_url", "brochure_url", "model_url", "options_url"}
WATCHED_FIELDS = ("price", "price_list_url", "brochure_url")
BUTTON_EDIT_FIELDS = ("price", "price_list_url", "brochure_url")
MODELS_PER_PAGE = 4

_state_lock = threading.RLock()
_refresh_condition = threading.Condition(_state_lock)
_cache: dict[str, object] | None = None
_source_cache: dict[str, object] | None = None
_overrides: dict[str, object] = {"models": {}}
_pending_changes: dict[str, dict[str, object]] = {}
_pending_edits: dict[str, dict[str, object]] = {}
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


def write_json_file(path: Path, payload: dict[str, object]) -> None:
    write_json_atomic(payload, path)


def load_overrides() -> None:
    global _overrides

    payload = read_json_file(OVERRIDES_PATH)
    if not payload:
        _overrides = {"models": {}}
        return

    if not isinstance(payload.get("models"), dict):
        payload["models"] = {}

    _overrides = payload


def load_pending_changes() -> None:
    global _pending_changes

    payload = read_json_file(PENDING_CHANGES_PATH)
    if not payload:
        _pending_changes = {}
        return

    changes = payload.get("changes", {})
    _pending_changes = changes if isinstance(changes, dict) else {}


def save_overrides() -> None:
    _overrides["updated_at_utc"] = utc_now()
    write_json_file(OVERRIDES_PATH, _overrides)


def save_pending_changes() -> None:
    write_json_file(
        PENDING_CHANGES_PATH,
        {
            "updated_at_utc": utc_now(),
            "changes": _pending_changes,
        },
    )


def models_by_slug(payload: dict[str, object] | None) -> dict[str, dict[str, object]]:
    if not payload:
        return {}

    result: dict[str, dict[str, object]] = {}
    for model in payload.get("models", []):
        if not isinstance(model, dict):
            continue
        slug = str(model.get("slug", ""))
        if slug:
            result[slug] = model
    return result


def apply_overrides(payload: dict[str, object]) -> dict[str, object]:
    merged = json.loads(json.dumps(payload, ensure_ascii=False))
    override_models = _overrides.get("models", {})
    if not isinstance(override_models, dict):
        return merged

    for model in merged.get("models", []):
        if not isinstance(model, dict):
            continue

        slug = str(model.get("slug", ""))
        model_override = override_models.get(slug)
        if not isinstance(model_override, dict):
            continue

        changed_fields: list[str] = []
        for field_name, field_value in model_override.items():
            if field_name.startswith("_"):
                continue
            model[field_name] = field_value
            changed_fields.append(field_name)

        if changed_fields:
            model["manual_override"] = True
            model["manual_override_fields"] = sorted(changed_fields)
            model["manual_override_updated_at_utc"] = model_override.get("_updated_at_utc", "")
            model["manual_override_updated_by"] = model_override.get("_updated_by", "")

    merged["manual_overrides_applied"] = bool(override_models)
    return merged


def load_initial_cache() -> None:
    global _cache, _last_success_epoch, _source_cache

    load_overrides()
    load_pending_changes()
    payload = read_json_file(SOURCE_CACHE_PATH) or read_json_file(CACHE_PATH) or read_json_file(SEED_PATH)
    if not payload:
        return

    effective_payload = apply_overrides(payload)
    with _state_lock:
        _source_cache = payload
        _cache = effective_payload
        _last_success_epoch = time.time()

    LOGGER.info("Loaded initial cache with %s models", effective_payload.get("count"))


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


def telegram_api(method: str, payload: dict[str, object]) -> dict[str, object]:
    if not TELEGRAM_BOT_TOKEN:
        LOGGER.warning("Telegram bot token is not configured")
        return {"ok": False, "description": "TELEGRAM_BOT_TOKEN is not configured"}

    request = Request(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def send_telegram_message(
    chat_id: str,
    text: str,
    reply_markup: dict[str, object] | None = None,
) -> None:
    if not chat_id:
        LOGGER.warning("Telegram chat id is not configured")
        return

    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        telegram_api("sendMessage", payload)
    except Exception:
        LOGGER.exception("Could not send Telegram message")


def edit_telegram_message(
    chat_id: str,
    message_id: str,
    text: str,
    reply_markup: dict[str, object] | None = None,
) -> None:
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        telegram_api("editMessageText", payload)
    except Exception:
        LOGGER.exception("Could not edit Telegram message")


def answer_callback_query(callback_query_id: str, text: str = "") -> None:
    if not callback_query_id:
        return

    payload: dict[str, object] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text

    try:
        telegram_api("answerCallbackQuery", payload)
    except Exception:
        LOGGER.exception("Could not answer Telegram callback query")


def send_kia_log(text: str) -> None:
    if KIA_LOGS_CHAT_ID:
        send_telegram_message(KIA_LOGS_CHAT_ID, text)
    else:
        LOGGER.warning("KIA_LOGS_CHAT_ID is not configured. Log message: %s", text)


def website_change_keyboard(change_id_value: str, slug: str) -> dict[str, object]:
    return {
        "inline_keyboard": [
            [
                {"text": "Accept new value", "callback_data": f"change:accept:{change_id_value}"},
                {"text": "Decline / keep old", "callback_data": f"change:decline:{change_id_value}"},
            ],
            [{"text": "Show model", "callback_data": f"model:{slug}:0"}],
        ]
    }


def format_single_change_message(change: dict[str, str]) -> str:
    return "\n".join(
        [
            "Kia website change detected.",
            "",
            f"Model: {change['model_name']} ({change['slug']})",
            f"Field: {change['field']}",
            f"Old value: {change.get('old_value') or '-'}",
            f"New website value: {change.get('new_value') or '-'}",
            "",
            "Choose what to do:",
            "- Accept: use the new website value.",
            "- Decline: keep the old value as a manual override.",
        ]
    )


def send_kia_change_logs(changes: list[dict[str, str]]) -> None:
    if not KIA_LOGS_CHAT_ID:
        LOGGER.warning("KIA_LOGS_CHAT_ID is not configured. Changes: %s", changes)
        return

    for change in changes:
        pending_id = queue_pending_change(change)
        send_telegram_message(
            KIA_LOGS_CHAT_ID,
            format_single_change_message(change),
            website_change_keyboard(pending_id, change["slug"]),
        )


def format_change_message(changes: list[dict[str, str]]) -> str:
    lines = ["Kia parser detected website changes:"]
    for change in changes:
        lines.extend(
            [
                "",
                f"Model: {change['model_name']} ({change['slug']})",
                f"Field: {change['field']}",
                f"Old: {change['old_value'] or '-'}",
                f"New: {change['new_value'] or '-'}",
            ]
        )
    lines.extend(
        [
            "",
            "To override a field manually:",
            "/change <slug> <field> <value>",
            "Example: /change sorento price от 17 000 000 ₸",
        ]
    )
    return "\n".join(lines)


def detect_website_changes(
    old_payload: dict[str, object] | None,
    new_payload: dict[str, object],
) -> list[dict[str, str]]:
    old_models = models_by_slug(old_payload)
    new_models = models_by_slug(new_payload)
    changes: list[dict[str, str]] = []

    for slug, new_model in new_models.items():
        old_model = old_models.get(slug)
        if not old_model:
            continue

        for field_name in WATCHED_FIELDS:
            old_value = str(old_model.get(field_name, "") or "")
            new_value = str(new_model.get(field_name, "") or "")
            if old_value == new_value:
                continue

            changes.append(
                {
                    "slug": slug,
                    "model_name": str(new_model.get("name", slug)),
                    "field": field_name,
                    "old_value": old_value,
                    "new_value": new_value,
                }
            )

    return changes


def ensure_model_override(slug: str) -> dict[str, object]:
    override_models = _overrides.setdefault("models", {})
    if not isinstance(override_models, dict):
        override_models = {}
        _overrides["models"] = override_models

    model_override = override_models.setdefault(slug, {})
    if not isinstance(model_override, dict):
        model_override = {}
        override_models[slug] = model_override

    return model_override


def refresh_effective_cache_from_source() -> None:
    global _cache

    with _state_lock:
        if _source_cache:
            _cache = apply_overrides(_source_cache)
            write_json_file(CACHE_PATH, _cache)


def set_manual_override(model: dict[str, object], field_name: str, value: str, updated_by: str) -> dict[str, object]:
    slug = str(model.get("slug", ""))
    if not slug:
        raise ValueError("Model has no slug")
    if field_name not in CHANGE_FIELDS:
        raise ValueError("Unsupported field")

    model_override = ensure_model_override(slug)

    model_override[field_name] = value
    model_override["_updated_at_utc"] = utc_now()
    model_override["_updated_by"] = updated_by

    save_overrides()
    refresh_effective_cache_from_source()

    return resolve_model_from_cache(slug) or model


def set_manual_override_by_slug(slug: str, field_name: str, value: str, updated_by: str) -> None:
    if field_name not in CHANGE_FIELDS:
        raise ValueError("Unsupported field")

    model_override = ensure_model_override(slug)
    model_override[field_name] = value
    model_override["_updated_at_utc"] = utc_now()
    model_override["_updated_by"] = updated_by
    save_overrides()
    refresh_effective_cache_from_source()


def remove_manual_override(slug: str, field_name: str) -> None:
    override_models = _overrides.get("models", {})
    if not isinstance(override_models, dict):
        return

    model_override = override_models.get(slug)
    if not isinstance(model_override, dict):
        return

    model_override.pop(field_name, None)
    remaining_fields = [key for key in model_override if not key.startswith("_")]
    if remaining_fields:
        model_override["_updated_at_utc"] = utc_now()
        model_override["_updated_by"] = "telegram-accept"
    else:
        override_models.pop(slug, None)

    save_overrides()
    refresh_effective_cache_from_source()


def change_id(change: dict[str, str]) -> str:
    raw = "|".join(
        [
            change.get("slug", ""),
            change.get("field", ""),
            change.get("old_value", ""),
            change.get("new_value", ""),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def queue_pending_change(change: dict[str, str]) -> str:
    queued_change = dict(change)
    queued_change["id"] = change_id(change)
    queued_change["status"] = "pending"
    queued_change["created_at_utc"] = utc_now()
    _pending_changes[queued_change["id"]] = queued_change
    save_pending_changes()
    return queued_change["id"]


def freeze_changes_until_approval(changes: list[dict[str, str]]) -> None:
    for change in changes:
        set_manual_override_by_slug(
            change["slug"],
            change["field"],
            change.get("old_value", ""),
            "pending-website-change",
        )


def telegram_user_name(message: dict[str, object]) -> str:
    user = message.get("from", {})
    if not isinstance(user, dict):
        return "telegram"

    username = str(user.get("username", "") or "")
    if username:
        return "@" + username

    full_name = " ".join(
        part for part in [str(user.get("first_name", "") or ""), str(user.get("last_name", "") or "")] if part
    )
    return full_name or str(user.get("id", "") or "telegram")


def telegram_chat_id(message: dict[str, object]) -> str:
    chat = message.get("chat", {})
    if isinstance(chat, dict):
        return str(chat.get("id", "") or "")
    return ""


def telegram_user_id(message: dict[str, object]) -> str:
    user = message.get("from", {})
    if isinstance(user, dict):
        return str(user.get("id", "") or "")
    return ""


def telegram_message_text(update: dict[str, object]) -> tuple[dict[str, object], str]:
    message = update.get("message") or update.get("edited_message") or {}
    if not isinstance(message, dict):
        return {}, ""
    return message, str(message.get("text", "") or "").strip()


def allowed_telegram_chat(chat_id: str) -> bool:
    return bool(KIA_LOGS_CHAT_ID and chat_id == KIA_LOGS_CHAT_ID)


def command_help_text() -> str:
    return "\n".join(
        [
            "Kia logs commands:",
            "/chatid - show this Telegram chat id",
            "/models - open model buttons",
            "/get <model-or-slug> - show current model data",
            "/change <model-or-slug> <field> <value> - set manual override",
            "/cancel - cancel pending button edit",
            "",
            "Fields:",
            ", ".join(BUTTON_EDIT_FIELDS),
            "",
            "Examples:",
            "/change sorento price от 17 000 000 ₸",
            "/change sorento price_list_url https://example.com/price.pdf",
        ]
    )


def model_slugs_text() -> str:
    with _state_lock:
        models = list((_cache or {}).get("models", []))

    lines = ["Kia model slugs:"]
    for model in models:
        if isinstance(model, dict):
            lines.append(f"- {model.get('slug')}: {model.get('name')}")
    return "\n".join(lines)


def get_cached_models() -> list[dict[str, object]]:
    with _state_lock:
        models = list((_cache or {}).get("models", []))
    return [model for model in models if isinstance(model, dict)]


def clamp_page(page: int, total_pages: int) -> int:
    if total_pages <= 0:
        return 0
    return min(max(page, 0), total_pages - 1)


def models_page_text(page: int, total_pages: int) -> str:
    return "\n".join(
        [
            "Choose a Kia model to edit.",
            f"Page {page + 1} of {max(total_pages, 1)}",
        ]
    )


def models_page_keyboard(page: int) -> tuple[str, dict[str, object]]:
    models = get_cached_models()
    total_pages = max(1, (len(models) + MODELS_PER_PAGE - 1) // MODELS_PER_PAGE)
    page = clamp_page(page, total_pages)
    start = page * MODELS_PER_PAGE
    page_models = models[start : start + MODELS_PER_PAGE]

    keyboard: list[list[dict[str, str]]] = []
    for model in page_models:
        name = str(model.get("name", model.get("slug", "")))
        slug = str(model.get("slug", ""))
        keyboard.append([{"text": name, "callback_data": f"model:{slug}:{page}"}])

    keyboard.append(
        [
            {
                "text": "Previous",
                "callback_data": f"models:{page - 1}" if page > 0 else "noop",
            },
            {
                "text": "Next",
                "callback_data": f"models:{page + 1}" if page + 1 < total_pages else "noop",
            },
        ]
    )
    return models_page_text(page, total_pages), {"inline_keyboard": keyboard}


def field_label(field_name: str) -> str:
    labels = {
        "price": "Price",
        "price_list_url": "Price-list PDF",
        "brochure_url": "Brochure PDF",
    }
    return labels.get(field_name, field_name)


def field_prompt(field_name: str) -> str:
    if field_name == "price":
        return "Reply with the new price, for example: от 17 000 000 ₸"
    return "Reply with the new PDF link."


def model_fields_keyboard(slug: str, page: int = 0) -> dict[str, object]:
    keyboard = [
        [{"text": field_label(field_name), "callback_data": f"field:{slug}:{field_name}"}]
        for field_name in BUTTON_EDIT_FIELDS
    ]
    keyboard.append([{"text": "Back to models", "callback_data": f"models:{page}"}])
    return {"inline_keyboard": keyboard}


def pending_key(chat_id: str, user_id: str) -> str:
    return f"{chat_id}:{user_id}"


def set_pending_edit(chat_id: str, user_id: str, slug: str, field_name: str) -> None:
    _pending_edits[pending_key(chat_id, user_id)] = {
        "chat_id": chat_id,
        "user_id": user_id,
        "slug": slug,
        "field": field_name,
        "created_at": time.time(),
    }


def pop_pending_edit(chat_id: str, user_id: str) -> dict[str, object] | None:
    return _pending_edits.pop(pending_key(chat_id, user_id), None)


def get_pending_edit(chat_id: str, user_id: str) -> dict[str, object] | None:
    return _pending_edits.get(pending_key(chat_id, user_id))


def send_models_page(chat_id: str, page: int = 0) -> None:
    text, keyboard = models_page_keyboard(page)
    send_telegram_message(chat_id, text, keyboard)


def edit_models_page(chat_id: str, message_id: str, page: int = 0) -> None:
    text, keyboard = models_page_keyboard(page)
    edit_telegram_message(chat_id, message_id, text, keyboard)


def edit_model_field_selector(chat_id: str, message_id: str, slug: str, page: int = 0) -> None:
    model = resolve_model_from_cache(slug)
    if not model:
        edit_telegram_message(chat_id, message_id, f"Model not found: {slug}")
        return

    text = "\n".join(
        [
            f"Model: {model.get('name')} ({model.get('slug')})",
            "",
            build_model_message(model),
            "",
            "What do you want to change?",
        ]
    )
    edit_telegram_message(chat_id, message_id, text, model_fields_keyboard(slug, page))


def handle_change_command(args_text: str, message: dict[str, object]) -> str:
    parts = args_text.split(maxsplit=2)
    if len(parts) < 3:
        return "Usage: /change <model-or-slug> <field> <value>"

    model_name, field_name, value = parts
    field_name = field_name.strip()
    value = value.strip()

    if field_name not in CHANGE_FIELDS:
        return "Unsupported field. Allowed fields: " + ", ".join(sorted(CHANGE_FIELDS))

    model = resolve_model_from_cache(model_name)
    if not model:
        return f"Model not found: {model_name}. Use /models to see available slugs."

    updated_model = set_manual_override(model, field_name, value, telegram_user_name(message))
    return "\n".join(
        [
            "Manual override saved.",
            f"Model: {updated_model.get('name')} ({updated_model.get('slug')})",
            f"Field: {field_name}",
            f"Value: {value}",
        ]
    )


def handle_get_command(args_text: str) -> str:
    if not args_text:
        return "Usage: /get <model-or-slug>"

    model = resolve_model_from_cache(args_text)
    if not model:
        return f"Model not found: {args_text}"

    return build_model_message(model)


def handle_pending_edit_message(message: dict[str, object], text: str) -> bool:
    chat_id = telegram_chat_id(message)
    user_id = telegram_user_id(message)
    if not chat_id or not user_id:
        return False

    pending = get_pending_edit(chat_id, user_id)
    if not pending:
        return False

    if text.lower() == "/cancel":
        pop_pending_edit(chat_id, user_id)
        send_telegram_message(chat_id, "Canceled the pending Kia model edit.")
        return True

    value = text.strip()
    if not value:
        send_telegram_message(chat_id, "Value is empty. Reply again or send /cancel.")
        return True

    slug = str(pending.get("slug", ""))
    field_name = str(pending.get("field", ""))
    model = resolve_model_from_cache(slug)
    if not model:
        pop_pending_edit(chat_id, user_id)
        send_telegram_message(chat_id, f"Model no longer found: {slug}")
        return True

    old_value = str(model.get(field_name, "") or "")
    updated_model = set_manual_override(model, field_name, value, telegram_user_name(message))
    pop_pending_edit(chat_id, user_id)

    response = "\n".join(
        [
            "Model updated.",
            f"Model: {updated_model.get('name')} ({updated_model.get('slug')})",
            f"Changed field: {field_name}",
            f"Old value: {old_value or '-'}",
            f"New value: {value}",
            "",
            build_model_message(updated_model),
        ]
    )
    send_telegram_message(chat_id, response, model_fields_keyboard(str(updated_model.get("slug", ""))))
    return True


def callback_chat_id(callback: dict[str, object]) -> str:
    message = callback.get("message", {})
    if isinstance(message, dict):
        return telegram_chat_id(message)
    return ""


def callback_message_id(callback: dict[str, object]) -> str:
    message = callback.get("message", {})
    if isinstance(message, dict):
        return str(message.get("message_id", "") or "")
    return ""


def callback_user_id(callback: dict[str, object]) -> str:
    user = callback.get("from", {})
    if isinstance(user, dict):
        return str(user.get("id", "") or "")
    return ""


def callback_user_name(callback: dict[str, object]) -> str:
    user = callback.get("from", {})
    if not isinstance(user, dict):
        return "there"
    username = str(user.get("username", "") or "")
    if username:
        return "@" + username
    return str(user.get("first_name", "") or "there")


def format_change_decision_message(change: dict[str, object], action: str, actor: str) -> str:
    status_text = "Accepted new website value" if action == "accept" else "Declined; kept old value"
    value_line = (
        f"Active value: {change.get('new_value') or '-'}"
        if action == "accept"
        else f"Active value: {change.get('old_value') or '-'}"
    )
    return "\n".join(
        [
            f"{status_text}.",
            "",
            f"Model: {change.get('model_name')} ({change.get('slug')})",
            f"Field: {change.get('field')}",
            f"Old value: {change.get('old_value') or '-'}",
            f"New website value: {change.get('new_value') or '-'}",
            value_line,
            f"Changed by: {actor}",
        ]
    )


def handle_website_change_decision(
    action: str,
    change_id_value: str,
    callback: dict[str, object],
    chat_id: str,
    message_id: str,
    callback_id: str,
) -> None:
    change = _pending_changes.get(change_id_value)
    if not change:
        answer_callback_query(callback_id, "This change is no longer pending.")
        edit_telegram_message(chat_id, message_id, "This website change is no longer pending.")
        return

    if change.get("status") != "pending":
        answer_callback_query(callback_id, f"Already {change.get('status')}.")
        edit_telegram_message(
            chat_id,
            message_id,
            format_change_decision_message(change, str(change.get("status")), str(change.get("decided_by", ""))),
        )
        return

    slug = str(change.get("slug", ""))
    field_name = str(change.get("field", ""))
    actor = callback_user_name(callback)

    if action == "accept":
        remove_manual_override(slug, field_name)
        change["status"] = "accept"
        callback_notice = "Accepted new website value."
    elif action == "decline":
        set_manual_override_by_slug(slug, field_name, str(change.get("old_value", "") or ""), actor)
        change["status"] = "decline"
        callback_notice = "Declined. Old value kept."
    else:
        answer_callback_query(callback_id, "Unknown action.")
        return

    change["decided_at_utc"] = utc_now()
    change["decided_by"] = actor
    save_pending_changes()

    edit_telegram_message(chat_id, message_id, format_change_decision_message(change, action, actor))
    answer_callback_query(callback_id, callback_notice)


def handle_telegram_callback(callback: dict[str, object]) -> None:
    callback_id = str(callback.get("id", "") or "")
    data = str(callback.get("data", "") or "")
    chat_id = callback_chat_id(callback)
    message_id = callback_message_id(callback)
    user_id = callback_user_id(callback)

    if data == "noop":
        answer_callback_query(callback_id)
        return

    if not allowed_telegram_chat(chat_id):
        answer_callback_query(callback_id, "This chat is not allowed.")
        return

    if data.startswith("change:"):
        parts = data.split(":", 2)
        if len(parts) < 3:
            answer_callback_query(callback_id, "Invalid change button.")
            return
        handle_website_change_decision(parts[1], parts[2], callback, chat_id, message_id, callback_id)
        return

    if data.startswith("models:"):
        page_text = data.split(":", 1)[1]
        page = int(page_text) if page_text.isdigit() or page_text.startswith("-") else 0
        edit_models_page(chat_id, message_id, page)
        answer_callback_query(callback_id)
        return

    if data.startswith("model:"):
        parts = data.split(":")
        slug = parts[1] if len(parts) > 1 else ""
        page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        edit_model_field_selector(chat_id, message_id, slug, page)
        answer_callback_query(callback_id)
        return

    if data.startswith("field:"):
        parts = data.split(":", 2)
        if len(parts) < 3:
            answer_callback_query(callback_id, "Invalid button.")
            return

        slug = parts[1]
        field_name = parts[2]
        model = resolve_model_from_cache(slug)
        if not model or field_name not in BUTTON_EDIT_FIELDS:
            answer_callback_query(callback_id, "Invalid model or field.")
            return

        set_pending_edit(chat_id, user_id, slug, field_name)
        prompt = "\n".join(
            [
                f"{callback_user_name(callback)}, editing {model.get('name')} -> {field_label(field_name)}.",
                field_prompt(field_name),
                "",
                "Reply to this message with the new value, or send /cancel.",
            ]
        )
        send_telegram_message(
            chat_id,
            prompt,
            {
                "force_reply": True,
                "selective": True,
                "input_field_placeholder": "New price or PDF link",
            },
        )
        answer_callback_query(callback_id, "Waiting for your reply.")
        return

    answer_callback_query(callback_id, "Unknown button.")


def handle_telegram_update(update: dict[str, object]) -> None:
    callback = update.get("callback_query")
    if isinstance(callback, dict):
        handle_telegram_callback(callback)
        return

    message, text = telegram_message_text(update)
    if not message or not text:
        return

    chat_id = telegram_chat_id(message)
    if handle_pending_edit_message(message, text):
        return

    command = text.split(maxsplit=1)[0].split("@", 1)[0].lower()
    args_text = text.split(maxsplit=1)[1] if len(text.split(maxsplit=1)) > 1 else ""

    if command == "/chatid":
        send_telegram_message(chat_id, f"Chat id: {chat_id}")
        return

    if not allowed_telegram_chat(chat_id):
        send_telegram_message(
            chat_id,
            "This chat is not allowed for Kia model changes. Set KIA_LOGS_CHAT_ID to this chat id to enable it.",
        )
        return

    if command in {"/help", "/start"}:
        send_telegram_message(chat_id, command_help_text())
    elif command == "/models":
        send_models_page(chat_id, 0)
    elif command == "/get":
        send_telegram_message(chat_id, handle_get_command(args_text))
    elif command == "/change":
        send_telegram_message(chat_id, handle_change_command(args_text, message))
    elif command == "/cancel":
        send_telegram_message(chat_id, "There is no pending Kia model edit for you.")
    else:
        send_telegram_message(chat_id, "Unknown command. Use /help.")


def refresh_cache() -> None:
    global _cache, _source_cache, _last_error, _last_success_epoch, _refreshing

    with _refresh_condition:
        if _refreshing:
            while _refreshing:
                _refresh_condition.wait(timeout=1)
            return
        _refreshing = True

    try:
        LOGGER.info("Refreshing Kia models cache")
        with _state_lock:
            previous_source_cache = _source_cache

        source_payload = scrape_once(CACHE_PATH, SCRAPE_TIMEOUT_SECONDS)
        source_payload["served_by"] = "render-web-service"
        source_payload["refreshed_at_utc"] = utc_now()
        changes = detect_website_changes(previous_source_cache, source_payload)
        write_json_file(SOURCE_CACHE_PATH, source_payload)
        if changes:
            freeze_changes_until_approval(changes)

        effective_payload = apply_overrides(source_payload)
        write_json_file(CACHE_PATH, effective_payload)

        with _state_lock:
            _source_cache = source_payload
            _cache = effective_payload
            _last_success_epoch = time.time()
            _last_error = ""

        if changes:
            send_kia_change_logs(changes)

        LOGGER.info("Refreshed Kia models cache with %s models", effective_payload.get("count"))
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
        override_models = _overrides.get("models", {})
        return {
            "ok": bool(_cache) and not _last_error,
            "has_cache": bool(_cache),
            "count": count,
            "fetched_at_utc": fetched_at,
            "cache_age_seconds": cache_age_seconds(),
            "refresh_interval_seconds": REFRESH_INTERVAL_SECONDS,
            "refreshing": _refreshing,
            "last_error": _last_error,
            "telegram_configured": bool(TELEGRAM_BOT_TOKEN and KIA_LOGS_CHAT_ID),
            "manual_override_count": len(override_models) if isinstance(override_models, dict) else 0,
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

        if route == "/telegram/webhook" or route.startswith("/telegram/webhook/"):
            if TELEGRAM_WEBHOOK_SECRET:
                received_secret = route.removeprefix("/telegram/webhook/") if route.startswith("/telegram/webhook/") else ""
                if received_secret != TELEGRAM_WEBHOOK_SECRET:
                    self.write_json({"ok": False, "error": "Forbidden"}, status=HTTPStatus.FORBIDDEN)
                    return

            update = self.read_json_body()
            handle_telegram_update(update)
            self.write_json({"ok": True})
            return

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
