from __future__ import annotations

from typing import Any
from flask import jsonify, request


def body() -> dict[str, Any]:
    return request.get_json(silent=True) or {}


def ok(data: Any = None, message: str | None = None, meta: dict[str, Any] | None = None, status_code: int = 200):
    payload: dict[str, Any] = {"success": True, "data": data if data is not None else {}}
    if message:
        payload["message"] = message
    if meta:
        payload["meta"] = meta
    return jsonify(payload), status_code


def fail(message: str, status_code: int = 400, *, error_code: str | None = None, details: dict[str, Any] | None = None):
    payload: dict[str, Any] = {"success": False, "message": message}
    if error_code:
        payload["error_code"] = error_code
    if details:
        payload["details"] = details
    return jsonify(payload), status_code


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def parse_int(value: Any, default: int = 0, *, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    if minimum is not None:
        out = max(minimum, out)
    if maximum is not None:
        out = min(maximum, out)
    return out


def pagination_args(default_page: int = 1, default_page_size: int = 25, max_page_size: int = 200) -> tuple[int, int]:
    page = parse_int(request.args.get("page"), default_page, minimum=1)
    page_size = parse_int(request.args.get("page_size"), default_page_size, minimum=1, maximum=max_page_size)
    return page, page_size
