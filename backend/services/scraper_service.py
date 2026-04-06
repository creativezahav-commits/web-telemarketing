from __future__ import annotations

import asyncio
import json
import random
import re
import string
import threading
from typing import Any

from telethon import errors, functions
from telethon.tl.types import Channel, Chat

from services.account_manager import _clients, run_sync
from utils.settings_manager import get as get_setting, get_int
from utils.storage_db import (
    create_scrape_job,
    create_scrape_keyword_runs,
    finish_scrape_job,
    get_existing_group_lookup,
    get_scrape_job,
    get_scrape_keyword_runs,
    get_scrape_results,
    mark_scrape_results_imported,
    save_scrape_results,
    set_scrape_job_status,
    simpan_banyak_grup,
    update_scrape_job,
    update_scrape_keyword_run,
)

_ACTIVE_THREADS: dict[int, threading.Thread] = {}


def _pilih_akun_scraper() -> str | None:
    """
    Pilih akun terbaik untuk scrape secara otomatis.
    Prioritas: akun active/online dulu, lalu restricted.
    Rotasi: pilih akun yang paling lama tidak scrape (berdasarkan last_login_at).
    Exclude: banned, suspended, session_expired, dan akun yang tidak ada di _clients.
    """
    from utils.database import get_conn
    conn = get_conn()
    rows = conn.execute("""
        SELECT phone, status, COALESCE(last_error_code,'') AS last_error_code
        FROM akun
        WHERE status NOT IN ('banned','suspended','session_expired')
        ORDER BY
            CASE WHEN status IN ('active','online') THEN 0 ELSE 1 END ASC,
            COALESCE(last_login_at, '1970-01-01') ASC
        LIMIT 20
    """).fetchall()
    conn.close()
    for row in rows:
        if row['phone'] in _clients:
            return row['phone']
    return None
_SOURCE_PRIORITY = {
    "base": 10,
    "custom": 18,
    "derived": 24,
    "smart": 32,
    "years": 42,
    "numbers": 54,
    "prefix_letters": 76,
    "suffix_letters": 78,
}
_SOURCE_TIER = {
    "base": "high",
    "custom": "high",
    "derived": "high",
    "smart": "medium",
    "years": "medium",
    "numbers": "medium",
    "prefix_letters": "low",
    "suffix_letters": "low",
}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_term(term: str) -> str:
    return re.sub(r"\s+", " ", (term or "").strip())


def _split_terms(raw_text: str) -> list[str]:
    parts = re.split(r"[\n,;]+", raw_text or "")
    return [_clean_term(p) for p in parts if _clean_term(p)]


def _parse_letters(start: str = "a", end: str = "z") -> list[str]:
    alphabet = list(string.ascii_lowercase)
    s = (start or "a").strip().lower()[:1] or "a"
    e = (end or "z").strip().lower()[:1] or "z"
    if s not in alphabet or e not in alphabet:
        return alphabet
    si = alphabet.index(s)
    ei = alphabet.index(e)
    if si > ei:
        si, ei = ei, si
    return alphabet[si : ei + 1]


def _smart_expand_words(base: str) -> list[str]:
    words = [w for w in re.split(r"\s+", base) if len(w) >= 4]
    out: list[str] = []
    if len(words) >= 2:
        out.append(" ".join(words[:2]))
        out.append("".join(words[:2]))
    out.extend(words[:3])
    return out


def _combine_with_base(base: str, pattern: str) -> str:
    pattern = _clean_term(pattern)
    if not pattern:
        return ""
    if "{base}" in pattern:
        return _clean_term(pattern.replace("{base}", base))
    if len(pattern.split()) >= 2:
        return pattern
    return _clean_term(f"{base} {pattern}")


def _source_priority(source: str) -> int:
    return int(_SOURCE_PRIORITY.get(source or "", 50))


def _source_tier(source: str) -> str:
    return _SOURCE_TIER.get(source or "", "medium")


def _keyword_max_attempts(source: str, options: dict[str, Any]) -> int:
    base = _safe_int(options.get("max_retry_per_keyword"), 2)
    if _source_tier(source) == "low":
        base = min(base, 1)
    return max(1, min(base, 4))


def generate_keyword_plan(raw_keywords: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
    options = dict(options or {})
    max_terms = max(1, min(_safe_int(options.get("max_terms"), 80), 500))
    include_base = bool(options.get("include_base", True))
    smart_expand = bool(options.get("expand_terms", False) or options.get("smart_expand", False))
    suffix_letters = bool(options.get("suffix_letters", False))
    prefix_letters = bool(options.get("prefix_letters", False))
    numbers_enabled = bool(options.get("number_suffix", False))
    years_enabled = bool(options.get("year_suffix", False))
    derived_enabled = bool(options.get("derived_terms_enabled", False))
    custom_enabled = bool(options.get("custom_terms_enabled", False))

    seen: set[str] = set()
    keyword_items: list[dict[str, Any]] = []
    source_counts = {
        "base": 0,
        "smart": 0,
        "suffix_letters": 0,
        "prefix_letters": 0,
        "numbers": 0,
        "years": 0,
        "derived": 0,
        "custom": 0,
    }
    tier_counts = {"high": 0, "medium": 0, "low": 0}
    truncated = False
    order_counter = 0

    def add(term: str, source: str):
        nonlocal truncated, order_counter
        term = _clean_term(term)
        if len(term) < 3:
            return
        key = term.lower()
        if key in seen:
            return
        if len(keyword_items) >= max_terms:
            truncated = True
            return
        order_counter += 1
        seen.add(key)
        tier = _source_tier(source)
        item = {
            "keyword": term,
            "source": source,
            "priority": _source_priority(source),
            "tier": tier,
            "max_attempts": _keyword_max_attempts(source, options),
            "order": order_counter,
        }
        keyword_items.append(item)
        source_counts[source] = source_counts.get(source, 0) + 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    base_terms = _split_terms(raw_keywords)
    if not base_terms:
        return {
            "keywords": [],
            "keyword_items": [],
            "total": 0,
            "source_counts": source_counts,
            "tier_counts": tier_counts,
            "base_terms": [],
            "truncated": False,
        }

    letters_suffix = _parse_letters(options.get("suffix_start"), options.get("suffix_end"))
    letters_prefix = _parse_letters(options.get("prefix_start"), options.get("prefix_end"))
    number_start = _safe_int(options.get("number_start"), 1)
    number_end = _safe_int(options.get("number_end"), 10)
    if number_start > number_end:
        number_start, number_end = number_end, number_start
    numbers = list(range(number_start, min(number_end, number_start + 200) + 1))
    years = [y for y in _split_terms(options.get("years_text") or "2024,2025,2026") if y]
    derived_terms = _split_terms(options.get("derived_terms_text") or "")
    custom_terms = _split_terms(options.get("custom_terms_text") or "")

    for base in base_terms:
        if include_base:
            add(base, "base")
        if smart_expand:
            for item in _smart_expand_words(base):
                add(item, "smart")
        if suffix_letters:
            for ch in letters_suffix:
                add(f"{base} {ch}", "suffix_letters")
        if prefix_letters:
            for ch in letters_prefix:
                add(f"{ch} {base}", "prefix_letters")
        if numbers_enabled:
            for num in numbers:
                add(f"{base} {num}", "numbers")
        if years_enabled:
            for year in years:
                add(f"{base} {year}", "years")
        if derived_enabled:
            for item in derived_terms:
                add(_combine_with_base(base, item), "derived")
        if custom_enabled:
            for item in custom_terms:
                add(_combine_with_base(base, item), "custom")
        if len(keyword_items) >= max_terms:
            truncated = True
            break

    keyword_items.sort(key=lambda item: (int(item.get("priority") or 50), int(item.get("order") or 0), len(item.get("keyword") or "")))
    keywords = [item["keyword"] for item in keyword_items]

    return {
        "keywords": keywords,
        "keyword_items": keyword_items,
        "total": len(keyword_items),
        "source_counts": source_counts,
        "tier_counts": tier_counts,
        "base_terms": base_terms,
        "truncated": truncated,
        "preview": keywords[:100],
        "preview_items": keyword_items[:100],
        "options": options,
    }


def _normalize_grup_id(entity) -> int:
    return int(entity.id)


def _is_valid_entity(
    entity: Any,
    *,
    include_basic_groups: bool = True,
    include_supergroups: bool = True,
    include_channels: bool = False,
) -> bool:
    if isinstance(entity, Chat):
        return include_basic_groups
    if isinstance(entity, Channel):
        # Filter grup forum (berisi topik/channel di dalamnya) — tidak cocok untuk broadcast
        if getattr(entity, "forum", False):
            return False
        if getattr(entity, "megagroup", False):
            return include_supergroups
        return include_channels
    return False


def _entity_type(entity: Any) -> str:
    if isinstance(entity, Chat):
        return "group"
    if isinstance(entity, Channel) and getattr(entity, "megagroup", False):
        return "supergroup"
    if isinstance(entity, Channel):
        return "channel"
    return "unknown"


async def _fetch_extra_info(client, entity: Any, enrich: bool) -> dict[str, Any]:
    if not enrich:
        return {}

    try:
        if isinstance(entity, Channel):
            full = await client(functions.channels.GetFullChannelRequest(channel=entity))
            full_chat = getattr(full, "full_chat", None)
            return {
                "deskripsi": getattr(full_chat, "about", None),
                "jumlah_member": getattr(full_chat, "participants_count", None)
                or getattr(entity, "participants_count", None)
                or 0,
            }
        if isinstance(entity, Chat):
            full = await client(functions.messages.GetFullChatRequest(chat_id=entity.id))
            full_chat = getattr(full, "full_chat", None)
            participants = getattr(full_chat, "participants", None)
            users = getattr(participants, "participants", None)
            return {
                "deskripsi": getattr(full_chat, "about", None),
                "jumlah_member": len(users) if users else getattr(entity, "participants_count", None) or 0,
            }
    except Exception:
        return {}
    return {}


def _score_candidate(item: dict[str, Any], keyword: str, min_members: int) -> tuple[int, list[str]]:
    text = " ".join(
        [
            item.get("nama") or "",
            item.get("username") or "",
            item.get("link") or "",
            item.get("deskripsi") or "",
        ]
    ).lower()
    words = [w.lower() for w in keyword.split() if len(w) >= 3]
    score = 0
    notes: list[str] = []

    if words and all(w in text for w in words):
        score += 35
        notes.append("keyword lengkap cocok")
    elif any(w in text for w in words):
        score += 18
        notes.append("keyword sebagian cocok")

    if item.get("username"):
        score += 18
        notes.append("punya username publik")
    if item.get("link"):
        score += 6

    tipe = item.get("tipe")
    if tipe == "supergroup":
        score += 18
        notes.append("supergroup")
    elif tipe == "group":
        score += 12
        notes.append("group")
    elif tipe == "channel":
        score += 4

    members = int(item.get("jumlah_member") or 0)
    if members >= 5000:
        score += 20
        notes.append("member tinggi")
    elif members >= 1000:
        score += 14
        notes.append("member bagus")
    elif members >= 200:
        score += 8
        notes.append("member cukup")

    if min_members and members < min_members:
        score -= 25
        notes.append("di bawah minimum member")

    if item.get("already_in_db"):
        score -= 15
        notes.append("sudah ada di database")

    if not item.get("username") and members == 0:
        score -= 8
        notes.append("data minim")

    score = max(0, min(100, score))
    return score, notes


async def _search_keyword(client, keyword: str, options: dict[str, Any], existing_ids: set[int], existing_usernames: set[str]) -> list[dict[str, Any]]:
    result = await client(
        functions.contacts.SearchRequest(
            q=keyword,
            limit=int(options.get("limit_per_keyword", 30)),
        )
    )

    rows: list[dict[str, Any]] = []
    seen_local: set[tuple[str, Any]] = set()

    for entity in getattr(result, "chats", []) or []:
        if not _is_valid_entity(
            entity,
            include_basic_groups=bool(options.get("include_basic_groups", True)),
            include_supergroups=bool(options.get("include_supergroups", True)),
            include_channels=bool(options.get("include_channels", False)),
        ):
            continue

        username = getattr(entity, "username", None)
        if options.get("require_public_username") and not username:
            continue

        unique_key = ("u", (username or "").lower()) if username else ("id", int(entity.id))
        if unique_key in seen_local:
            continue
        seen_local.add(unique_key)

        extra = await _fetch_extra_info(client, entity, bool(options.get("enrich_details", False)))
        members = extra.get("jumlah_member") or getattr(entity, "participants_count", None) or 0
        item = {
            "grup_id": int(entity.id),
            "nama": getattr(entity, "title", None) or str(entity.id),
            "username": username or "",
            "link": f"https://t.me/{username}" if username else None,
            "tipe": _entity_type(entity),
            "jumlah_member": int(members or 0),
            "deskripsi": extra.get("deskripsi") or "",
            "sumber_keyword": keyword,
            "already_in_db": int(int(entity.id) in existing_ids or ((username or "").lower() in existing_usernames if username else False)),
            "metadata": json.dumps(
                {
                    "megagroup": bool(getattr(entity, "megagroup", False)),
                    "broadcast": bool(getattr(entity, "broadcast", False)),
                    "verified": bool(getattr(entity, "verified", False)),
                    "scam": bool(getattr(entity, "scam", False)),
                    "forum": bool(getattr(entity, "forum", False)),
                },
                ensure_ascii=False,
            ),
        }
        item["relevance_score"], alasan = _score_candidate(item, keyword, int(options.get("min_members") or 0))
        item["recommended"] = int(
            item["relevance_score"] >= int(options.get("recommended_score", 40))
            and not item["already_in_db"]
        )
        item["catatan"] = ", ".join(alasan[:5])
        rows.append(item)

    return rows


def _classify_error(exc: Exception) -> str:
    name = exc.__class__.__name__.lower()
    if "flood" in name:
        return "flood_wait"
    if "timeout" in name or isinstance(exc, asyncio.TimeoutError):
        return "timeout"
    if "connection" in name:
        return "connection"
    if "querytooshort" in name or "searchqueryempty" in name:
        return "invalid_query"
    if "rpc" in name:
        return "rpc_error"
    return name[:60] or "unknown"


def _is_retryable_exception(exc: Exception) -> bool:
    retryable_types = tuple(
        t for t in (
            asyncio.TimeoutError,
            getattr(errors, 'FloodWaitError', None),
            getattr(errors, 'ServerError', None),
            getattr(errors, 'TimedOutError', None),
        ) if t
    )
    if retryable_types and isinstance(exc, retryable_types):
        return True
    name = exc.__class__.__name__.lower()
    retry_tokens = ("timeout", "flood", "server", "connection", "workerbusy", "internal")
    return any(token in name for token in retry_tokens)


def _retry_sleep_seconds(attempt: int, delay_min: float, delay_max: float) -> float:
    base = max(delay_max, delay_min, 1.0)
    return min(30.0, base * max(1, attempt) * 2.0)


def _keyword_quality_score(rows: list[dict[str, Any]], unique_rows: list[dict[str, Any]], source: str, attempts: int) -> int:
    if not rows:
        return 0
    avg_relevance = sum(int(r.get("relevance_score") or 0) for r in rows) / max(1, len(rows))
    uniqueness = min(25, len(unique_rows) * 4)
    source_bonus = {"high": 8, "medium": 4, "low": 0}.get(_source_tier(source), 0)
    penalty = max(0, attempts - 1) * 6
    score = int(round(avg_relevance * 0.65 + uniqueness + source_bonus - penalty))
    return max(0, min(100, score))


def _job_thread_alive(job_id: int) -> bool:
    thread = _ACTIVE_THREADS.get(job_id)
    return bool(thread and thread.is_alive())


def _spawn_thread(job_id: int):
    if _job_thread_alive(job_id):
        return
    thread = threading.Thread(target=_thread_entry, args=(job_id,), daemon=True)
    _ACTIVE_THREADS[job_id] = thread
    thread.start()


async def _run_scrape_job(job_id: int):
    job = get_scrape_job(job_id)
    if not job:
        return

    phone = job["phone"]
    client = _clients.get(phone)
    if not client:
        raise RuntimeError(f"Akun {phone} tidak aktif")

    options = json.loads(job.get("options_json") or "{}")
    plan = generate_keyword_plan(job.get("keywords_text") or "", options)
    runs = get_scrape_keyword_runs(job_id)
    if not runs:
        if not plan["keyword_items"]:
            raise ValueError("Keyword kosong atau terlalu pendek")
        create_scrape_keyword_runs(job_id, plan["keyword_items"])
        runs = get_scrape_keyword_runs(job_id)

    existing_results = get_scrape_results(job_id, include_imported=True)
    job_seen: set[tuple[str, Any]] = set()
    for row in existing_results:
        key = ("u", row["username"].lower()) if row.get("username") else ("id", row["grup_id"])
        job_seen.add(key)

    processed = sum(1 for r in runs if r["status"] in {"done", "failed", "skipped", "stopped"})
    total_found = sum(int(r.get("found_count") or 0) for r in runs)
    total_saved = sum(int(r.get("saved_count") or 0) for r in runs)
    total_keywords = len(runs)

    update_scrape_job(
        job_id,
        status="running",
        total_keywords=total_keywords,
        processed_keywords=processed,
        total_found=total_found,
        total_saved=total_saved,
        error_message=None,
    )

    existing_ids, existing_usernames = get_existing_group_lookup()
    delay_min = max(0.2, _safe_float(options.get("delay_keyword_min"), 0.8))
    delay_max = max(delay_min, _safe_float(options.get("delay_keyword_max"), 1.6))

    for run in get_scrape_keyword_runs(job_id):
        if run["status"] in {"done", "skipped", "stopped"}:
            continue

        current_job = get_scrape_job(job_id) or {}
        while current_job.get("status") == "paused":
            await asyncio.sleep(1)
            current_job = get_scrape_job(job_id) or {}
        if current_job.get("status") == "stopped":
            finish_scrape_job(
                job_id,
                status="stopped",
                processed_keywords=processed,
                total_found=total_found,
                total_saved=total_saved,
            )
            return

        # Fallback rotasi akun: kalau akun sekarang sudah offline/banned di tengah job
        if phone not in _clients:
            new_phone = _pilih_akun_scraper()
            if new_phone and new_phone != phone:
                print(f"[Scraper] Job #{job_id} — akun {phone} tidak tersedia, rotasi ke {new_phone}")
                phone = new_phone
                client = _clients[phone]
                try:
                    from utils.database import get_conn as _gc_sr
                    _c_sr = _gc_sr()
                    _c_sr.execute("UPDATE scrape_job SET phone=%s WHERE id=%s", (phone, job_id))
                    _c_sr.commit()
                    _c_sr.close()
                except Exception:
                    pass
                update_scrape_job(job_id, error_message=f"Akun diganti ke {phone} (rotasi otomatis)")
            else:
                update_scrape_job(
                    job_id,
                    status="paused",
                    error_message="no_available_account — tidak ada akun aktif untuk melanjutkan",
                )
                return

        keyword = run["keyword"]
        source = run.get("source") or "base"
        attempts = int(run.get("attempt_count") or 0)
        max_attempts = max(1, int(run.get("max_attempts") or options.get("max_retry_per_keyword") or _keyword_max_attempts(source, options)))
        found_count = 0
        saved_count = 0
        final_status = None
        quality_score = int(run.get("quality_score") or 0)

        while attempts < max_attempts:
            attempts += 1
            update_scrape_keyword_run(
                run["id"],
                status="running" if attempts == 1 else "retrying",
                error_message=None,
                attempt_count=attempts,
                max_attempts=max_attempts,
                source=source,
                priority=int(run.get("priority") or _source_priority(source)),
                tier=run.get("tier") or _source_tier(source),
                started=(attempts == 1),
            )
            try:
                rows = await _search_keyword(client, keyword, options, existing_ids, existing_usernames)
                found_count = len(rows)
            except (errors.QueryTooShortError, errors.SearchQueryEmptyError):
                final_status = "skipped"
                update_scrape_keyword_run(
                    run["id"],
                    status="skipped",
                    found_count=0,
                    saved_count=0,
                    error_message="Keyword terlalu pendek / kosong",
                    attempt_count=attempts,
                    max_attempts=max_attempts,
                    last_error_code="invalid_query",
                    finished=True,
                )
                processed += 1
                update_scrape_job(job_id, processed_keywords=processed, error_message=f"Keyword '{keyword}' dilewati karena terlalu pendek")
                break
            except Exception as exc:
                error_code = _classify_error(exc)
                retryable = _is_retryable_exception(exc)
                if retryable and attempts < max_attempts:
                    update_scrape_keyword_run(
                        run["id"],
                        status="retrying",
                        error_message=f"{exc} · retry {attempts}/{max_attempts}",
                        attempt_count=attempts,
                        max_attempts=max_attempts,
                        last_error_code=error_code,
                    )
                    update_scrape_job(job_id, error_message=f"Keyword '{keyword}' retry karena {error_code}")
                    await asyncio.sleep(_retry_sleep_seconds(attempts, delay_min, delay_max))
                    continue
                final_status = "failed"
                update_scrape_keyword_run(
                    run["id"],
                    status="failed",
                    found_count=0,
                    saved_count=0,
                    error_message=str(exc),
                    attempt_count=attempts,
                    max_attempts=max_attempts,
                    last_error_code=error_code,
                    finished=True,
                )
                processed += 1
                update_scrape_job(job_id, processed_keywords=processed, error_message=f"Keyword '{keyword}': {exc}")
                break

            unique_rows: list[dict[str, Any]] = []
            for row in rows:
                key = ("u", row["username"].lower()) if row.get("username") else ("id", row["grup_id"])
                if key in job_seen:
                    continue
                job_seen.add(key)
                unique_rows.append(row)

            if unique_rows:
                save_scrape_results(job_id, unique_rows)
                saved_count = len(unique_rows)
            total_found += found_count
            total_saved += saved_count
            processed += 1
            quality_score = _keyword_quality_score(rows, unique_rows, source, attempts)
            update_scrape_keyword_run(
                run["id"],
                status="done",
                found_count=found_count,
                saved_count=saved_count,
                attempt_count=attempts,
                max_attempts=max_attempts,
                quality_score=quality_score,
                last_error_code="",
                error_message=None,
                finished=True,
            )
            update_scrape_job(
                job_id,
                processed_keywords=processed,
                total_found=total_found,
                total_saved=total_saved,
                error_message=None,
            )
            final_status = "done"
            break

        if final_status == "done":
            await client.get_me()
            await client(functions.updates.GetStateRequest())
            # Jeda utama antar keyword — acak agar mirip manusia
            await asyncio.sleep(random.uniform(delay_min, delay_max))
            # Istirahat panjang setiap 5 keyword (2-5 menit)
            if processed % 5 == 0 and processed > 0:
                jeda_istirahat = random.uniform(120, 300)
                update_scrape_job(job_id, error_message=f"Istirahat {int(jeda_istirahat//60)}m setelah {processed} keyword")
                await asyncio.sleep(jeda_istirahat)
                update_scrape_job(job_id, error_message=None)
            # Istirahat lebih panjang setiap 20 keyword (10-20 menit)
            elif processed % 20 == 0 and processed > 0:
                jeda_panjang = random.uniform(600, 1200)
                update_scrape_job(job_id, error_message=f"Istirahat panjang {int(jeda_panjang//60)}m setelah {processed} keyword")
                await asyncio.sleep(jeda_panjang)
                update_scrape_job(job_id, error_message=None)

    finish_scrape_job(job_id, total_found=total_found, total_saved=total_saved)


def _thread_entry(job_id: int):
    try:
        run_sync(_run_scrape_job(job_id), timeout=60 * 45)
    except Exception as exc:
        update_scrape_job(job_id, status="failed", error_message=str(exc), selesai=True)
    finally:
        _ACTIVE_THREADS.pop(job_id, None)


def preview_scrape_keywords(raw_keywords: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
    plan = generate_keyword_plan(raw_keywords, options)
    return {
        "ok": True,
        "base_terms": plan["base_terms"],
        "keywords": plan["preview"],
        "keyword_items": plan.get("preview_items") or [],
        "total": plan["total"],
        "source_counts": plan["source_counts"],
        "tier_counts": plan.get("tier_counts") or {},
        "truncated": plan["truncated"],
    }


def start_scrape_job(phone: str, raw_keywords: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
    phone = (phone or "").strip()
    if not phone or phone == 'auto':
        # Mode otomatis: sistem pilih akun terbaik & rotasi secara otomatis
        phone = _pilih_akun_scraper()
        if not phone:
            raise ValueError("Tidak ada akun yang tersedia untuk scrape")
    elif phone not in _clients:
        raise ValueError("Akun scraper belum online — login dulu di tab Akun")
    # Catatan: akun banned/restrict tetap BOLEH scrape
    # karena SearchRequest Telegram masih bisa berjalan meski akun dibatasi kirim pesan

    options = dict(options or {})
    options.setdefault("limit_per_keyword", get_int('scraper_limit_per_keyword', 30))
    options.setdefault("min_members", get_int('scraper_min_members', 0))
    options.setdefault("require_public_username", True)
    options.setdefault("include_basic_groups", True)
    options.setdefault("include_supergroups", True)
    options.setdefault("include_channels", False)
    options.setdefault("expand_terms", True)
    options.setdefault("max_terms", get_int('scraper_max_terms', 80))
    options.setdefault("enrich_details", False)
    options.setdefault("recommended_score", get_int('scraper_recommended_score', 30))
    options.setdefault("include_base", True)
    options.setdefault("delay_keyword_min", float(get_setting('scraper_delay_keyword_min', 1) or 1))
    options.setdefault("delay_keyword_max", float(get_setting('scraper_delay_keyword_max', 3) or 3))
    options.setdefault("max_retry_per_keyword", 2)

    plan = generate_keyword_plan(raw_keywords, options)
    if not plan["keyword_items"]:
        raise ValueError("Masukkan minimal satu keyword yang valid")

    job_id = create_scrape_job(phone, "\n".join(plan["keywords"]), len(plan["keyword_items"]), options)
    create_scrape_keyword_runs(job_id, plan["keyword_items"])
    _spawn_thread(job_id)
    return get_scrape_job(job_id) or {"id": job_id, "status": "queued"}


def control_scrape_job(job_id: int, action: str) -> dict[str, Any]:
    job = get_scrape_job(job_id)
    if not job:
        raise ValueError("Job scraper tidak ditemukan")

    action = (action or "").strip().lower()
    if action == "pause":
        if job["status"] not in {"queued", "running"}:
            raise ValueError("Job hanya bisa dipause saat queued/running")
        set_scrape_job_status(job_id, "paused")
    elif action == "resume":
        if job["status"] not in {"paused", "queued", "failed"}:
            raise ValueError("Job hanya bisa di-resume dari paused/queued/failed")
        set_scrape_job_status(job_id, "running")
        _spawn_thread(job_id)
    elif action == "stop":
        if job["status"] in {"done", "stopped"}:
            raise ValueError("Job sudah selesai")
        set_scrape_job_status(job_id, "stopped")
    elif action == "retry_failed":
        runs = get_scrape_keyword_runs(job_id)
        failed = [r for r in runs if r.get("status") == "failed"]
        if not failed:
            raise ValueError("Tidak ada keyword failed untuk diulang")
        for run in failed:
            update_scrape_keyword_run(
                run["id"],
                status="pending",
                found_count=0,
                saved_count=0,
                error_message=None,
                last_error_code="",
                quality_score=0,
                attempt_count=0,
                max_attempts=int(run.get("max_attempts") or 2),
            )
        refreshed = get_scrape_keyword_runs(job_id)
        processed = sum(1 for r in refreshed if r["status"] in {"done", "failed", "skipped", "stopped"})
        update_scrape_job(job_id, status="running", processed_keywords=processed, error_message=None)
        _spawn_thread(job_id)
    else:
        raise ValueError("Aksi job tidak dikenali")

    return get_scrape_job(job_id) or {"id": job_id}


def import_scrape_results(job_id: int, result_ids: list[int] | None = None, mode: str = "selected") -> dict[str, Any]:
    results = get_scrape_results(
        job_id=job_id,
        only_recommended=(mode == "recommended"),
        only_new=(mode in {"recommended", "new", "all_new"}),
        include_imported=False,
    )
    if result_ids:
        wanted = {int(x) for x in result_ids}
        results = [r for r in results if r["id"] in wanted]
    elif mode == "selected":
        raise ValueError("Pilih minimal satu hasil scrape")

    if not results:
        total_hasil = get_scrape_results(job_id=job_id, include_imported=True)
        if not total_hasil:
            return {
                "ok": False,
                "imported": 0,
                "skipped_channels": 0,
                "skipped_low_score": 0,
                "skipped_no_username": 0,
                "skipped_wrong_type": 0,
                "groups": [],
                "error": "Job ini belum punya hasil scrape. Tunggu job selesai dulu.",
            }
        return {
            "ok": True,
            "imported": 0,
            "skipped_channels": 0,
            "skipped_low_score": 0,
            "skipped_no_username": 0,
            "skipped_wrong_type": 0,
            "groups": [],
            "info": f"Tidak ada hasil baru yang memenuhi kriteria. Total hasil job: {len(total_hasil)}, sudah diimpor: {sum(1 for r in total_hasil if r.get('imported'))}",
        }

    min_quality = int(get_int('result_min_quality_score', 30))
    username_required = str(get_setting('result_username_required', 0) or 0).strip() in {'1', 'true', 'True'}
    allowed_types_raw = str(get_setting('result_allowed_entity_types', 'group,supergroup') or 'group,supergroup')
    allowed_types = {x.strip().lower() for x in allowed_types_raw.split(',') if x.strip()} or {'group', 'supergroup'}

    groups = []
    imported_ids = []
    skipped_channels = 0
    skipped_low_score = 0
    skipped_no_username = 0
    skipped_wrong_type = 0
    for row in results:
        tipe = str(row.get("tipe") or "group").lower()
        if tipe == "channel":
            skipped_channels += 1
        if tipe not in allowed_types:
            skipped_wrong_type += 1
            continue

        quality = int(row.get('relevance_score') or row.get('quality_score') or row.get('score') or row.get('recommended_score') or 0)
        if quality < min_quality:
            skipped_low_score += 1
            continue

        username = (row.get("username") or '').strip()
        if username_required and not username:
            skipped_no_username += 1
            continue

        groups.append(
            {
                "id": row["grup_id"],
                "nama": row["nama"],
                "username": username or None,
                "tipe": tipe,
                "jumlah_member": row.get("jumlah_member") or 0,
                "link": row.get("link") or None,
                "sumber": f"scraper:{row.get('sumber_keyword') or ''}"[:120],
            }
        )
        imported_ids.append(row["id"])

    if groups:
        simpan_banyak_grup(groups, sumber="scraper")
    if imported_ids:
        mark_scrape_results_imported(imported_ids)
    current = get_scrape_job(job_id) or {}
    update_scrape_job(
        job_id,
        total_imported=(current.get("total_imported", 0) or 0) + len(imported_ids),
    )
    return {
        "ok": True,
        "imported": len(imported_ids),
        "skipped_channels": skipped_channels,
        "skipped_low_score": skipped_low_score,
        "skipped_no_username": skipped_no_username,
        "skipped_wrong_type": skipped_wrong_type,
        "groups": groups,
    }
