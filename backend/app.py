from __future__ import annotations

import asyncio
import threading
from typing import Any

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.exceptions import HTTPException, NotFound
from flask_cors import CORS

import config
from core.broadcast_session import buat_sesi, get_sesi, get_semua_sesi, hapus_sesi, jalankan_sesi_thread, stop_sesi
from core.grup_analisis import fetch_last_chat, get_semua_analisis, update_semua_score, start_last_chat_worker, start_daily_reset_worker
from core.scoring import get_label_akun, get_label_grup, update_score_akun
from core.smart_sender import pilih_akun_tersedia, ringkasan_akun
from core.sync_manager import buat_sesi_sync, get_sesi_sync, jalankan_sync, stop_sesi_sync
from core.warming import get_info_warming
from services.account_manager import _clients, _loop, auto_reconnect_semua, delete_akun_permanen, login_akun, logout_akun, run_sync, submit_otp
from services.group_manager import fetch_grup_dari_akun
from services.message_service import kirim_pesan_manual
from services.scraper_service import control_scrape_job, import_scrape_results, preview_scrape_keywords, start_scrape_job
from services.orchestrator_service import start_orchestrator_worker
from utils.database import init_db
from routes import register_blueprints
from utils.settings_manager import get_semua as get_semua_settings, update_banyak
from utils.storage_db import (
    get_akun_by_grup,
    get_draft_aktif,
    get_grup_aktif,
    get_grup_by_akun,
    get_grup_hot,
    get_ringkasan_hari_ini,
    get_riwayat_hari_ini,
    get_scrape_job,
    get_scrape_jobs,
    get_scrape_keyword_runs,
    get_scrape_results,
    get_semua_akun,
    get_semua_antrian,
    get_semua_draft,
    get_semua_grup,
    grup_sudah_ada,
    hapus_antrian,
    hapus_draft,
    set_level_warming,
    set_score_akun,
    set_score_grup,
    set_status_akun,
    set_status_grup,
    set_status_grup_massal,
    simpan_banyak_grup,
    simpan_draft,
    sudah_dikirim_hari_ini,
    tambah_antrian,
    update_status_antrian,
)

app = Flask(__name__)
CORS(app)
register_blueprints(app)


# ── HELPERS ───────────────────────────────────────────────

def _body() -> dict[str, Any]:
    return request.get_json(silent=True) or {}


def _error(message: str, status_code: int = 400):
    return jsonify({"error": message}), status_code


def _require(body: dict[str, Any], key: str, label: str | None = None):
    value = body.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"{label or key} wajib diisi")
    return value


def _as_int(value: Any, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} harus berupa angka")


def _normalize_settings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})

    int_min_zero = {
        'scraper_limit_per_keyword', 'scraper_min_members', 'scraper_recommended_score', 'scraper_max_terms',
        'result_min_quality_score', 'permission_min_score', 'assignment_min_health_score', 'assignment_min_warming_level',
        'assignment_retry_count', 'assignment_reassign_count', 'auto_join_max_per_cycle', 'auto_join_reserve_quota',
        'assignment_broadcast_delay_minutes', 'campaign_group_cooldown_hours', 'campaign_group_cooldown_minutes', 'campaign_inactive_threshold_days',
        'campaign_retry_delay_minutes', 'orchestrator_interval_seconds', 'orchestrator_import_batch',
        'orchestrator_permission_batch', 'orchestrator_assign_batch', 'orchestrator_campaign_batch',
        'orchestrator_delivery_batch', 'orchestrator_recovery_batch', 'recovery_stuck_scrape_threshold',
        'recovery_stuck_assignment_threshold', 'recovery_stuck_campaign_threshold',
        'broadcast_jeda_min', 'broadcast_jeda_max',
        'broadcast_jeda_kirim_min_detik', 'broadcast_jeda_kirim_max_detik', 'broadcast_masa_tunggu_setelah_assign_menit',
        'broadcast_cooldown_grup_jam', 'broadcast_cooldown_grup_menit', 'broadcast_batas_grup_sepi_hari', 'broadcast_retry_delay_detik',
        'broadcast_target_per_sesi', 'broadcast_target_per_akun_per_sesi', 'broadcast_batch_delivery',
        'pipeline_interval_detik', 'pipeline_batch_import', 'pipeline_batch_permission', 'pipeline_batch_assign',
        'pipeline_batch_campaign', 'pipeline_batch_delivery', 'pipeline_batch_recovery',
        'pipeline_retry_maks_per_item', 'pipeline_retry_jeda_detik',
    }
    for key in list(int_min_zero):
        if key in normalized:
            try:
                normalized[key] = str(max(0, int(float(normalized[key]))))
            except (TypeError, ValueError):
                normalized.pop(key, None)

    # Score settings angka
    for key in list(normalized.keys()):
        if key.startswith('score_akun_') or key.startswith('score_grup_'):
            if key.endswith('_jadi_nol'):
                normalized[key] = '1' if str(normalized[key]).strip().lower() in {'1', 'true', 'yes', 'on'} else '0'
                continue
            try:
                normalized[key] = str(max(0, int(float(normalized[key]))))
            except (TypeError, ValueError):
                # boolean/text tertentu biarkan apa adanya
                if key in {'score_akun_banned_jadi_nol'}:
                    normalized.pop(key, None)

    for key in ('scraper_delay_keyword_min', 'scraper_delay_keyword_max'):
        if key in normalized:
            try:
                normalized[key] = f"{max(0.2, float(normalized[key])):.2f}".rstrip('0').rstrip('.')
            except (TypeError, ValueError):
                normalized.pop(key, None)

    for key in ('broadcast_jeda_min', 'broadcast_jeda_max', 'broadcast_jeda_kirim_min_detik', 'broadcast_jeda_kirim_max_detik'):
        if key in normalized:
            try:
                normalized[key] = str(max(0, int(float(normalized[key]))))
            except (TypeError, ValueError):
                normalized.pop(key, None)

    if 'broadcast_jeda_min' in normalized or 'broadcast_jeda_max' in normalized:
        try:
            jeda_min = int(normalized.get('broadcast_jeda_min', payload.get('broadcast_jeda_min', 20)))
            jeda_max = int(normalized.get('broadcast_jeda_max', payload.get('broadcast_jeda_max', 45)))
            if jeda_max < jeda_min:
                jeda_max = jeda_min
            normalized['broadcast_jeda_min'] = str(jeda_min)
            normalized['broadcast_jeda_max'] = str(jeda_max)
        except (TypeError, ValueError):
            pass

    if 'broadcast_jeda_kirim_min_detik' in normalized or 'broadcast_jeda_kirim_max_detik' in normalized:
        try:
            jeda_min = int(normalized.get('broadcast_jeda_kirim_min_detik', payload.get('broadcast_jeda_kirim_min_detik', 20)))
            jeda_max = int(normalized.get('broadcast_jeda_kirim_max_detik', payload.get('broadcast_jeda_kirim_max_detik', 45)))
            if jeda_max < jeda_min:
                jeda_max = jeda_min
            normalized['broadcast_jeda_kirim_min_detik'] = str(jeda_min)
            normalized['broadcast_jeda_kirim_max_detik'] = str(jeda_max)
        except (TypeError, ValueError):
            pass

    if 'scraper_delay_keyword_min' in normalized or 'scraper_delay_keyword_max' in normalized:
        try:
            delay_min = float(normalized.get('scraper_delay_keyword_min', payload.get('scraper_delay_keyword_min', 1)))
            delay_max = float(normalized.get('scraper_delay_keyword_max', payload.get('scraper_delay_keyword_max', 3)))
            if delay_max < delay_min:
                delay_max = delay_min
            normalized['scraper_delay_keyword_min'] = f"{delay_min:.2f}".rstrip('0').rstrip('.')
            normalized['scraper_delay_keyword_max'] = f"{delay_max:.2f}".rstrip('0').rstrip('.')
        except (TypeError, ValueError):
            pass

    if 'result_allowed_entity_types' in normalized:
        raw = str(normalized.get('result_allowed_entity_types') or '')
        allowed = ','.join([part.strip().lower() for part in raw.split(',') if part.strip()])
        normalized['result_allowed_entity_types'] = allowed or 'group,supergroup'

    # Alias key baru -> key lama agar engine lama tetap jalan, tanpa menduplikasi UI.
    alias_map = {
        'broadcast_enabled': ('auto_campaign_enabled', None),
        'broadcast_jeda_kirim_min_detik': ('broadcast_jeda_min', None),
        'broadcast_jeda_kirim_max_detik': ('broadcast_jeda_max', None),
        'broadcast_masa_tunggu_setelah_assign_menit': ('assignment_broadcast_delay_minutes', None),
        'broadcast_cooldown_grup_jam': ('campaign_group_cooldown_hours', None),
        'broadcast_cooldown_grup_menit': ('campaign_group_cooldown_minutes', None),
        'broadcast_tahan_grup_sepi': ('campaign_skip_inactive_groups_enabled', None),
        'broadcast_batas_grup_sepi_hari': ('campaign_inactive_threshold_days', None),
        'broadcast_tahan_jika_chat_terakhir_milik_sendiri': ('campaign_skip_if_last_chat_is_ours', None),
        'broadcast_requeue_jika_sender_tidak_siap': ('campaign_requeue_sender_missing', None),
        'broadcast_target_per_sesi': ('campaign_session_target_limit', None),
        'broadcast_target_per_akun_per_sesi': ('campaign_session_per_sender_limit', None),
        'broadcast_izinkan_grup_baru_masuk_sesi_berjalan': ('campaign_allow_mid_session_enqueue', None),
        'broadcast_batch_delivery': ('orchestrator_delivery_batch', None),
        'pipeline_enabled': ('pause_all_automation', lambda v: '0' if str(v).strip().lower() in {'1','true','yes','on'} else '1'),
        'pipeline_pause_semua': ('pause_all_automation', None),
        'pipeline_maintenance_mode': ('maintenance_mode', None),
        'pipeline_interval_detik': ('orchestrator_interval_seconds', None),
        'pipeline_batch_import': ('orchestrator_import_batch', None),
        'pipeline_batch_permission': ('orchestrator_permission_batch', None),
        'pipeline_batch_assign': ('orchestrator_assign_batch', None),
        'pipeline_batch_campaign': ('orchestrator_campaign_batch', None),
        'pipeline_batch_delivery': ('orchestrator_delivery_batch', None),
        'pipeline_batch_recovery': ('orchestrator_recovery_batch', None),
        'pipeline_wajib_permission_valid': ('campaign_valid_permission_required', None),
        'pipeline_wajib_status_managed_untuk_broadcast': ('campaign_managed_required', None),
        'pipeline_sender_pool_default': ('campaign_default_sender_pool', None),
        'pipeline_lanjutkan_proses_setelah_restart': ('recovery_resume_on_restart', None),
        'pipeline_tandai_proses_setengah_jalan': ('recovery_mark_partial_if_worker_missing', None),
    }
    for source_key, (legacy_key, transform) in alias_map.items():
        if source_key in normalized:
            value = normalized[source_key]
            normalized[legacy_key] = transform(value) if transform else value

    if 'broadcast_retry_delay_detik' in normalized:
        try:
            seconds = max(0, int(normalized['broadcast_retry_delay_detik']))
            normalized['campaign_retry_delay_minutes'] = str(max(1, (seconds + 59) // 60))
        except Exception:
            pass

    if 'pipeline_retry_umum_enabled' in normalized:
        enabled = str(normalized['pipeline_retry_umum_enabled']).strip().lower() in {'1','true','yes','on'}
        if not enabled:
            normalized['campaign_retry_policy'] = 'no_retry'
        else:
            retries = int(normalized.get('pipeline_retry_maks_per_item', payload.get('pipeline_retry_maks_per_item', 3)) or 3)
            normalized['campaign_retry_policy'] = 'retry_once' if retries <= 1 else ('retry_twice' if retries == 2 else 'retry_three')
    elif 'pipeline_retry_maks_per_item' in normalized:
        retries = int(normalized.get('pipeline_retry_maks_per_item', 3) or 3)
        normalized['campaign_retry_policy'] = 'retry_once' if retries <= 1 else ('retry_twice' if retries == 2 else 'retry_three')

    return normalized


@app.errorhandler(HTTPException)
def handle_http_error(err):
    if request.path.startswith('/api/'):
        return jsonify({'error': err.description or err.name}), err.code
    return err


@app.errorhandler(Exception)
def handle_unexpected_error(err):
    app.logger.exception("Unhandled error: %s", err)
    return jsonify({"error": str(err)}), 500


def _safe_startup():
    init_db()
    try:
        from services.automation_rule_engine import ensure_default_rules, sync_system_rules_to_fast_profile
        ensure_default_rules()
        sync_result = sync_system_rules_to_fast_profile()
        print(f"Startup: rule sistem disinkronkan ke profil cepat ({sync_result.get('updated', 0)} rule diperbarui)")
    except Exception as e:
        print(f"Startup: gagal seed/sinkron automation rules ({e})")

    from utils.database import get_conn

    # [1] Fix akun lama: tanggal_buat NULL → hari ini
    try:
        from datetime import date
        conn = get_conn()
        today = str(date.today())
        n = conn.execute(
            "UPDATE akun SET tanggal_buat=%s WHERE tanggal_buat IS NULL OR tanggal_buat=''",
            (today,)
        ).rowcount
        conn.commit()
        conn.close()
        if n > 0:
            print(f"Startup: {n} akun tanggal_buat di-set ke hari ini ({today})")
    except Exception as e:
        print(f"Startup: gagal fix tanggal_buat ({e})")

    # [2] Fix akun lama: health_score NULL/0 → 100
    # Tanpa ini, akun tidak lolos filter min_health_score=50 → auto assign gagal
    try:
        conn = get_conn()
        n = conn.execute(
            "UPDATE akun SET health_score=100 WHERE health_score IS NULL OR health_score=0"
        ).rowcount
        conn.commit()
        conn.close()
        if n > 0:
            print(f"Startup: {n} akun health_score di-set ke 100")
    except Exception as e:
        print(f"Startup: gagal fix health_score ({e})")

    # [3] Fix grup lama: assignment_status NULL → 'ready_assign'
    # _ensure_column di SQLite tidak mengisi nilai untuk baris lama
    try:
        conn = get_conn()
        n = conn.execute(
            """UPDATE grup SET assignment_status='ready_assign'
               WHERE (assignment_status IS NULL OR assignment_status='')
               AND status='active'"""
        ).rowcount
        conn.commit()
        conn.close()
        if n > 0:
            print(f"Startup: {n} grup assignment_status di-set ke 'ready_assign'")
    except Exception as e:
        print(f"Startup: gagal fix assignment_status ({e})")

    # [4] Fix grup lama: permission_status NULL/'unknown' → 'opt_in'
    # Assignment stage wajibkan permission_status IN ('valid','owned','opt_in',...)
    # Grup yang masih 'unknown' tidak akan pernah di-assign
    try:
        conn = get_conn()
        n = conn.execute(
            """UPDATE grup SET permission_status='opt_in'
               WHERE (permission_status IS NULL OR permission_status='unknown')
               AND status='active'"""
        ).rowcount
        conn.commit()
        conn.close()
        if n > 0:
            print(f"Startup: {n} grup permission_status di-set ke 'opt_in'")
    except Exception as e:
        print(f"Startup: gagal fix permission_status ({e})")

    # [5] Fix daily_new_group_cap akun lama yang masih 10 (terlalu kecil)
    # Dengan 3 akun dan 151 grup, cap 10 berarti hanya 30 grup yang bisa di-assign
    # Naikkan ke 60 agar tiap akun bisa pegang lebih banyak grup
    try:
        conn = get_conn()
        n = conn.execute(
            "UPDATE akun SET daily_new_group_cap=60 WHERE daily_new_group_cap IS NULL OR daily_new_group_cap <= 10"
        ).rowcount
        conn.commit()
        conn.close()
        if n > 0:
            print(f"Startup: {n} akun daily_new_group_cap dinaikkan ke 60")
    except Exception as e:
        print(f"Startup: gagal fix daily_new_group_cap ({e})")

    # [6] Reset setting kritis yang menyebabkan grup stuck di stabilization
    # Nilai lama di DB akan di-override ke nilai cepat & aman
    try:
        from utils.settings_manager import get as _gs, set as _ss
        # Kategori 1: selalu override (setting sistem kritis)
        always_override = {
            "assignment_broadcast_delay_minutes": "0",
            "broadcast_masa_tunggu_setelah_assign_menit": "0",
            "campaign_managed_required": "0",
            "pipeline_wajib_status_managed_untuk_broadcast": "0",
            "campaign_skip_if_last_chat_is_ours": "0",
            "broadcast_tahan_jika_chat_terakhir_milik_sendiri": "0",
            "campaign_skip_inactive_groups_enabled": "0",
            "broadcast_tahan_grup_sepi": "0",
            "assignment_min_health_score": "0",
            "orchestrator_interval_seconds": "10",
            "pipeline_interval_detik": "10",
        }
        for _k, _v in always_override.items():
            _ss(_k, _v)

        # Kategori 2: hanya isi kalau belum ada — user bebas ubah dari UI
        default_only = {
            "campaign_session_target_limit": "200",
            "broadcast_target_per_sesi": "200",
            "campaign_session_per_sender_limit": "20",
            "broadcast_target_per_akun_per_sesi": "20",
            "broadcast_jeda_min": "3",
            "broadcast_jeda_max": "7",
            "broadcast_jeda_kirim_min_detik": "3",
            "broadcast_jeda_kirim_max_detik": "7",
            "campaign_group_cooldown_hours": "0",
            "broadcast_cooldown_grup_jam": "0",
            "campaign_group_cooldown_minutes": "1",
            "broadcast_cooldown_grup_menit": "1",
            "campaign_retry_delay_minutes": "1",
            "broadcast_retry_delay_detik": "60",
            "orchestrator_import_batch": "200",
            "orchestrator_permission_batch": "200",
            "orchestrator_assign_batch": "150",
            "orchestrator_campaign_batch": "150",
            "orchestrator_delivery_batch": "40",
            "pipeline_batch_import": "200",
            "pipeline_batch_permission": "200",
            "pipeline_batch_assign": "150",
            "pipeline_batch_campaign": "150",
            "pipeline_batch_delivery": "40",
            "result_min_quality_score": "20",
            "result_username_required": "0",
            "auto_join_max_per_cycle": "8",
            "auto_join_reserve_quota": "0",
            "w1_maks_join": "20",
            "w1_maks_kirim": "20",
            "w1_jeda_join": "3600",
            "w1_jeda_kirim": "5",
            "w2_maks_join": "10",
            "w2_maks_kirim": "25",
            "w2_jeda_join": "1800",
            "w2_jeda_kirim": "5",
            "w3_maks_join": "20",
            "w3_jeda_join": "600",
            "w4_maks_join": "30",
            "w4_jeda_join": "300",
        }
        diisi = 0
        for _k, _v in default_only.items():
            existing = _gs(_k, None)
            if existing is None or str(existing).strip() == '':
                _ss(_k, _v)
                diisi += 1
        print(f"Startup: setting kritis di-reset, {diisi} default baru diisi")
    except Exception as e:
        print(f"Startup: gagal reset setting ({e})")

    # [6b] Sinkronkan akun baru agar tidak terlihat macet terlalu lama di UI
    try:
        conn = get_conn()
        conn.execute("UPDATE akun SET fresh_login_grace_minutes=15 WHERE COALESCE(fresh_login_grace_minutes,180) > 15")
        conn.execute("UPDATE akun SET fresh_login_health_floor=0 WHERE COALESCE(fresh_login_health_floor,80) > 0")
        conn.execute("UPDATE akun SET fresh_login_warming_floor=1 WHERE COALESCE(fresh_login_warming_floor,2) > 1")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Startup: gagal sinkron akun baru ({e})")

    # [7] Update level warming semua akun sesuai umur
    try:
        from utils.storage_db import get_semua_akun
        from core.warming import update_level_otomatis
        for akun in get_semua_akun():
            try:
                update_level_otomatis(akun["phone"])
            except Exception:
                pass
    except Exception:
        pass

    if config.has_telegram_credentials():
        try:
            print("Menghubungkan ulang akun...")
            run_sync(auto_reconnect_semua(), timeout=180)
        except Exception as exc:
            print(f"Startup warning: auto-reconnect gagal ({exc})")
    else:
        print("Startup: API_ID/API_HASH belum diatur, auto-reconnect dilewati.")


_safe_startup()

# Startup: blacklist grup forum yang sudah ada di database
try:
    from utils.database import get_conn as _get_conn_startup
    _conn_startup = _get_conn_startup()
    _r_forum = _conn_startup.execute("""
        UPDATE grup SET
            broadcast_status='blocked',
            broadcast_hold_reason='forum_group',
            is_forum=1
        WHERE is_forum=1
        AND COALESCE(broadcast_status,'hold') != 'blocked'
    """)
    _conn_startup.commit()
    _conn_startup.close()
    if _r_forum.rowcount > 0:
        print(f'Startup: {_r_forum.rowcount} grup forum diblacklist otomatis')
except Exception as _e_forum:
    print(f'Startup: gagal blacklist forum — {_e_forum}')


# ═══════════════════════════════════════════════════════════
# MESIN OTOMASI PENUH
# Kamu hanya perlu: scraper grup + isi keyword.
# Sisanya dikerjakan otomatis oleh 4 worker di bawah ini.
#
# Alur kerja otomatis:
#   [1] Scraper selesai → AUTO IMPORT hasil terbaik ke database
#   [2] Grup baru masuk → AUTO PERMISSION diberikan otomatis
#   [3] Grup punya permission → AUTO ASSIGN akun owner
#   [4] Grup siap → AUTO BROADCAST pakai draft aktif
# ═══════════════════════════════════════════════════════════

import time as _time
import json as _json
import random as _random
from datetime import datetime as _dt


def _setting(key, default=0):
    """Ambil setting dari database. Aman dari error."""
    try:
        from utils.settings_manager import get as _gs
        val = _gs(key, default)
        return val if val is not None else default
    except Exception:
        return default


def _setting_bool(key, default=0) -> bool:
    try:
        return bool(int(_setting(key, default) or 0))
    except Exception:
        return bool(default)


def _automation_allowed(engine_key: str | None = None, default: int = 0) -> bool:
    """Hormati pause global, maintenance, dan toggle engine per worker."""
    if _setting_bool("maintenance_mode", 0):
        return False
    if _setting_bool("pause_all_automation", 0):
        return False
    if engine_key:
        return _setting_bool(engine_key, default)
    return True


def _log(modul, aksi, pesan, **kw):
    """Tulis ke audit log. Tidak crash kalau gagal."""
    try:
        from utils.storage_db import add_audit_log
        add_audit_log("info", modul, aksi, pesan,
                      entity_type=kw.get("entity_type", modul),
                      entity_id=kw.get("entity_id", "auto"),
                      result="success")
    except Exception:
        pass


# ── WORKER 1: AUTO IMPORT ────────────────────────────────
# Cek setiap 30 detik. Kalau ada job scraper yang baru selesai
# dan auto_import_enabled=1, langsung import hasil terbaiknya.
def _worker_auto_import():
    _time.sleep(20)
    _sudah_diimpor = set()  # track job_id yang sudah diimpor sesi ini

    while True:
        try:
            if _automation_allowed("auto_import_enabled", 0):
                from utils.storage_db import get_scrape_jobs
                from services.scraper_service import import_scrape_results

                jobs = get_scrape_jobs(limit=20)
                for job in jobs:
                    if job["status"] != "done":
                        continue
                    jid = job["id"]
                    if jid in _sudah_diimpor:
                        continue
                    # Import semua yang belum diimpor, mode "all"
                    hasil = import_scrape_results(jid, mode="all")
                    jumlah = hasil.get("imported", 0)
                    _sudah_diimpor.add(jid)
                    if jumlah > 0:
                        _log("auto_import", "scrape_imported",
                             f"Auto import job #{jid}: {jumlah} grup masuk database",
                             entity_id=str(jid))
        except Exception as exc:
            _log("auto_import", "worker_error", f"Worker auto import error: {exc}")
        _time.sleep(30)


# ── WORKER 2: AUTO PERMISSION ─────────────────────────────
# Cek setiap 30 detik. Grup yang baru masuk (permission_status=unknown)
# langsung diberi permission "opt_in" secara otomatis.
def _worker_auto_permission():
    _time.sleep(25)

    while True:
        try:
            if _automation_allowed("auto_import_enabled", 0):  # ikut flag auto_import
                from utils.storage_db import get_semua_grup, create_permission, add_audit_log
                from utils.database import get_conn

                conn = get_conn()
                # Ambil grup yang belum punya permission
                rows = conn.execute(
                    """
                    SELECT g.id FROM grup g
                    WHERE (g.permission_status IS NULL OR g.permission_status = 'unknown')
                    AND g.status = 'active'
                    AND NOT EXISTS (
                        SELECT 1 FROM group_permission gp WHERE gp.group_id = g.id AND gp.status = 'valid'
                    )
                    LIMIT 200
                    """
                ).fetchall()
                conn.close()

                dibuat = 0
                for row in rows:
                    gid = int(row["id"])
                    create_permission(
                        gid, "opt_in",
                        approval_source="auto_system",
                        approved_by="system",
                        approved_at=_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                        notes="Diberikan otomatis oleh sistem",
                        status="valid",
                    )
                    # Update kolom permission_status di tabel grup
                    conn2 = get_conn()
                    conn2.execute(
                        "UPDATE grup SET permission_status='valid' WHERE id=%s", (gid,)
                    )
                    conn2.commit()
                    conn2.close()
                    dibuat += 1

                if dibuat > 0:
                    _log("auto_permission", "permission_bulk_granted",
                         f"Auto permission: {dibuat} grup diberi izin opt_in",
                         entity_id="bulk")
        except Exception as exc:
            _log("auto_permission", "worker_error", f"Worker auto permission error: {exc}")
        _time.sleep(30)


# ── WORKER 3: AUTO ASSIGN ─────────────────────────────────
# Cek setiap 60 detik. Grup yang punya permission valid tapi
# belum punya akun owner → assign akun terbaik otomatis.
def _worker_auto_assign():
    _time.sleep(15)

    while True:
        try:
            if _automation_allowed("auto_assign_enabled", 0):
                from utils.storage_db import get_semua_grup, create_assignment
                from routes.assignments_routes import _pick_best_candidate

                groups = [
                    g for g in get_semua_grup()
                    if (g.get("assignment_status") or "ready_assign") == "ready_assign"
                    and g.get("status") == "active"
                ]
                created = 0
                for group in groups[:150]:
                    best, candidates = _pick_best_candidate(int(group["id"]))
                    if not best:
                        continue
                    snapshot = _json.dumps(candidates[:5], ensure_ascii=False)
                    create_assignment(
                        int(group["id"]), str(best["account_id"]),
                        status="assigned",
                        assign_reason="auto_assign_background",
                        assign_score_snapshot=snapshot,
                    )
                    created += 1

                if created > 0:
                    _log("auto_assign", "auto_assign_background",
                         f"Auto assign: {created} grup dapat akun owner",
                         entity_id="bulk")
        except Exception as exc:
            _log("auto_assign", "worker_error", f"Worker auto assign error: {exc}")
        _time.sleep(60)


# ── WORKER 4: AUTO BROADCAST ──────────────────────────────
# Cek setiap 5 menit. Kalau ada akun online, draft aktif ada,
# dan auto_campaign_enabled=1 → broadcast ke semua grup aktif
# yang belum dikirim hari ini. Pakai jeda dari settings.
def _worker_auto_broadcast():
    _time.sleep(60)  # tunggu lebih lama — biarkan worker lain siap dulu

    while True:
        try:
            if _automation_allowed("auto_campaign_enabled", 0):
                from utils.storage_db import (
                    get_draft_aktif, get_grup_aktif,
                    sudah_dikirim_hari_ini, get_grup_by_akun
                )
                from services.account_manager import _clients
                from core.broadcast_session import buat_sesi, jalankan_sesi_thread, _sesi_aktif

                if not _clients:
                    _time.sleep(300)
                    continue

                if any((s.get("status") or "") in {"menunggu", "berjalan"} for s in _sesi_aktif.values()):
                    _time.sleep(300)
                    continue

                draft = get_draft_aktif()
                if not draft or not draft.get("isi"):
                    _time.sleep(300)
                    continue

                pesan    = draft["isi"]
                jeda_min = max(0, int(_setting("broadcast_jeda_min", 15)))
                jeda_max = max(jeda_min, int(_setting("broadcast_jeda_max", 40)))
                jeda     = _random.randint(jeda_min, jeda_max)

                # Ambil semua grup aktif — filter channel dan yang sudah dikirim
                semua_grup = get_grup_aktif()
                semua_grup = [
                    g for g in semua_grup
                    if (g.get("tipe") or "group") != "channel"   # skip channel
                    and not sudah_dikirim_hari_ini(g["id"])       # belum dikirim hari ini
                ]

                if not semua_grup:
                    _time.sleep(300)
                    continue

                phones = list(_clients.keys())

                # Untuk setiap akun, hanya kirim ke grup yang akun itu sudah JOIN
                # (ada di tabel akun_grup). Ini mencegah error "can't write in this chat"
                grup_per_akun = {}
                grup_sudah_ditugaskan = set()

                for phone in phones:
                    grup_akun = get_grup_by_akun(phone)  # grup yang akun ini sudah join
                    id_join   = {g["id"] for g in grup_akun}

                    daftar = [
                        {"id": g["id"], "nama": g["nama"]}
                        for g in semua_grup
                        if g["id"] in id_join
                        and g["id"] not in grup_sudah_ditugaskan
                    ]
                    if daftar:
                        grup_per_akun[phone] = daftar
                        grup_sudah_ditugaskan.update(g["id"] for g in daftar)

                # Grup yang tidak ada di akun_grup manapun → lewati
                if not grup_per_akun:
                    _time.sleep(300)
                    continue

                grup_list = [g for daftar in grup_per_akun.values() for g in daftar]
                sid = buat_sesi(phones, grup_list, pesan, jeda)

                if sid in _sesi_aktif:
                    _sesi_aktif[sid]["grup_per_akun"] = {
                        p: items for p, items in grup_per_akun.items()
                        if p in _clients
                    }

                jalankan_sesi_thread(sid, dict(_clients))
                _log("auto_broadcast", "auto_broadcast_started",
                     f"Auto broadcast: {len(grup_list)} grup (channel & belum join dilewati), jeda {jeda}s",
                     entity_id=sid)
        except Exception as exc:
            _log("auto_broadcast", "worker_error", f"Worker auto broadcast error: {exc}")
        _time.sleep(300)


# Jalankan orchestrator tunggal sebagai background thread.
# Worker lama dipertahankan sebagai fallback utilitas, tetapi tidak dijalankan
# agar tidak terjadi overlap state/queue dengan orchestrator baru.
start_orchestrator_worker()
start_last_chat_worker(interval_menit=10)  # Update last_chat otomatis tiap 10 menit
start_daily_reset_worker()                  # Reset stuck targets otomatis tiap tengah malam

def _start_akun_health_check_worker():
    import threading, time as _time

    def _jalankan_health_check():
        """Satu putaran pengecekan semua akun secara paralel (asyncio.gather)."""
        import asyncio as _asyncio
        import concurrent.futures as _cf
        from services.account_manager import _clients, _loop
        from utils.storage_db import tandai_akun_banned, tandai_akun_restricted

        _AUTH_SIGNALS = (
            'authkeyunregistered', 'userdeactivated', 'phonenumberbanned',
            'sessionrevoked', 'auth key', 'user deactivated',
            'account banned', 'your account has been',
        )
        _BLOCKED_SIGNALS = (
            'blocked', 'limited', 'violations',
            'terms of service', 'moderators',
            'your account has been', 'remain blocked',
        )
        _CLEAN_SIGNALS = (
            'no limits', 'good news', 'no limitations',
            'not limited', 'your account is not',
        )

        async def _cek_satu_akun(phone, client, semaphore):
            async with semaphore:
                # CEK 1: get_me() — deteksi deleted / restricted / auth error
                try:
                    me = await _asyncio.wait_for(client.get_me(), timeout=12)
                    if me is None:
                        return {'phone': phone, 'status': 'ok'}
                    if getattr(me, 'deleted', False):
                        return {'phone': phone, 'status': 'banned', 'alasan': 'akun dihapus Telegram'}
                    if getattr(me, 'restricted', False):
                        alasan_list = getattr(me, 'restriction_reason', []) or []
                        alasan_teks = ', '.join(
                            str(getattr(r, 'reason', '') or getattr(r, 'text', '') or str(r))
                            for r in alasan_list
                        ) if alasan_list else 'Akun dibatasi oleh Telegram'
                        return {'phone': phone, 'status': 'restricted', 'alasan': alasan_teks}
                except Exception as e:
                    err = str(e).lower()
                    if any(x in err for x in _AUTH_SIGNALS):
                        return {'phone': phone, 'status': 'banned', 'alasan': str(e)}
                    # Koneksi error biasa — jangan salah tandai
                    return {'phone': phone, 'status': 'ok'}

                # CEK 2: SpamBot — deteksi akun yang diblokir moderator Telegram
                try:
                    await client.send_message('SpamBot', '/start')
                    await _asyncio.sleep(3)
                    msgs = await client.get_messages('SpamBot', limit=2)
                    sb_reply = ''
                    for msg in (msgs or []):
                        if msg and msg.message:
                            sb_reply = msg.message.lower()
                            break
                    if any(x in sb_reply for x in _CLEAN_SIGNALS):
                        return {'phone': phone, 'status': 'ok'}
                    if any(x in sb_reply for x in _BLOCKED_SIGNALS):
                        return {'phone': phone, 'status': 'spam',
                                'alasan': 'Diblokir moderator Telegram (laporan pengguna) — SpamBot: ' + sb_reply[:120]}
                    # Balasan tidak dikenali — anggap normal
                    return {'phone': phone, 'status': 'ok'}
                except Exception as e:
                    err = str(e).lower()
                    if any(x in err for x in ('peerflood', 'peer_flood', 'flood')):
                        return {'phone': phone, 'status': 'spam',
                                'alasan': 'PeerFloodError saat hubungi SpamBot'}
                    # Gagal hubungi SpamBot karena network/timeout — jangan salah tandai
                    return {'phone': phone, 'status': 'ok'}

        async def _cek_semua_paralel(clients_snapshot):
            semaphore = _asyncio.Semaphore(20)
            tasks = [_cek_satu_akun(p, c, semaphore) for p, c in clients_snapshot]
            return await _asyncio.wait_for(
                _asyncio.gather(*tasks, return_exceptions=True),
                timeout=300
            )

        try:
            clients_snapshot = list(_clients.items())
            if not clients_snapshot:
                print("[HealthCheck] Tidak ada akun online, skip.")
                return
            print("[HealthCheck] Mengecek " + str(len(clients_snapshot)) + " akun secara paralel...")

            future = _asyncio.run_coroutine_threadsafe(_cek_semua_paralel(clients_snapshot), _loop)
            results = future.result(timeout=320)

            jumlah_cek = 0
            jumlah_banned = 0
            jumlah_restricted = 0
            jumlah_spam = 0

            for item in results:
                if isinstance(item, Exception):
                    jumlah_cek += 1
                    continue
                phone  = item.get('phone', '')
                status = item.get('status', 'ok')
                alasan = item.get('alasan', '')
                if status == 'banned':
                    if tandai_akun_banned(phone):
                        print("[HealthCheck] BANNED: " + phone + (" — " + alasan if alasan else ""))
                        jumlah_banned += 1
                    _clients.pop(phone, None)
                elif status == 'restricted':
                    if tandai_akun_restricted(phone, alasan):
                        print("[HealthCheck] RESTRICTED: " + phone + " — " + alasan)
                        jumlah_restricted += 1
                elif status == 'spam':
                    alasan_spam = 'Diblokir moderator — SpamBot: ' + alasan if alasan else 'SpamBot restriction'
                    if tandai_akun_restricted(phone, alasan_spam):
                        print("[HealthCheck] SPAM/BLOKIR: " + phone + " — " + alasan[:120])
                        jumlah_spam += 1
                else:
                    jumlah_cek += 1

            ringkasan = str(jumlah_cek) + " akun OK"
            if jumlah_banned:
                ringkasan += ", " + str(jumlah_banned) + " banned"
            if jumlah_restricted:
                ringkasan += ", " + str(jumlah_restricted) + " dibatasi"
            if jumlah_spam:
                ringkasan += ", " + str(jumlah_spam) + " kena spam limit"
            if jumlah_banned == 0 and jumlah_restricted == 0 and jumlah_spam == 0:
                ringkasan += ", tidak ada masalah"
            print("[HealthCheck] Selesai: " + ringkasan)

            # Kalau ada akun bermasalah, langsung jalankan cleanup tanpa tunggu orchestrator
            ada_masalah = jumlah_banned + jumlah_restricted + jumlah_spam
            if ada_masalah > 0:
                try:
                    from services.orchestrator_service import _cleanup_banned_accounts
                    hasil = _cleanup_banned_accounts(max_reassign=50)
                    print("[HealthCheck] Cleanup langsung: sender_reset=" + str(hasil.get('sender_reset', 0))
                          + ", owner_reassigned=" + str(hasil.get('owner_reassigned', 0))
                          + ", managed_reset=" + str(hasil.get('managed_reset', 0))
                          + ", managed_no_candidate=" + str(hasil.get('managed_no_candidate', 0)))
                except Exception as _ce:
                    print("[HealthCheck] Cleanup gagal: " + str(_ce))

        except _cf.TimeoutError:
            print("[HealthCheck] Timeout: batch pengecekan melebihi 320 detik")
        except Exception as _ex:
            print("[HealthCheck] Error saat cek: " + str(_ex))

    def _worker():
        # Langsung cek saat startup — tidak perlu tunggu 30 menit dulu
        # Ini menutup celah: akun yang dibanned saat aplikasi mati
        # akan langsung ketahuan begitu aplikasi dinyalakan kembali
        _time.sleep(10)  # Beri jeda 10 detik agar semua akun selesai connect dulu
        print("[HealthCheck] Menjalankan pengecekan awal...")
        _jalankan_health_check()

        # Setelah itu jalan rutin tiap 30 menit
        while True:
            _time.sleep(1800)
            print("[HealthCheck] Menjalankan pengecekan rutin...")
            _jalankan_health_check()

    t = threading.Thread(target=_worker, daemon=True, name='AkunHealthCheck')
    t.start()
    print("[HealthCheck] Worker dimulai — cek awal dalam 10 detik, lalu rutin tiap 30 menit")
_start_akun_health_check_worker()

# ── STATUS & TOGGLE ENDPOINT ──────────────────────────────

@app.route("/")
def index():
    return send_from_directory(config.get_frontend_dir(), "index.html")


@app.route("/<path:f>")
def static_files(f):
    frontend_dir = config.get_frontend_dir()
    from pathlib import Path

    if f.startswith('api/'):
        raise NotFound()

    target = Path(frontend_dir) / f
    if target.is_file():
        return send_from_directory(frontend_dir, f)

    if '.' not in Path(f).name:
        return send_from_directory(frontend_dir, 'index.html')

    raise NotFound()


@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify(
        {
            "ok": True,
            "online_accounts": len(_clients),
            "db": config.DB_FILE,
            "frontend": config.get_frontend_dir(),
        }
    )




@app.route("/api/flow", methods=["GET"])
def api_flow():
    from services.overview_service import get_pipeline_flow
    return jsonify(get_pipeline_flow())
# ── AKUN ──────────────────────────────────────────────────
@app.route("/api/akun", methods=["GET"])
def api_get_akun():
    from core.warming import get_daily_capacity, get_info_warming
    from utils.storage_db import get_grup_by_akun
    from utils.database import get_conn as _gc
    from datetime import datetime as _dt
    data = get_semua_akun()

    def _hitung_spam_indicator(phone: str) -> dict:
        """Hitung indikator spam/restrict berdasarkan riwayat pengiriman 3 hari terakhir."""
        try:
            conn2 = _gc()
            # Ambil riwayat 3 hari terakhir
            rows = conn2.execute(
                """SELECT status FROM riwayat
                   WHERE phone=%s AND waktu >= NOW() - INTERVAL '3 days'
                   ORDER BY id DESC LIMIT 100""",
                (phone,)
            ).fetchall()
            # Hitung blocked recent (1 hari)
            rows_1d = conn2.execute(
                """SELECT status FROM riwayat
                   WHERE phone=%s AND waktu >= NOW() - INTERVAL '1 day'
                   ORDER BY id DESC LIMIT 50""",
                (phone,)
            ).fetchall()
            # Cek cooldown aktif
            akun_row = conn2.execute(
                "SELECT cooldown_until, total_flood, total_banned FROM akun WHERE phone=%s",
                (phone,)
            ).fetchone()
            conn2.close()

            statuses = [r[0] for r in rows]
            statuses_1d = [r[0] for r in rows_1d]

            total = len(statuses)
            total_1d = len(statuses_1d)
            blocked = sum(1 for s in statuses if s in ('send_blocked','blocked','tidak_punya_izin'))
            failed = sum(1 for s in statuses if s in ('send_failed','gagal','join_failed'))
            blocked_1d = sum(1 for s in statuses_1d if s in ('send_blocked','blocked','tidak_punya_izin'))
            failed_1d = sum(1 for s in statuses_1d if s in ('send_failed','gagal','join_failed'))
            success_1d = sum(1 for s in statuses_1d if s in ('berhasil','send_success'))

            # Cek cooldown aktif
            cooldown_aktif = False
            cooldown_until = ''
            if akun_row and akun_row[0]:
                try:
                    cd = _dt.strptime(str(akun_row[0])[:19], '%Y-%m-%d %H:%M:%S')
                    if cd > _dt.now():
                        cooldown_aktif = True
                        cooldown_until = akun_row[0][:16]
                except Exception:
                    pass

            total_flood = int((akun_row[1] if akun_row else 0) or 0)
            total_banned_grup = int((akun_row[2] if akun_row else 0) or 0)

            # Tentukan level indikator
            # 🔴 Curiga kena restrict/spam
            if (blocked_1d >= 3 and success_1d == 0) or (total_1d > 0 and blocked_1d / max(total_1d, 1) > 0.6):
                level = 'curiga'
                label = '🔴 Curiga Restrict'
                keterangan = f'{blocked_1d} gagal kirim berturut-turut hari ini'
            # 🟡 Waspada — ada tanda-tanda masalah
            elif cooldown_aktif or (blocked_1d >= 1 and success_1d <= blocked_1d) or total_flood >= 5:
                level = 'waspada'
                label = '🟡 Waspada'
                if cooldown_aktif:
                    keterangan = f'Cooldown aktif s/d {cooldown_until}'
                elif total_flood >= 5:
                    keterangan = f'Sering kena FloodWait ({total_flood}x total)'
                else:
                    keterangan = f'{blocked_1d} gagal, {success_1d} berhasil hari ini'
            # 🟢 Sehat
            else:
                level = 'sehat'
                label = '🟢 Sehat'
                keterangan = f'{success_1d} berhasil hari ini' if success_1d > 0 else 'Belum ada aktivitas'

            return {
                'spam_level': level,
                'spam_label': label,
                'spam_keterangan': keterangan,
                'spam_blocked_1d': blocked_1d,
                'spam_failed_1d': failed_1d,
                'spam_success_1d': success_1d,
                'cooldown_aktif': cooldown_aktif,
                'cooldown_until': cooldown_until,
                'total_flood': total_flood,
                'total_banned_grup': total_banned_grup,
            }
        except Exception:
            return {
                'spam_level': 'tidak_diketahui',
                'spam_label': '⚪ Tidak Diketahui',
                'spam_keterangan': '-',
                'spam_blocked_1d': 0,
                'spam_failed_1d': 0,
                'spam_success_1d': 0,
                'cooldown_aktif': False,
                'cooldown_until': '',
                'total_flood': 0,
                'total_banned_grup': 0,
            }

    for akun in data:
        phone = akun["phone"]
        akun["online"]       = phone in _clients
        akun["label_score"]  = get_label_akun(akun.get("score", 0))
        info = get_info_warming(phone)
        akun["label_level"]  = info.get("label_level", "")
        akun["umur_hari"]    = info.get("umur_hari", 0)
        cap = get_daily_capacity(phone)
        akun["maks_kirim"]   = cap.get("kirim", {}).get("limit", info.get("maks_kirim", 0))
        akun["maks_join"]    = cap.get("join", {}).get("limit", info.get("maks_join", 0))
        akun["sudah_kirim"]  = cap.get("kirim", {}).get("used", 0)
        akun["sudah_join"]   = cap.get("join", {}).get("used", 0)
        akun["sisa_kirim"]   = cap.get("kirim", {}).get("remaining", 0)
        akun["sisa_join"]    = cap.get("join", {}).get("remaining", 0)
        akun["jumlah_grup"]  = len(get_grup_by_akun(phone))
        akun["manual_health_override_enabled"] = bool(akun.get("manual_health_override_enabled", 0))
        akun["manual_warming_override_enabled"] = bool(akun.get("manual_warming_override_enabled", 0))
        akun["fresh_login_grace_enabled"] = bool(akun.get("fresh_login_grace_enabled", 1))
        # Tambah indikator spam/restrict
        akun.update(_hitung_spam_indicator(phone))
    return jsonify(data)


@app.route("/api/akun/login", methods=["POST"])
def api_login():
    body = _body()
    phone = body.get("phone")
    if not phone:
        return _error("Nomor HP wajib")
    return jsonify(run_sync(login_akun(phone)))


@app.route("/api/akun/otp", methods=["POST"])
def api_otp():
    body = _body()
    phone = body.get("phone")
    kode = body.get("kode")
    if not phone or not kode:
        return _error("phone dan kode wajib")
    return jsonify(run_sync(submit_otp(phone, kode, body.get("password"))))


@app.route("/api/akun/logout", methods=["POST"])
def api_logout():
    phone = _body().get("phone")
    if not phone:
        return _error("phone wajib")
    return jsonify(run_sync(logout_akun(phone)))


@app.route("/api/akun/hapus", methods=["POST"])
def api_hapus_akun():
    phone = _body().get("phone")
    if not phone:
        return _error("phone wajib")
    return jsonify(run_sync(delete_akun_permanen(phone)))


@app.route("/api/akun/status", methods=["POST"])
def api_status_akun():
    body = _body()
    try:
        set_status_akun(_require(body, "phone", "phone"), _require(body, "status", "status"))
    except ValueError as exc:
        return _error(str(exc))
    return jsonify({"ok": True})


@app.route("/api/akun/pulihkan", methods=["POST"])
def api_pulihkan_akun():
    phone = _body().get("phone")
    if not phone:
        return _error("phone wajib")
    set_status_akun(phone, "active")
    return jsonify({"ok": True})


@app.route("/api/akun/tersedia", methods=["GET"])
def api_akun_tersedia():
    return jsonify(pilih_akun_tersedia(_clients))


@app.route("/api/akun/ringkasan", methods=["GET"])
def api_ringkasan_akun():
    return jsonify(ringkasan_akun([a["phone"] for a in get_semua_akun()]))


@app.route("/api/akun/<phone>/score", methods=["POST"])
def api_score_akun(phone):
    score = update_score_akun(phone)
    return jsonify({"ok": True, "score": score})


@app.route("/api/akun/<phone>/score/manual", methods=["POST"])
def api_score_akun_manual(phone):
    try:
        score = _as_int(_body().get("score", 0), "score")
    except ValueError as exc:
        return _error(str(exc))
    set_score_akun(phone, score)
    return jsonify({"ok": True})


@app.route("/api/akun/<phone>/level", methods=["POST"])
def api_level_akun(phone):
    try:
        level = _as_int(_body().get("level", 1), "level")
    except ValueError as exc:
        return _error(str(exc))
    set_level_warming(phone, level)
    return jsonify({"ok": True})


@app.route("/api/akun/<phone>/health", methods=["POST"])
def api_set_health_score(phone):
    """Edit health_score akun secara manual (0-100)."""
    try:
        health = _as_int(_body().get("health_score", 100), "health_score")
        health = max(0, min(100, health))
    except ValueError as exc:
        return _error(str(exc))
    from utils.database import get_conn
    conn = get_conn()
    conn.execute("UPDATE akun SET health_score=%s WHERE phone=%s", (health, phone))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "health_score": health})


@app.route("/api/akun/<phone>/warming", methods=["GET"])
def api_warming(phone):
    return jsonify(get_info_warming(phone))

@app.route("/api/akun/<phone>/config", methods=["GET"])
def api_get_akun_config(phone):
    row = next((a for a in get_semua_akun() if a['phone'] == phone), None)
    if not row:
        return _error("Akun tidak ditemukan", 404)
    return jsonify({
        "phone": row["phone"],
        "nama": row.get("nama") or row["phone"],
        "auto_assign_enabled": bool(row.get("auto_assign_enabled", 1)),
        "priority_weight": row.get("priority_weight", 100),
        "daily_new_group_cap": row.get("daily_new_group_cap", 10),
        "manual_health_override_enabled": bool(row.get("manual_health_override_enabled", 0)),
        "manual_health_override_score": row.get("manual_health_override_score", 80),
        "manual_warming_override_enabled": bool(row.get("manual_warming_override_enabled", 0)),
        "manual_warming_override_level": row.get("manual_warming_override_level", 2),
        "fresh_login_grace_enabled": bool(row.get("fresh_login_grace_enabled", 1)),
        "fresh_login_grace_minutes": row.get("fresh_login_grace_minutes", 180),
        "fresh_login_health_floor": row.get("fresh_login_health_floor", 80),
        "fresh_login_warming_floor": row.get("fresh_login_warming_floor", 2),
        "assignment_notes": row.get("assignment_notes") or "",
        "last_login_at": row.get("last_login_at"),
    })


@app.route("/api/akun/<phone>/config", methods=["POST"])
def api_set_akun_config(phone):
    payload = _body()
    row = next((a for a in get_semua_akun() if a['phone'] == phone), None)
    if not row:
        return _error("Akun tidak ditemukan", 404)
    from utils.database import get_conn
    allowed = {
        "auto_assign_enabled", "priority_weight", "daily_new_group_cap",
        "manual_health_override_enabled", "manual_health_override_score",
        "manual_warming_override_enabled", "manual_warming_override_level",
        "fresh_login_grace_enabled", "fresh_login_grace_minutes",
        "fresh_login_health_floor", "fresh_login_warming_floor",
        "assignment_notes",
        # Edit langsung nilai score dan health_score
        "score", "health_score",
    }
    parts, values = [], []
    for key, value in payload.items():
        if key in allowed:
            parts.append(f"{key}=%s")
            values.append(value)
    if not parts:
        return _error("Tidak ada konfigurasi yang diubah")
    values.append(phone)
    conn = get_conn()
    conn.execute(f"UPDATE akun SET {', '.join(parts)} WHERE phone=%s", values)
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ── GRUP ──────────────────────────────────────────────────
@app.route("/api/grup", methods=["GET"])
def api_get_grup():
    return jsonify(get_semua_grup())


@app.route("/api/grup/aktif", methods=["GET"])
def api_grup_aktif():
    return jsonify(get_grup_aktif())


@app.route("/api/grup/hot", methods=["GET"])
def api_grup_hot():
    return jsonify(get_grup_hot())


@app.route("/api/grup/analisis", methods=["GET"])
def api_grup_analisis():
    from services.group_send_guard import annotate_group_row

    return jsonify([annotate_group_row(row) for row in get_semua_analisis()])


@app.route("/api/grup/fetch", methods=["POST"])
def api_fetch_grup():
    phone = _body().get("phone")
    if not phone:
        return _error("Pilih akun")
    hasil = run_sync(fetch_grup_dari_akun(phone))
    simpan_banyak_grup(hasil)
    return jsonify(hasil)


@app.route("/api/grup/fetch/baru", methods=["POST"])
def api_fetch_grup_baru():
    phone = _body().get("phone")
    if not phone:
        return _error("Pilih akun")
    semua = run_sync(fetch_grup_dari_akun(phone))
    baru = [g for g in semua if not grup_sudah_ada(g["id"])]
    return jsonify(baru)


@app.route("/api/grup/tambah", methods=["POST"])
def api_tambah_grup():
    body = _body()
    try:
        grup_id = _as_int(_require(body, "id", "id grup"), "id grup")
        nama = _require(body, "nama", "nama grup")
    except ValueError as exc:
        return _error(str(exc))

    grup = {
        "id": grup_id,
        "nama": nama,
        "username": body.get("username"),
        "tipe": body.get("tipe") or "group",
        "jumlah_member": _as_int(body.get("jumlah_member", 0), "jumlah_member") if body.get("jumlah_member") not in (None, "") else 0,
        "link": body.get("link"),
        "status": body.get("status") or "active",
        "sumber": body.get("sumber") or "manual",
    }
    simpan_banyak_grup([grup], sumber=grup["sumber"])
    return jsonify({"ok": True, "grup": grup})


@app.route("/api/grup/tambah-manual", methods=["POST"])
def api_tambah_grup_manual():
    body = _body()
    phone = body.get("phone")
    link = (body.get("link") or "").strip()
    if not phone or not link:
        return _error("phone dan link wajib")
    client = _clients.get(phone)
    if not client:
        return _error("Akun tidak aktif")

    async def _fetch():
        username = link.replace("https://t.me/", "").replace("@", "").strip("/")
        try:
            entity = await client.get_entity(username)
            return {
                "id": entity.id,
                "nama": entity.title,
                "username": getattr(entity, "username", None),
                "tipe": "supergroup" if getattr(entity, "megagroup", False) else "channel",
                "jumlah_member": getattr(entity, "participants_count", 0),
                "link": link,
                "status": "active",
                "sumber": "manual",
            }
        except Exception as exc:
            return {"error": str(exc)}

    hasil = run_sync(_fetch())
    if "error" in hasil:
        return jsonify(hasil), 400
    simpan_banyak_grup([hasil], sumber="manual")
    return jsonify({"ok": True, "grup": hasil})


@app.route("/api/grup/status", methods=["POST"])
def api_status_grup():
    body = _body()
    try:
        grup_id = _as_int(_require(body, "grup_id", "grup_id"), "grup_id")
        status = _require(body, "status", "status")
    except ValueError as exc:
        return _error(str(exc))
    set_status_grup(grup_id, status)
    return jsonify({"ok": True})


@app.route("/api/grup/pulihkan", methods=["POST"])
def api_pulihkan_grup():
    try:
        grup_id = _as_int(_body().get("grup_id"), "grup_id")
    except ValueError as exc:
        return _error(str(exc))
    set_status_grup(grup_id, "active")
    return jsonify({"ok": True})


@app.route("/api/grup/<int:gid>/score/manual", methods=["POST"])
def api_score_grup_manual(gid):
    try:
        score = _as_int(_body().get("score", 0), "score")
    except ValueError as exc:
        return _error(str(exc))
    label = get_label_grup(score)
    set_score_grup(gid, score, label)
    return jsonify({"ok": True, "label": label})


@app.route("/api/grup/<int:gid>/last-chat", methods=["POST"])
def api_last_chat(gid):
    phone = _body().get("phone")
    client = _clients.get(phone) if phone else next(iter(_clients.values()), None)
    if not client:
        return _error("Tidak ada akun aktif")
    waktu = run_sync(fetch_last_chat(client, gid))
    return jsonify({"ok": True, "last_chat": waktu})


@app.route("/api/grup/score/update-semua", methods=["POST"])
def api_update_score_semua():
    update_semua_score()
    return jsonify({"ok": True})



# ── SCRAPER ───────────────────────────────────────────────
@app.route("/api/scraper/preview", methods=["POST"])
def api_scraper_preview():
    body = _body()
    try:
        keywords = _require(body, "keywords", "keyword")
        options = body.get("options") or {}
        return jsonify(preview_scrape_keywords(keywords, options))
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


@app.route("/api/scraper/jobs", methods=["GET"])
def api_scraper_jobs():
    limit = request.args.get("limit", default=20, type=int) or 20
    return jsonify(get_scrape_jobs(limit=min(max(limit, 1), 100)))


@app.route("/api/scraper/jobs/<int:job_id>", methods=["GET"])
def api_scraper_job(job_id):
    job = get_scrape_job(job_id)
    if not job:
        return _error("Job scraper tidak ditemukan", 404)
    return jsonify(job)


@app.route("/api/scraper/jobs/<int:job_id>/keywords", methods=["GET"])
def api_scraper_keywords(job_id):
    return jsonify(get_scrape_keyword_runs(job_id))


@app.route("/api/scraper/jobs/<int:job_id>/results", methods=["GET"])
def api_scraper_results(job_id):
    only_recommended = request.args.get("recommended", "0") == "1"
    only_new = request.args.get("new", "0") == "1"
    include_imported = request.args.get("include_imported", "1") == "1"
    return jsonify(
        get_scrape_results(
            job_id,
            only_recommended=only_recommended,
            only_new=only_new,
            include_imported=include_imported,
        )
    )


@app.route("/api/scraper/start", methods=["POST"])
def api_scraper_start():
    body = _body()
    try:
        phone = _require(body, "phone", "akun scraper")
        keywords = _require(body, "keywords", "keyword")
        options = body.get("options") or {}
        job = start_scrape_job(phone, keywords, options)
        return jsonify({"ok": True, "job": job})
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


@app.route("/api/scraper/jobs/<int:job_id>/pause", methods=["POST"])
def api_scraper_pause(job_id):
    try:
        return jsonify({"ok": True, "job": control_scrape_job(job_id, "pause")})
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


@app.route("/api/scraper/jobs/<int:job_id>/resume", methods=["POST"])
def api_scraper_resume(job_id):
    try:
        return jsonify({"ok": True, "job": control_scrape_job(job_id, "resume")})
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


@app.route("/api/scraper/jobs/<int:job_id>/stop", methods=["POST"])
def api_scraper_stop(job_id):
    try:
        return jsonify({"ok": True, "job": control_scrape_job(job_id, "stop")})
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


@app.route("/api/scraper/jobs/<int:job_id>/retry-failed", methods=["POST"])
def api_scraper_retry_failed(job_id):
    try:
        return jsonify({"ok": True, "job": control_scrape_job(job_id, "retry_failed")})
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


@app.route("/api/scraper/import", methods=["POST"])
def api_scraper_import():
    body = _body()
    try:
        job_id = _as_int(_require(body, "job_id", "job_id"), "job_id")
        result_ids = body.get("result_ids") or []
        result_ids = [_as_int(x, "result_id") for x in result_ids]
        mode = body.get("mode") or ("selected" if result_ids else "recommended")
        hasil = import_scrape_results(job_id, result_ids=result_ids, mode=mode)
        return jsonify(hasil)
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(str(exc), 500)


# ── DRAFT# ── DRAFT ─────────────────────────────────────────────────
@app.route("/api/draft", methods=["GET"])
def api_get_draft():
    return jsonify(get_semua_draft())


@app.route("/api/draft/aktif", methods=["GET"])
def api_draft_aktif():
    return jsonify(get_draft_aktif() or {})


@app.route("/api/draft", methods=["POST"])
def api_post_draft():
    body = _body()
    try:
        judul = _require(body, "judul", "judul")
        isi = _require(body, "isi", "isi")
    except ValueError as exc:
        return _error(str(exc))
    return jsonify(simpan_draft(judul, isi))


@app.route("/api/draft/<int:did>/aktif", methods=["POST"])
def api_set_draft_aktif(did):
    from utils.storage_db import set_draft_aktif

    set_draft_aktif(did)
    return jsonify({"ok": True})


@app.route("/api/draft/<int:did>", methods=["DELETE"])
def api_del_draft(did):
    hapus_draft(did)
    return jsonify({"ok": True})


# ── KIRIM ─────────────────────────────────────────────────
@app.route("/api/pesan/kirim", methods=["POST"])
def api_kirim():
    body = _body()
    phone = body.get("phone")
    pesan = body.get("pesan")
    try:
        grup_id = _as_int(body.get("grup_id"), "grup_id")
    except ValueError as exc:
        return _error(str(exc))
    if not all([phone, pesan]):
        return _error("phone, grup_id, pesan wajib")
    return jsonify(run_sync(kirim_pesan_manual(phone, grup_id, pesan)))


@app.route("/api/pesan/log", methods=["GET"])
def api_log():
    return jsonify(get_riwayat_hari_ini())


# ── ANTRIAN ───────────────────────────────────────────────
@app.route("/api/antrian", methods=["GET"])
def api_get_antrian():
    return jsonify(get_semua_antrian())


@app.route("/api/antrian", methods=["POST"])
def api_post_antrian():
    body = _body()
    try:
        phone = _require(body, "phone", "phone")
        grup_id = _as_int(_require(body, "grup_id", "grup_id"), "grup_id")
        pesan = _require(body, "pesan", "pesan")
    except ValueError as exc:
        return _error(str(exc))
    return jsonify(tambah_antrian(phone, grup_id, pesan))


@app.route("/api/antrian/<int:iid>/kirim", methods=["POST"])
def api_kirim_antrian(iid):
    antrian = get_semua_antrian()
    item = next((a for a in antrian if a["id"] == iid), None)
    if not item:
        return _error("Tidak ditemukan", 404)
    hasil = run_sync(kirim_pesan_manual(item["phone"], item["grup_id"], item["pesan"]))
    update_status_antrian(iid, "terkirim" if hasil.get("status") == "berhasil" else "gagal")
    return jsonify(hasil)


@app.route("/api/antrian/<int:iid>", methods=["DELETE"])
def api_del_antrian(iid):
    hapus_antrian(iid)
    return jsonify({"ok": True})


# ── RIWAYAT ───────────────────────────────────────────────
@app.route("/api/riwayat", methods=["GET"])
def api_riwayat():
    return jsonify(get_riwayat_hari_ini())


@app.route("/api/riwayat/ringkasan", methods=["GET"])
def api_ringkasan():
    return jsonify(get_ringkasan_hari_ini())


@app.route("/api/riwayat/cek/<int:gid>", methods=["GET"])
def api_cek_kirim(gid):
    return jsonify({"sudah_dikirim": sudah_dikirim_hari_ini(gid)})


# ── BROADCAST ─────────────────────────────────────────────
@app.route("/api/broadcast/mulai", methods=["POST"])
def api_broadcast_mulai():
    body = _body()
    pesan = body.get("pesan")
    try:
        jeda = _as_int(body.get("jeda", 30), "jeda")
    except ValueError as exc:
        return _error(str(exc))

    # Frontend baru kirim grup_per_akun (dict phone → list grup)
    # Frontend lama kirim grup_list (flat list)
    grup_per_akun = body.get("grup_per_akun") or {}
    grup_list     = body.get("grup_list", []) or []

    # Bangun grup_list dari grup_per_akun kalau grup_list kosong
    if not grup_list and grup_per_akun:
        for items in grup_per_akun.values():
            grup_list.extend(items)

    if not pesan:
        return _error("Isi pesan wajib")
    if not grup_list:
        return _error("Pilih minimal 1 grup")
    if not _clients:
        return _error("Tidak ada akun online")

    sid = buat_sesi(list(_clients.keys()), grup_list, pesan, jeda)

    # Simpan grup_per_akun ke sesi agar broadcast_session bisa pakai mode per-akun
    from core.broadcast_session import _sesi_aktif
    if sid in _sesi_aktif and grup_per_akun:
        _sesi_aktif[sid]["grup_per_akun"] = {
            phone: items for phone, items in grup_per_akun.items()
            if phone in _clients
        }

    jalankan_sesi_thread(sid, dict(_clients))
    return jsonify({"ok": True, "session_id": sid, "pesan": f"Mulai ke {len(grup_list)} grup"})


@app.route("/api/broadcast/status/<sid>", methods=["GET"])
def api_broadcast_status(sid):
    sesi = get_sesi(sid)
    if not sesi:
        return _error("Tidak ditemukan", 404)
    return jsonify({k: sesi[k] for k in ["session_id", "daftar_phone", "status", "total", "selesai", "countdown", "hasil", "mulai"] if k in sesi})


@app.route("/api/broadcast/stop/<sid>", methods=["POST"])
def api_broadcast_stop(sid):
    stop_sesi(sid)
    return jsonify({"ok": True})


@app.route("/api/broadcast/semua", methods=["GET"])
def api_broadcast_semua():
    return jsonify([{k: s[k] for k in ["session_id", "status", "total", "selesai", "mulai", "daftar_phone"] if k in s} for s in get_semua_sesi()])


@app.route("/api/broadcast/hapus/<sid>", methods=["DELETE"])
def api_broadcast_hapus(sid):
    hapus_sesi(sid)
    return jsonify({"ok": True})


# ── SINKRONISASI ──────────────────────────────────────────
def _run_sync_task(sid, client):
    asyncio.run_coroutine_threadsafe(jalankan_sync(sid, client), _loop)


@app.route("/api/sync/mulai", methods=["POST"])
def api_sync_mulai():
    phone = _body().get("phone")
    if not phone:
        return _error("phone wajib")
    client = _clients.get(phone)
    if not client:
        return _error("Akun tidak aktif")
    semua_hot = get_grup_hot()
    if not semua_hot:
        return _error("Tidak ada grup Hot di database")
    sid = buat_sesi_sync(phone, semua_hot)
    threading.Thread(target=_run_sync_task, args=(sid, client), daemon=True).start()
    return jsonify({"ok": True, "session_id": sid, "total": len(semua_hot)})


@app.route("/api/sync/status/<sid>", methods=["GET"])
def api_sync_status(sid):
    sesi = get_sesi_sync(sid)
    if not sesi:
        return _error("Tidak ditemukan", 404)
    return jsonify(sesi)


@app.route("/api/sync/stop/<sid>", methods=["POST"])
def api_sync_stop(sid):
    stop_sesi_sync(sid)
    return jsonify({"ok": True})


# ── SETTINGS ──────────────────────────────────────────────
@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    return jsonify(get_semua_settings())


@app.route("/api/settings", methods=["POST"])
def api_update_settings():
    update_banyak(_normalize_settings_payload(_body()))
    return jsonify({"ok": True})


# ── RELASI AKUN-GRUP ──────────────────────────────────────
@app.route("/api/grup/by-akun/<phone>", methods=["GET"])
def api_grup_by_akun(phone):
    return jsonify(get_grup_by_akun(phone))


@app.route("/api/grup/<int:gid>/akun", methods=["GET"])
def api_akun_by_grup(gid):
    return jsonify(get_akun_by_grup(gid))


@app.route("/api/grup/status/massal", methods=["POST"])
def api_status_grup_massal():
    body = _body()
    grup_ids = body.get("grup_ids", []) or []
    status = body.get("status", "active")
    set_status_grup_massal(grup_ids, status)
    return jsonify({"ok": True})


# ── AUTOMATION CONTROL ENDPOINTS ─────────────────────────
@app.route("/api/auto-assign/status", methods=["GET"])
def api_auto_assign_status():
    from utils.settings_manager import get as _gs
    aktif = bool(int(_gs("auto_assign_enabled", 0) or 0))
    return jsonify({"enabled": aktif, "interval_detik": 60})


@app.route("/api/auto-assign/toggle", methods=["POST"])
def api_auto_assign_toggle():
    from utils.settings_manager import set as _ss, get as _gs
    body = _body()
    aktif = bool(body["enabled"]) if "enabled" in body else not bool(int(_gs("auto_assign_enabled", 0) or 0))
    _ss("auto_assign_enabled", "1" if aktif else "0", label="Automation: auto assign aktif", tipe="boolean")
    return jsonify({"ok": True, "enabled": aktif,
                    "pesan": "Auto assign diaktifkan" if aktif else "Auto assign dimatikan"})


@app.route("/api/automation/status", methods=["GET"])
def api_automation_status():
    """Status lengkap semua mesin otomasi."""
    from utils.settings_manager import get as _gs
    return jsonify({
        "auto_import":   bool(int(_gs("auto_import_enabled",   0) or 0)),
        "auto_permission": bool(int(_gs("auto_permission_enabled", 0) or 0)),
        "auto_assign":   bool(int(_gs("auto_assign_enabled",   0) or 0)),
        "auto_join":     bool(int(_gs("auto_join_enabled",     0) or 0)),
        "auto_campaign": bool(int(_gs("auto_campaign_enabled", 0) or 0)),
        "auto_recovery": bool(int(_gs("auto_recovery_enabled", 1) or 0)),
        "interval": {"import_detik": 30, "assign_detik": 30, "broadcast_detik": 30},
    })


@app.route("/api/automation/toggle-all", methods=["POST"])
def api_automation_toggle_all():
    """Aktifkan atau matikan seluruh card otomasi sekaligus."""
    from utils.settings_manager import set as _ss
    aktif = bool(_body().get("enabled", True))
    val = "1" if aktif else "0"
    _ss("auto_import_enabled",   val, label="Automation: auto import aktif",   tipe="boolean")
    _ss("auto_permission_enabled", val, label="Automation: auto permission aktif", tipe="boolean")
    _ss("auto_assign_enabled",   val, label="Automation: auto assign aktif",   tipe="boolean")
    _ss("auto_join_enabled",    val, label="Automation: auto join aktif", tipe="boolean")
    _ss("auto_campaign_enabled", val, label="Automation: auto campaign aktif", tipe="boolean")
    _ss("auto_recovery_enabled", val, label="Automation: auto recovery aktif", tipe="boolean")
    _ss("pause_all_automation",  "0" if aktif else "1", label="System: pause all automation", tipe="boolean")
    return jsonify({"ok": True, "enabled": aktif,
                    "pesan": "Semua otomasi diaktifkan" if aktif else "Semua otomasi dimatikan"})


@app.route("/api/automation/toggle", methods=["POST"])
def api_automation_toggle():
    """Toggle satu mesin tertentu: import | assign | campaign | recovery."""
    from utils.settings_manager import set as _ss, get as _gs
    body = _body()
    mesin = body.get("mesin", "")
    _map = {
        "import":   "auto_import_enabled",
        "permission": "auto_permission_enabled",
        "assign":   "auto_assign_enabled",
        "autojoin": "auto_join_enabled",
        "campaign": "auto_campaign_enabled",
        "recovery": "auto_recovery_enabled",
    }
    if mesin not in _map:
        return _error("mesin tidak dikenal. Pilih: import, permission, assign, autojoin, campaign, recovery")
    key = _map[mesin]
    aktif = bool(body["enabled"]) if "enabled" in body else not bool(int(_gs(key, 0) or 0))
    _ss(key, "1" if aktif else "0")
    return jsonify({"ok": True, "mesin": mesin, "enabled": aktif})


if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=5000)