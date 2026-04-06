from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from services.account_manager import _clients, run_sync
from services.message_service import kirim_pesan_manual
from services.scraper_service import _ACTIVE_THREADS, control_scrape_job, import_scrape_results
from services.automation_rule_engine import get_rule_overview, record_stage_result, resolve_stage_rules
from services.group_send_guard import evaluate_group_send_guard, persist_group_send_guard
from utils.database import get_conn
from utils.settings_manager import get as get_setting, get_int
from utils.storage_db import (
    add_audit_log,
    tandai_grup_masa_istirahat,
    tandai_akun_banned,
    get_next_join_at,
    set_next_join_at,
    create_assignment,
    create_campaign,
    create_or_update_recovery_item,
    create_permission,
    get_assignment_candidates,
    get_auto_join_summary,
    get_draft_aktif,
    get_recovery_items,
    get_scrape_jobs,
    get_scrape_results,
    get_semua_grup,
    get_queue_target,
    get_campaign,
    get_recovery_item,
    get_assignment,
    set_scrape_job_status,
    update_assignment,
    update_campaign,
    update_queue_target,
)

_LOCK = threading.Lock()
_WORKER_THREAD: threading.Thread | None = None
_STATE: dict[str, Any] = {
    'worker_running': False,
    'current_stage': None,
    'last_started_at': None,
    'last_finished_at': None,
    'last_trigger': None,
    'run_count': 0,
    'failure_count': 0,
    'last_result': {},
    # Statistik kumulatif auto join (reset tiap hari)
    'auto_join_stats': {
        'joined_today': 0,
        'failed_today': 0,
        'waiting': 0,
        'last_run_at': None,
    },
}

FLOW = [
    {'stage': 'import', 'setting': 'auto_import_enabled', 'description': 'Import hasil scrape selesai ke tabel grup'},
    {'stage': 'permission', 'setting': 'auto_permission_enabled', 'description': 'Beri permission otomatis untuk grup baru/unknown'},
    {'stage': 'assignment', 'setting': 'auto_assign_enabled', 'description': 'Pilih owner akun terbaik dan sinkronkan managed status'},
    {'stage': 'campaign_prepare', 'setting': 'auto_campaign_enabled', 'description': 'Bentuk queue campaign dari grup managed yang eligible'},
    {'stage': 'delivery', 'setting': 'auto_campaign_enabled', 'description': 'Kirim pesan dari queue campaign menggunakan draft aktif'},
    {'stage': 'recovery_scan', 'setting': 'auto_recovery_enabled', 'description': 'Deteksi scrape, assignment, dan campaign yang macet'},
    {'stage': 'recovery_execute', 'setting': 'auto_recovery_enabled', 'description': 'Pulihkan item recoverable secara aman'},
]


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None




def _row_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return dict(value)
    except Exception:
        out: dict[str, Any] = {}
        try:
            keys = value.keys()
        except Exception:
            return out
        for key in keys:
            try:
                out[str(key)] = value[key]
            except Exception:
                pass
        return out


def _row_get(value: Any, key: str, default=None):
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(key, default)
    try:
        return value[key]
    except Exception:
        try:
            data = dict(value)
            return data.get(key, default)
        except Exception:
            return default

def _minutes_since(value: Any) -> float | None:
    dt = _parse_dt(value)
    if not dt:
        return None
    return (datetime.now() - dt).total_seconds() / 60.0


def _automation_allowed(setting_key: str, default: int = 0) -> bool:
    try:
        if bool(int(get_setting('maintenance_mode', 0) or 0)):
            return False
        if bool(int(get_setting('pause_all_automation', 0) or 0)):
            return False
        return bool(int(get_setting(setting_key, default) or 0))
    except Exception:
        return bool(default)


def _log(level: str, module: str, action: str, message: str, **kwargs):
    try:
        meta = ' '.join(f"{k}={v}" for k, v in kwargs.items() if v not in (None, '', [], {}))
        print(f"[AUTO][{module.upper()}][{level.upper()}] action={action} {message}{(' ' + meta) if meta else ''}")
    except Exception:
        pass
    try:
        add_audit_log(level, module, action, message, **kwargs)
    except Exception:
        pass


def _setting_int(key: str, default: int) -> int:
    try:
        return int(get_int(key, default))
    except Exception:
        return default


def _valid_permission_statuses() -> set[str]:
    return {'valid', 'owned', 'admin', 'partner_approved', 'opt_in'}


def _refresh_campaign_counts(campaign_id: int):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total_count,
          SUM(CASE WHEN status='eligible' THEN 1 ELSE 0 END) AS eligible_count,
          SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued_count,
          SUM(CASE WHEN status='sending' THEN 1 ELSE 0 END) AS sending_count,
          SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent_count,
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
          SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END) AS blocked_count,
          SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped_count
        FROM campaign_target WHERE campaign_id=%s
        """,
        (campaign_id,),
    ).fetchone()
    session_row = conn.execute('SELECT status, session_status, started_at, session_started_at FROM campaign WHERE id=%s', (campaign_id,)).fetchone()
    conn.close()
    if not row:
        return None
    total = int(row['total_count'] or 0)
    sent = int(row['sent_count'] or 0)
    failed = int(row['failed_count'] or 0)
    blocked = int(row['blocked_count'] or 0)
    skipped = int(row['skipped_count'] or 0)
    queued = int(row['queued_count'] or 0)
    eligible = int(row['eligible_count'] or 0)
    sending = int(row['sending_count'] or 0)
    status = None
    session_status = str((session_row['session_status'] if session_row else '') or '')
    terminal = sent + failed + blocked + skipped
    update_fields: dict[str, Any] = {
        'total_targets': total,
        'eligible_targets': eligible + queued + sending,
        'sent_count': sent,
        'failed_count': failed,
        'blocked_count': blocked,
    }
    if total > 0 and terminal >= total:
        if sent == total:
            status = 'completed'
        elif sent > 0:
            status = 'partial'
        else:
            status = 'failed'
        update_fields.update({
            'status': status,
            'finished_at': _now(),
            'session_status': 'finished',
            'session_finished_at': _now(),
        })
    elif sending > 0:
        status = 'running'
        update_fields.update({'status': 'running', 'session_status': 'running'})
        if session_row and not session_row['started_at']:
            update_fields['started_at'] = _now()
        if session_row and not session_row['session_started_at']:
            update_fields['session_started_at'] = _now()
    elif queued > 0 or eligible > 0:
        status = 'queued'
        update_fields.update({'status': 'queued', 'session_status': 'queued'})
    elif total == 0 and session_status in {'queued', 'running'}:
        update_fields.update({'status': 'draft', 'session_status': 'idle'})
    update_campaign(campaign_id, **update_fields)
    return {
        'total_targets': total,
        'eligible_targets': eligible + queued + sending,
        'sent_count': sent,
        'failed_count': failed,
        'blocked_count': blocked,
        'status': update_fields.get('status') or (session_row['status'] if session_row else 'draft'),
        'session_status': update_fields.get('session_status') or session_status or 'idle',
    }


def _candidate_distribution_key(candidate: dict[str, Any], *, prefer_joined: bool = False, joined_phones: set[str] | None = None) -> tuple:
    phone = str(candidate.get('account_id') or '')
    joined = phone in (joined_phones or set())
    active_assignment_count = int(candidate.get('active_assignment_count') or 0)
    ranking_score = int(candidate.get('ranking_score') or 0)
    effective_health = int(candidate.get('effective_health_score') or candidate.get('health_score') or 0)
    effective_warming = int(candidate.get('effective_warming_level') or candidate.get('warming_level') or 0)
    # Urutan: bila diminta, utamakan akun yang sudah join di grup; lalu pilih beban assignment paling kecil,
    # baru sisa skor/health/warming sebagai tie-breaker.
    return (
        0 if (prefer_joined and joined) else 1,
        active_assignment_count,
        -ranking_score,
        -effective_health,
        -effective_warming,
        phone,
    )


def _choose_candidate(group_id: int) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    candidates = get_assignment_candidates(group_id)
    if not candidates:
        return None, []
    conn = get_conn()
    joined_rows = conn.execute('SELECT phone FROM akun_grup WHERE grup_id=%s', (group_id,)).fetchall()
    conn.close()
    joined = {str(r['phone']) for r in joined_rows}
    ordered = sorted(candidates, key=lambda c: _candidate_distribution_key(c, prefer_joined=True, joined_phones=joined))
    return (ordered[0] if ordered else None), ordered



def _setting_bool(key: str, default: bool = False) -> bool:
    try:
        return bool(int(get_setting(key, 1 if default else 0) or 0))
    except Exception:
        return default


def _now_plus(*, seconds: int = 0, minutes: int = 0, hours: int = 0, days: int = 0) -> str:
    return (datetime.now() + timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days)).strftime('%Y-%m-%d %H:%M:%S')


def _rule_int(action: dict[str, Any], action_key: str, setting_key: str, default: int) -> int:
    try:
        if action_key in action and action.get(action_key) not in (None, ''):
            return int(action.get(action_key) or 0)
    except Exception:
        pass
    return _setting_int(setting_key, default)


def _rule_bool(action: dict[str, Any], action_key: str, setting_key: str, default: bool) -> bool:
    if action_key in action:
        value = action.get(action_key)
        if isinstance(value, bool):
            return value
        try:
            return bool(int(value or 0))
        except Exception:
            return str(value or '').strip().lower() in {'1', 'true', 'yes', 'y', 'on'}
    return _setting_bool(setting_key, default)


def _mark_group_hold(group_id: int, status: str, reason_code: str, *, ready_at: str | None = None):
    payload: dict[str, Any] = {'broadcast_status': status, 'broadcast_hold_reason': reason_code}
    if ready_at is not None:
        payload['broadcast_ready_at'] = ready_at
    _set_group_state(group_id, **payload)


def _promote_broadcast_ready_groups(limit: int = 500) -> dict[str, int]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT id, assignment_status, permission_status, COALESCE(broadcast_status,'hold') AS broadcast_status, broadcast_ready_at
        FROM grup
        WHERE status='active'
          AND COALESCE(broadcast_status,'hold') IN ('stabilization_wait','cooldown')
          AND broadcast_ready_at IS NOT NULL
          AND broadcast_ready_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        ORDER BY score DESC, id DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    conn.close()
    promoted = 0
    held = 0
    for row in rows:
        gid = int(row['id'])
        if row['assignment_status'] == 'managed' and str(row['permission_status'] or '') in _valid_permission_statuses():
            _set_group_state(gid, broadcast_status='broadcast_eligible', broadcast_hold_reason=None)
            promoted += 1
        elif row['assignment_status'] != 'managed':
            _mark_group_hold(gid, 'stabilization_wait', 'waiting_owner_join')
            held += 1
        else:
            _mark_group_hold(gid, 'hold', 'permission_not_valid')
            held += 1
    return {'promoted': promoted, 'held': held}


def _session_candidates() -> list[dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT c.*,
               (SELECT COUNT(*) FROM campaign_target ct WHERE ct.campaign_id=c.id) AS total_target_count,
               (SELECT COUNT(*) FROM campaign_target ct WHERE ct.campaign_id=c.id AND ct.status IN ('eligible','queued','sending')) AS active_target_count
        FROM campaign c
        WHERE c.status IN ('queued','running','paused')
        ORDER BY CASE c.status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END, c.id ASC
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _create_session_campaign(*, sender_pool: str, target_limit: int, note: str) -> dict[str, Any]:
    campaign_id = create_campaign(
        name=f"Auto Session {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        sender_pool=sender_pool,
        target_mode='orchestrated_session',
        auto_start_enabled=1,
        required_permission_status='valid',
        required_group_status='managed',
    )
    session_key = f"AUTO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{campaign_id}"
    update_campaign(campaign_id, status='queued', session_key=session_key, session_status='queued', session_target_limit=target_limit, session_note=note)
    return get_campaign(campaign_id) or {'id': campaign_id, 'session_key': session_key, 'status': 'queued', 'session_target_limit': target_limit}


def _resolve_session_campaign(*, sender_pool: str, target_limit: int, allow_mid_session_enqueue: bool, create_if_missing: bool, note: str) -> dict[str, Any] | None:
    sessions = _session_candidates()
    running = next((s for s in sessions if s.get('status') == 'running'), None)
    queued = next((s for s in sessions if s.get('status') == 'queued'), None)

    def _capacity_left(session: dict[str, Any]) -> int | None:
        limit_val = int(session.get('session_target_limit') or target_limit or 0)
        active_count = int(session.get('total_target_count') or 0)
        if limit_val <= 0:
            return None
        return max(0, limit_val - active_count)

    if running and allow_mid_session_enqueue:
        cap = _capacity_left(running)
        if cap is None or cap > 0:
            return running
    if queued:
        cap = _capacity_left(queued)
        if cap is None or cap > 0:
            return queued
    if running and not allow_mid_session_enqueue and create_if_missing:
        return _create_session_campaign(sender_pool=sender_pool, target_limit=target_limit, note=note)
    if running and allow_mid_session_enqueue and create_if_missing:
        return _create_session_campaign(sender_pool=sender_pool, target_limit=target_limit, note=note)
    if create_if_missing:
        return _create_session_campaign(sender_pool=sender_pool, target_limit=target_limit, note=note)
    return None


def _send_quota_snapshot(phone: str) -> dict[str, int]:
    from core.warming import get_daily_capacity

    cap = get_daily_capacity(phone).get('kirim', {})
    total = max(0, int(cap.get('limit') or 0))
    used = max(0, int(cap.get('used') or 0))
    remaining = max(0, int(cap.get('remaining') or max(0, total - used)))
    return {'limit': total, 'used': used, 'remaining': remaining}



def _sender_in_cooldown(cooldown_until: Any) -> bool:
    dt = _parse_dt(cooldown_until)
    return bool(dt and dt > datetime.now())



def _sender_available_for_delivery(phone: str, *, require_online_sender: bool = True) -> bool:
    if require_online_sender and phone not in _clients:
        return False
    conn = get_conn()
    row = conn.execute(
        """
        SELECT COALESCE(status,'active') AS status,
               COALESCE(auto_send_enabled,1) AS auto_send_enabled,
               COALESCE(cooldown_until,'') AS cooldown_until
        FROM akun
        WHERE phone=%s
        LIMIT 1
        """,
        (phone,),
    ).fetchone()
    conn.close()
    if not row:
        return False
    status = str(row['status'] or 'active').strip().lower()
    if status not in {'active', 'online'}:
        return False
    if int(row['auto_send_enabled'] or 1) != 1:
        return False
    if _sender_in_cooldown(row['cooldown_until']):
        return False
    quota = _send_quota_snapshot(phone)
    if quota['limit'] > 0 and quota['remaining'] <= 0:
        return False
    return True



def _available_online_senders() -> list[str]:
    if not _clients:
        return []
    phones = [str(phone) for phone in _clients.keys() if _sender_available_for_delivery(str(phone), require_online_sender=True)]
    return sorted(set(phones))



def _mark_sender_delivery_exhausted(phone: str):
    next_day = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
    conn = get_conn()
    conn.execute(
        """
        UPDATE akun
        SET cooldown_until=%s,
            last_error_code='daily_limit_exhausted',
            last_error_message='Batas harian kirim tercapai'
        WHERE phone=%s
        """,
        (next_day.strftime('%Y-%m-%d %H:%M:%S'), phone),
    )
    conn.commit()
    conn.close()



def _get_online_sender_candidates(group_id: int, preferred_sender: str | None = None) -> list[dict[str, Any]]:
    if not _clients:
        return []
    conn = get_conn()
    joined_rows = conn.execute('SELECT phone FROM akun_grup WHERE grup_id=%s', (group_id,)).fetchall()
    rows = conn.execute(
        """
        SELECT phone, COALESCE(nama, phone) AS nama, COALESCE(status,'active') AS status,
               COALESCE(priority_weight,100) AS priority_weight,
               COALESCE(health_score,100) AS health_score,
               COALESCE(level_warming,1) AS level_warming,
               COALESCE(auto_send_enabled,1) AS auto_send_enabled,
               COALESCE(cooldown_until,'') AS cooldown_until
        FROM akun
        WHERE COALESCE(status,'active') IN ('active','online')
          AND COALESCE(auto_send_enabled,1)=1
        ORDER BY COALESCE(priority_weight,100) DESC, COALESCE(health_score,100) DESC, COALESCE(level_warming,1) DESC
        LIMIT 50
        """
    ).fetchall()
    conn.close()
    joined = {str(r['phone']) for r in joined_rows}
    candidates: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        phone = str(item['phone'])
        if phone not in _clients:
            continue
        if _sender_in_cooldown(item.get('cooldown_until')):
            continue
        quota = _send_quota_snapshot(phone)
        item['send_quota'] = quota
        if quota['limit'] > 0 and quota['remaining'] <= 0:
            continue
        item['joined_group'] = phone in joined
        if phone not in joined:
            continue
        item['is_preferred'] = bool(preferred_sender and phone == preferred_sender)
        item['ranking_score'] = int(item.get('priority_weight') or 0) + int(item.get('health_score') or 0) + (int(item.get('level_warming') or 0) * 10) + (60 if item['joined_group'] else 0) + (40 if item['is_preferred'] else 0) + min(int(quota.get('remaining') or 0), 20)
        candidates.append(item)
    candidates.sort(key=lambda x: x['ranking_score'], reverse=True)
    return candidates



def _resolve_sender_for_group(group_id: int, preferred_sender: str | None = None, *, require_online_sender: bool = True) -> tuple[str | None, list[dict[str, Any]]]:
    if preferred_sender and _sender_available_for_delivery(str(preferred_sender), require_online_sender=require_online_sender):
        return str(preferred_sender), [{'phone': str(preferred_sender), 'ranking_score': 9999, 'joined_group': True, 'is_preferred': True, 'send_quota': _send_quota_snapshot(str(preferred_sender))}]
    candidates = _get_online_sender_candidates(group_id, preferred_sender=preferred_sender)
    if candidates:
        return str(candidates[0]['phone']), candidates
    if preferred_sender and not require_online_sender and _sender_available_for_delivery(str(preferred_sender), require_online_sender=False):
        return str(preferred_sender), []
    return None, candidates


def _max_delivery_attempts() -> int:
    if not _setting_bool('pipeline_retry_umum_enabled', True):
        return 1
    retries = _setting_int('pipeline_retry_maks_per_item', -1)
    if retries >= 0:
        return max(1, retries + 1)
    policy = str(get_setting('campaign_retry_policy', 'retry_once') or 'retry_once').strip().lower()
    mapping = {'no_retry': 1, 'retry_once': 2, 'retry_twice': 3, 'retry_three': 4}
    return mapping.get(policy, 2)


def _latest_assignment_for_group(group_id: int) -> dict[str, Any] | None:
    conn = get_conn()
    row = conn.execute('SELECT * FROM group_assignment WHERE group_id=%s ORDER BY id DESC LIMIT 1', (group_id,)).fetchone()
    conn.close()
    return dict(row) if row else None




def _join_quota_snapshot(phone: str) -> dict[str, int]:
    from core.warming import get_daily_capacity
    cap = get_daily_capacity(phone).get('join', {})
    total = max(0, int(cap.get('limit') or 0))
    used = max(0, int(cap.get('used') or 0))
    remaining = max(0, int(cap.get('remaining') or max(0, total - used)))
    return {'limit': total, 'used': used, 'remaining': remaining}


def _pick_reassign_owner_for_join(group_id: int, current_owner: str | None = None, *, reserve_quota: int = 0) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    candidates = get_assignment_candidates(group_id)
    if not candidates:
        return None, []
    reserve_quota = max(0, int(reserve_quota or 0))
    eligible: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for cand in candidates:
        phone = str(cand.get('account_id') or '')
        if not phone:
            continue
        info = {
            'account_id': phone,
            'active_assignment_count': int(cand.get('active_assignment_count') or 0),
            'ranking_score': int(cand.get('ranking_score') or 0),
        }
        if phone == str(current_owner or ''):
            info['reason'] = 'same_as_current_owner'
            rejected.append(info)
            continue
        if phone not in _clients:
            info['reason'] = 'not_online_client'
            rejected.append(info)
            continue
        quota = _join_quota_snapshot(phone)
        info['join_quota'] = quota
        if quota['limit'] <= 0:
            info['reason'] = 'join_limit_zero'
            rejected.append(info)
            continue
        if quota['remaining'] <= reserve_quota:
            info['reason'] = 'join_quota_exhausted'
            rejected.append(info)
            continue
        cand = dict(cand)
        cand['join_quota'] = quota
        eligible.append(cand)
    if not eligible:
        return None, rejected
    # Pilih akun dengan beban assignment paling ringan, lalu sisa kuota join terbanyak,
    # lalu ranking_score sebagai tie-breaker. Ini membuat distribusi lebih merata.
    eligible.sort(key=lambda c: (
        int(c.get('active_assignment_count') or 0),
        -int((c.get('join_quota') or {}).get('remaining') or 0),
        -int(c.get('ranking_score') or 0),
        str(c.get('account_id') or ''),
    ))
    return eligible[0], rejected


def _reassign_group_owner_for_join(group_id: int, group_name: str, current_owner: str | None, *, reason_code: str, reserve_quota: int = 0) -> tuple[bool, dict[str, Any] | None]:
    best, rejected = _pick_reassign_owner_for_join(group_id, current_owner=current_owner, reserve_quota=reserve_quota)
    if not best:
        _log('warning', 'join', 'owner_bottleneck', f"Grup {group_name} tetap menunggu karena owner {current_owner or '-'} tidak siap dan tidak ada kandidat pengganti", entity_type='group', entity_id=str(group_id), result='waiting', payload=json.dumps({'reason_code': reason_code, 'owner_before': current_owner, 'owner_after': current_owner, 'rejected_candidates': rejected[:10]}, ensure_ascii=False))
        return False, None
    latest = _latest_assignment_for_group(group_id)
    owner_after = str(best['account_id'])
    if latest and latest.get('id'):
        update_assignment(int(latest['id']), assigned_account_id=owner_after, status='assigned', reassign_count=int(latest.get('reassign_count') or 0) + 1, failure_reason=reason_code, last_attempt_at=_now(), assign_reason=f'auto_reassign:{reason_code}', assign_score_snapshot=json.dumps(best, ensure_ascii=False))
    else:
        create_assignment(group_id, owner_after, assignment_type='auto_reassign', status='assigned', priority_level=int(best.get('priority_weight') or 100), assign_reason=f'auto_reassign:{reason_code}', assign_score_snapshot=json.dumps(best, ensure_ascii=False))
    _set_group_state(group_id, owner_phone=owner_after, assignment_status='assigned', broadcast_hold_reason='waiting_owner_join')
    _log('warning', 'join', 'owner_reassigned', f"Owner grup {group_name} dipindahkan dari {current_owner or '-'} ke {owner_after} karena {reason_code}", entity_type='group', entity_id=str(group_id), result='reassigned', payload=json.dumps({'owner_before': current_owner, 'owner_after': owner_after, 'reason_code': reason_code, 'replacement_join_quota': best.get('join_quota'), 'current_owner_join_quota': _join_quota_snapshot(str(current_owner or '')), 'rejected_candidates': rejected[:10]}, ensure_ascii=False))
    return True, best

def _set_group_state(group_id: int, **fields):
    allowed = {
        'permission_status', 'permission_basis', 'approved_by', 'approved_at', 'permission_expires_at',
        'assignment_status', 'broadcast_status', 'owner_phone', 'status', 'notes', 'source_keyword',
        'broadcast_ready_at', 'broadcast_hold_reason', 'join_ready_at', 'join_hold_reason', 'join_status', 'join_attempt_count',
        'broadcast_attempt_count', 'broadcast_last_sender'
    }
    parts = []
    values = []
    for key, value in fields.items():
        if key in allowed:
            parts.append(f"{key}=%s")
            values.append(value)
    if not parts:
        return
    parts.append("diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    values.append(group_id)
    conn = get_conn()
    conn.execute(f"UPDATE grup SET {', '.join(parts)} WHERE id=%s", values)
    conn.commit()
    conn.close()


def stage_import(limit_jobs: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('import')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule import yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan['effective_action']
    scope = plan['effective_scope']
    limit_jobs = limit_jobs or int(action.get('limit_jobs') or _setting_int('orchestrator_import_batch', 10))
    mode = str(action.get('mode') or 'all_new')
    valid_statuses = set(scope.get('job_status_in') or ['done'])
    require_unimported = bool(scope.get('require_unimported_results', True))
    imported_jobs = 0
    imported_groups = 0
    skipped = 0
    processed = 0
    try:
        for job in get_scrape_jobs(limit=max(limit_jobs * 3, limit_jobs)):
            if str(job.get('status') or '') not in valid_statuses:
                continue
            processed += 1
            if require_unimported:
                results_left = get_scrape_results(int(job['id']), only_new=True, include_imported=False)
                if not results_left:
                    skipped += 1
                    continue
            result = import_scrape_results(int(job['id']), mode=mode)
            imported = int(result.get('imported') or 0)
            if imported > 0:
                imported_jobs += 1
                imported_groups += imported
            else:
                skipped += 1
            if imported_jobs >= limit_jobs:
                break
        result = {'processed_jobs': processed, 'imported_jobs': imported_jobs, 'imported_groups': imported_groups, 'skipped_jobs': skipped, 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
        record_stage_result('import', matched_rules, True, result)
        if imported_groups:
            _log('info', 'orchestrator', 'stage_import', f'Import otomatis memindahkan {imported_groups} grup dari {imported_jobs} job', entity_type='scrape_job', entity_id='bulk', result='success')
        return result
    except Exception:
        record_stage_result('import', matched_rules, False, {'processed_jobs': processed})
        raise


def stage_permission(limit: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('permission')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule permission yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan.get('effective_action') or {}
    scope = plan.get('effective_scope') or {}
    limit = limit or int(action.get('limit') or _setting_int('orchestrator_permission_batch', 100))

    statuses = tuple(scope.get('group_status_in') or [])
    if not statuses:
        status_text = str(get_setting('permission_group_status_in', '') or '').strip()
        statuses = tuple([x.strip() for x in status_text.split(',') if x.strip()]) or ('active',)

    permission_statuses = tuple(scope.get('permission_status_in') or [])
    if not permission_statuses:
        perm_text = str(get_setting('permission_status_in', '') or '').strip()
        permission_statuses = tuple([x.strip() for x in perm_text.split(',') if x.strip()]) or ('unknown',)

    exclude_channels = bool(scope.get('exclude_channels')) if 'exclude_channels' in scope else _setting_bool('permission_exclude_channels', True)
    require_username = _setting_bool('permission_require_username', 0)
    min_score = _setting_int('permission_min_score', 0)

    now = _now()
    created = 0
    processed = 0
    conn = get_conn()
    where_parts = [
        f"g.status IN ({','.join('%s' for _ in statuses)})",
        f"COALESCE(g.permission_status,'unknown') IN ({','.join('%s' for _ in permission_statuses)})",
    ]
    values = [*statuses, *permission_statuses]
    if exclude_channels:
        where_parts.append("COALESCE(g.tipe,'group') != 'channel'")
    if require_username:
        where_parts.append("COALESCE(g.username,'') != ''")
    if min_score > 0:
        where_parts.append("COALESCE(g.score,0) >= %s")
        values.append(min_score)
    rows = conn.execute(
        f"""
        SELECT g.id
        FROM grup g
        WHERE {' AND '.join(where_parts)}
        ORDER BY g.score DESC, g.id DESC
        LIMIT %s
        """,
        (*values, limit),
    ).fetchall()
    conn.close()
    try:
        for row in rows:
            processed += 1
            gid = int(row['id'])
            create_permission(
                gid,
                str(action.get('permission_basis') or 'opt_in'),
                str(action.get('approval_source') or 'auto_orchestrator'),
                str(action.get('approved_by') or 'system'),
                now,
                expires_at=action.get('expires_at'),
                notes=str(action.get('notes') or 'Diset otomatis oleh orchestrator'),
                status=str(action.get('status') or 'valid'),
            )
            _set_group_state(
                gid,
                permission_status=str(action.get('status') or 'valid'),
                permission_basis=str(action.get('permission_basis') or 'opt_in'),
                approved_by=str(action.get('approved_by') or 'system'),
                approved_at=now,
            )
            created += 1
        result = {
            'processed': processed,
            'created': created,
            'rule_ids': [r['id'] for r in matched_rules],
            'context': plan.get('context') or {},
            'filters': {
                'min_score': min_score,
                'require_username': require_username,
                'exclude_channels': exclude_channels,
            },
        }
        record_stage_result('permission', matched_rules, True, result)
        if created:
            _log('info', 'orchestrator', 'stage_permission', f'Permission otomatis dibuat untuk {created} grup', entity_type='group_permission', entity_id='bulk', result='success')
        return result
    except Exception:
        record_stage_result('permission', matched_rules, False, {'processed': processed, 'created': created})
        raise

def _heal_abandoned_groups(limit: int = 50) -> dict:
    """
    Self-healing untuk grup terbengkalai karena owner tidak aktif.
    """
    result = {
        'ready_assign_cleaned': 0,
        'assigned_reset': 0,
    }
    STATUS_TIDAK_AKTIF = ('banned', 'restricted', 'suspended', 'session_expired')

    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT g.id, g.nama, g.owner_phone
            FROM grup g
            WHERE g.assignment_status = 'ready_assign'
              AND g.owner_phone IS NOT NULL
              AND g.status = 'active'
              AND g.owner_phone IN (
                  SELECT phone FROM akun
                  WHERE COALESCE(status,'active') IN ('banned','restricted',
                        'suspended','session_expired')
              )
            LIMIT %s
            """,
            (limit,)
        ).fetchall()
        conn.close()

        for row in rows:
            try:
                conn2 = get_conn()
                conn2.execute(
                    """UPDATE grup SET owner_phone = NULL
                       WHERE id = %s AND assignment_status = 'ready_assign'""",
                    (int(row['id']),)
                )
                conn2.commit()
                conn2.close()
                result['ready_assign_cleaned'] += 1
                print(f"[SelfHeal] Grup '{row['nama']}' — owner lama {row['owner_phone']} dibersihkan")
            except Exception as e:
                print(f"[SelfHeal] Gagal bersihkan grup {row['id']}: {e}")
    except Exception as e:
        print(f"[SelfHeal] Gagal cek kondisi 1: {e}")

    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT g.id, g.nama, g.owner_phone, g.diupdate
            FROM grup g
            WHERE g.assignment_status = 'assigned'
              AND g.owner_phone IS NOT NULL
              AND g.status = 'active'
              AND g.owner_phone IN (
                  SELECT phone FROM akun
                  WHERE COALESCE(status,'active') IN ('banned','restricted',
                        'suspended','session_expired')
              )
              AND (
                  g.diupdate IS NULL
                  OR g.diupdate <= TO_CHAR(
                      NOW() - INTERVAL '60 minutes',
                      'YYYY-MM-DD HH24:MI:SS'
                  )
              )
            LIMIT %s
            """,
            (limit,)
        ).fetchall()
        conn.close()

        for row in rows:
            try:
                _set_group_state(
                    int(row['id']),
                    owner_phone=None,
                    assignment_status='ready_assign',
                    broadcast_status='hold',
                    broadcast_hold_reason='owner_tidak_aktif_reset',
                )
                latest = _latest_assignment_for_group(int(row['id']))
                if latest and latest.get('id'):
                    update_assignment(
                        int(latest['id']),
                        status='failed',
                        failure_reason='owner_tidak_aktif',
                        last_attempt_at=_now(),
                    )
                result['assigned_reset'] += 1
                print(f"[SelfHeal] Grup '{row['nama']}' — assigned ke {row['owner_phone']} (tidak aktif), reset ke ready_assign")
            except Exception as e:
                print(f"[SelfHeal] Gagal reset grup {row['id']}: {e}")
    except Exception as e:
        print(f"[SelfHeal] Gagal cek kondisi 2: {e}")

    total = result['ready_assign_cleaned'] + result['assigned_reset']
    if total > 0:
        print(f"[SelfHeal] Selesai: {result['ready_assign_cleaned']} dibersihkan, {result['assigned_reset']} di-reset")

    return result


def _cleanup_banned_accounts(max_reassign: int = 20) -> dict:
    result = {
        'sender_reset': 0,
        'owner_reassigned': 0,
        'owner_no_candidate': 0,
        'managed_reset': 0,       # BARU: grup managed yang owner-nya banned
        'managed_no_candidate': 0, # BARU: grup managed tidak ada akun pengganti
    }

    # ── LANGKAH 1: Reset antrian broadcast yang masih pakai akun banned atau restricted ──
    # Akun restricted juga tidak boleh kirim — auto_send_enabled sudah 0,
    # tapi target yang sudah terlanjur di-assign perlu dibersihkan juga.
    try:
        conn = get_conn()
        r = conn.execute("""UPDATE campaign_target SET sender_account_id = NULL WHERE status IN ('queued','sending','eligible') AND sender_account_id IS NOT NULL AND sender_account_id IN (SELECT phone FROM akun WHERE status IN ('banned','restricted'))""")
        result['sender_reset'] = r.rowcount
        conn.commit()
        conn.close()
        if result['sender_reset'] > 0:
            print(f"[BannedCleanup] {result['sender_reset']} target broadcast di-reset dari akun banned/restricted")
    except Exception as e:
        print(f"[BannedCleanup] Gagal reset sender: {e}")

    # ── LANGKAH 2: Grup BELUM join (assigned) dengan owner banned → cari owner baru ──
    # Ibarat: grup yang belum sempat dikunjungi karyawan → tugaskan karyawan lain
    try:
        conn = get_conn()
        banned_rows = conn.execute(
            """SELECT g.id, g.nama, g.owner_phone FROM grup g
               WHERE g.assignment_status = 'assigned'
                 AND g.owner_phone IS NOT NULL
                 AND g.status = 'active'
                 AND g.owner_phone IN (SELECT phone FROM akun WHERE status='banned')
                 AND NOT EXISTS (
                     SELECT 1 FROM akun_grup ag
                     WHERE ag.phone = g.owner_phone AND ag.grup_id = g.id
                 )
               LIMIT %s""",
            (max_reassign,)
        ).fetchall()
        conn.close()
        for row in banned_rows:
            group_id = int(row['id'])
            group_name = row['nama'] or str(group_id)
            banned_owner = row['owner_phone']
            try:
                reassigned, best = _reassign_group_owner_for_join(group_id, group_name, banned_owner, reason_code='owner_banned')
                if reassigned and best:
                    result['owner_reassigned'] += 1
                    print(f"[BannedCleanup] Grup '{group_name}' (belum join) → owner pindah dari {banned_owner} ke {best['account_id']}")
                else:
                    result['owner_no_candidate'] += 1
                    print(f"[BannedCleanup] Grup '{group_name}' (belum join) → tidak ada kandidat pengganti, tetap menunggu")
            except Exception as e:
                print(f"[BannedCleanup] Gagal reassign grup {group_id}: {e}")
        if result['owner_reassigned'] > 0 or result['owner_no_candidate'] > 0:
            print(f"[BannedCleanup] Grup belum join — reassign: {result['owner_reassigned']} berhasil, {result['owner_no_candidate']} tidak ada kandidat")
    except Exception as e:
        print(f"[BannedCleanup] Gagal proses owner (belum join): {e}")

    # ── LANGKAH 3 (BARU): Grup SUDAH join (managed) dengan owner banned ──
    # Ini adalah bug yang sebelumnya tidak ditangani sama sekali.
    # Ibarat: grup yang sudah dikunjungi karyawan, tapi karyawannya dipecat →
    # kembalikan ke antrian agar bisa ditugaskan ke karyawan lain yang sudah masuk grup itu.
    try:
        conn = get_conn()
        managed_banned = conn.execute(
            """SELECT g.id, g.nama, g.owner_phone FROM grup g
               WHERE g.assignment_status = 'managed'
                 AND g.owner_phone IS NOT NULL
                 AND g.status = 'active'
                 AND g.owner_phone IN (SELECT phone FROM akun WHERE status IN ('banned','restricted'))
               LIMIT %s""",
            (max_reassign,)
        ).fetchall()
        conn.close()

        for row in managed_banned:
            group_id   = int(row['id'])
            group_name = row['nama'] or str(group_id)
            banned_owner = row['owner_phone']
            try:
                # Cari akun lain yang sudah join di grup ini (ada di akun_grup)
                conn2 = get_conn()
                candidate_row = conn2.execute(
                    """SELECT ag.phone FROM akun_grup ag
                       JOIN akun a ON a.phone = ag.phone
                       WHERE ag.grup_id = %s
                         AND ag.phone != %s
                         AND COALESCE(a.status,'active') NOT IN ('banned','restricted','session_expired')
                       ORDER BY COALESCE(a.health_score,100) DESC, COALESCE(a.level_warming,1) DESC
                       LIMIT 1""",
                    (group_id, banned_owner)
                ).fetchone()
                conn2.close()

                if candidate_row:
                    # Ada akun lain yang sudah join di grup ini → langsung jadikan owner baru
                    new_owner = candidate_row['phone']
                    latest = _latest_assignment_for_group(group_id)
                    if latest and latest.get('id'):
                        update_assignment(
                            int(latest['id']),
                            assigned_account_id=new_owner,
                            status='managed',
                            reassign_count=int(latest.get('reassign_count') or 0) + 1,
                            failure_reason='owner_banned_managed',
                            last_attempt_at=_now(),
                            assign_reason='auto_reassign:owner_banned_managed',
                        )
                    _set_group_state(group_id,
                        owner_phone=new_owner,
                        assignment_status='managed',
                        broadcast_status='broadcast_eligible',
                        broadcast_hold_reason=None,
                    )
                    _log('warning', 'join', 'owner_reassigned_managed',
                         f"Grup managed '{group_name}' — owner banned {banned_owner} diganti {new_owner} (akun lain sudah join)",
                         entity_type='group', entity_id=str(group_id), result='reassigned')
                    print(f"[BannedCleanup] Grup managed '{group_name}' → owner baru: {new_owner} (sudah join di grup ini)")
                    result['managed_reset'] += 1

                else:
                    # Tidak ada akun lain yang join di grup ini →
                    # kembalikan ke ready_assign agar pipeline assign ulang dari awal
                    _set_group_state(group_id,
                        owner_phone=None,
                        assignment_status='ready_assign',
                        broadcast_status='hold',
                        broadcast_hold_reason='owner_banned_reassign_needed',
                    )
                    latest = _latest_assignment_for_group(group_id)
                    if latest and latest.get('id'):
                        update_assignment(
                            int(latest['id']),
                            status='failed',
                            failure_reason='owner_banned_managed_no_candidate',
                            last_attempt_at=_now(),
                        )
                    create_or_update_recovery_item(
                        'assignment', str(group_id),
                        entity_name=group_name,
                        current_status='ready_assign',
                        worker_status='degraded',
                        problem_type='owner_banned_managed',
                        severity='high',
                        recovery_status='recovery_needed',
                        last_activity_at=_now(),
                        note=f'Owner {banned_owner} banned, tidak ada akun lain yang join di grup ini — perlu assign ulang',
                    )
                    _log('warning', 'join', 'owner_banned_managed_reset',
                         f"Grup managed '{group_name}' — owner {banned_owner} banned, dikembalikan ke ready_assign",
                         entity_type='group', entity_id=str(group_id), result='reset_to_ready_assign')
                    print(f"[BannedCleanup] Grup managed '{group_name}' → tidak ada akun lain yang join, dikembalikan ke ready_assign")
                    result['managed_no_candidate'] += 1

            except Exception as e:
                print(f"[BannedCleanup] Gagal proses grup managed {group_id}: {e}")

        if result['managed_reset'] > 0 or result['managed_no_candidate'] > 0:
            print(f"[BannedCleanup] Grup managed — owner baru: {result['managed_reset']}, perlu assign ulang: {result['managed_no_candidate']}")
    except Exception as e:
        print(f"[BannedCleanup] Gagal proses grup managed: {e}")

    return result


def stage_sync_join(limit: int = 500) -> dict[str, Any]:
    """
    Sinkronisasi join otomatis (Opsi B).

    Cek semua grup yang statusnya 'assigned' (akun sudah dipilih tapi belum
    terkonfirmasi join). Kalau akun yang di-assign memang sudah join grup itu
    (ada di tabel akun_grup), langsung upgrade ke 'managed' agar bisa broadcast.

    Juga: sinkronkan ulang tabel akun_grup dari Telegram untuk akun yang online,
    agar data join selalu up-to-date tanpa perlu logout-login ulang.
    """
    from services.account_manager import _clients, _loop, run_sync
    from services.group_manager import fetch_grup_dari_akun
    from utils.storage_db import sinkronkan_relasi_akun_grup, simpan_banyak_grup

    synced_accounts = 0
    promoted = 0
    still_waiting = 0
    now = _now()

    # LANGKAH 1: Refresh tabel akun_grup untuk semua akun yang online
    # Ini yang membuat data join selalu sinkron dengan kondisi Telegram sebenarnya
    for phone, client in list(_clients.items()):
        try:
            _conn_sc = get_conn()
            _row_sc = _conn_sc.execute("SELECT status FROM akun WHERE phone=%s", (phone,)).fetchone()
            _conn_sc.close()
            if _row_sc:
                _st = str(_row_sc['status'] or '').lower()
                if _st in ('banned', 'restricted', 'suspended'):
                    continue
        except Exception:
            pass
        try:
            semua = run_sync(fetch_grup_dari_akun(phone), timeout=60)
            if semua:
                simpan_banyak_grup(semua, sumber='sync_join')
                sinkronkan_relasi_akun_grup(phone, [g['id'] for g in semua])
                synced_accounts += 1
                print(f"[SyncJoin] {phone}: {len(semua)} grup disinkronkan")
        except Exception as e:
            err_str = str(e).lower()
            _AUTH_ERRORS = ('authkeyunregistered','userdeactivated','phonenumberbanned','sessionrevoked','auth_key','user deactivated','auth key','account banned','your account has been')
            if any(x in err_str for x in _AUTH_ERRORS):
                if tandai_akun_banned(phone):
                    print(f"[SyncJoin] ⛔ {phone} DIBEKUKAN TELEGRAM — otomatis ditandai banned")
                _clients.pop(phone, None)
            else:
                print(f"[SyncJoin] {phone} gagal sync: {e}")

    # LANGKAH 2: Cek grup yang masih 'assigned' — apakah akun sudah join sekarang%s
    conn = get_conn()
    assigned_rows = conn.execute(
        """
        SELECT id, nama, owner_phone, broadcast_ready_at,
               COALESCE(broadcast_status,'hold') AS broadcast_status
        FROM grup
        WHERE assignment_status = 'assigned'
          AND owner_phone IS NOT NULL
          AND status = 'active'
        LIMIT %s
        """,
        (limit,)
    ).fetchall()
    conn.close()

    assignment_delay = _setting_int('assignment_broadcast_delay_minutes', 120)
    ready_at = _now_plus(minutes=assignment_delay)

    for row in assigned_rows:
        group_id = int(row['id'])
        owner_phone = row['owner_phone']
        current_broadcast = str(row['broadcast_status'] or 'hold')

        # Cek apakah akun sudah join grup ini di tabel akun_grup
        conn = get_conn()
        rel = conn.execute(
            'SELECT 1 FROM akun_grup WHERE phone=%s AND grup_id=%s',
            (owner_phone, group_id)
        ).fetchone()
        conn.close()

        if rel:
            # Akun sudah join → upgrade ke managed
            latest = _latest_assignment_for_group(group_id)
            if latest:
                update_assignment(int(latest['id']), status='managed', last_attempt_at=now)

            fields = {'assignment_status': 'managed', 'owner_phone': owner_phone, 'join_ready_at': None, 'join_hold_reason': None, 'join_status': 'joined'}
            if current_broadcast not in {'queued', 'sending', 'cooldown', 'blocked',
                                          'hold_inactive', 'hold_waiting_response'}:
                fields.update({
                    'broadcast_status': 'stabilization_wait',
                    'broadcast_hold_reason': 'new_assignment_wait',
                    'broadcast_ready_at': ready_at,
                })
            _set_group_state(group_id, **fields)

            # Hapus recovery item kalau ada
            create_or_update_recovery_item(
                'assignment', str(group_id),
                recovery_status='recovered',
                last_recovery_at=now,
                last_recovery_result='join_confirmed_sync'
            )
            promoted += 1
            print(f"[SyncJoin] Grup #{group_id} '{row['nama']}' → managed (akun {owner_phone} sudah join)")
        else:
            still_waiting += 1

    result = {
        'synced_accounts': synced_accounts,
        'promoted_to_managed': promoted,
        'still_waiting': still_waiting,
    }
    if promoted > 0:
        _log('info', 'orchestrator', 'stage_sync_join',
             f'Sync join: {synced_accounts} akun disinkron, {promoted} grup naik ke managed',
             entity_type='assignment', entity_id='bulk', result='success')
    return result


def stage_auto_join(limit: int = 30) -> dict[str, Any]:
    """
    Auto Join Grup — desain per-akun, non-blocking.

    Loop berdasarkan AKUN aktif. Setiap akun maksimal join 1 grup per siklus.
    Jeda antar-join murni dikontrol lewat next_join_at di DB.
    """
    from services.account_manager import _clients, _loop, run_sync
    from utils.storage_db import catat_riwayat, simpan_relasi_akun_grup

    if not _setting_bool('auto_join_enabled', False):
        return {'skipped': True, 'reason': 'auto_join_enabled=0'}

    if not _clients:
        return {'skipped': True, 'reason': 'Tidak ada akun online'}

    sisakan_kuota = _setting_int('auto_join_reserve_quota', 2)
    hanya_public  = _setting_bool('auto_join_public_only', True)

    joined_total  = 0
    skipped_total = 0
    failed_total  = 0

    # Ambil semua akun aktif yang sedang online (di _clients)
    phones_online = list(_clients.keys())
    conn_ak = get_conn()
    akun_aktif: list[str] = []
    for ph in phones_online:
        row_ak = conn_ak.execute(
            "SELECT status, auto_send_enabled FROM akun WHERE phone=%s", (ph,)
        ).fetchone()
        if not row_ak:
            continue
        st = str(row_ak['status'] or '').lower()
        aus = int(row_ak['auto_send_enabled'] or 1)
        if st in ('banned', 'restricted', 'suspended'):
            continue
        if st not in ('active', 'online'):
            continue
        if aus == 0:
            continue
        akun_aktif.append(ph)
    conn_ak.close()

    if not akun_aktif:
        return {'joined': 0, 'skipped': 0, 'failed': 0, 'reason': 'Tidak ada akun aktif'}

    # Loop per akun — tiap akun independen
    for phone in akun_aktif:
        owner_phone = phone
        # 1) Throttle per akun lewat next_join_at
        boleh_join, _alasan = _join_boleh_sekarang(owner_phone)
        if not boleh_join:
            skipped_total += 1
            continue

        # 2) Kuota harian
        quota_owner = _join_quota_snapshot(owner_phone)
        batas_harian = int(quota_owner.get('limit') or 0)
        sudah_hari   = int(quota_owner.get('used') or 0)
        sisa_hari    = int(quota_owner.get('remaining') or 0)
        if batas_harian > 0 and (sudah_hari + sisakan_kuota >= batas_harian or sisa_hari <= sisakan_kuota):
            skipped_total += 1
            continue

        client = _clients.get(owner_phone)
        if not client:
            skipped_total += 1
            continue

        # 3) Cari 1 grup untuk akun ini
        where = [
            "g.assignment_status = 'assigned'",
            "g.owner_phone = %s",
            "g.status = 'active'",
            "COALESCE(g.broadcast_status,'hold') != 'blocked'",
            "COALESCE(g.join_hold_reason,'') != 'invalid_target_final'",
            "(g.join_ready_at IS NULL OR g.join_ready_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))",
        ]
        if hanya_public:
            where.append("g.username IS NOT NULL AND g.username != ''")

        conn = get_conn()
        row = conn.execute(
            f"""
            SELECT g.id, g.nama, g.username, g.owner_phone, g.join_ready_at, g.join_hold_reason,
                   COALESCE(g.broadcast_status,'hold') AS broadcast_status,
                   COALESCE(g.join_attempt_count, 0) AS join_attempt_count
            FROM grup g
            WHERE {' AND '.join(where)}
              AND NOT EXISTS (
                SELECT 1 FROM akun_grup ag
                WHERE ag.phone = g.owner_phone AND ag.grup_id = g.id
              )
            ORDER BY g.score DESC, g.id DESC
            LIMIT 1
            """,
            (owner_phone,)
        ).fetchone()
        conn.close()

        if not row:
            skipped_total += 1
            continue

        group_id   = int(row['id'])
        group_name = row['nama'] or str(group_id)
        username   = row['username']

        # 4) Join grup (non-blocking; jeda lewat next_join_at)
        try:
            jeda_detik = _hitung_jeda_join(owner_phone)

            async def _do_join(c, uname):
                from telethon.tl.functions.channels import JoinChannelRequest
                entity = await c.get_entity(uname)
                await c(JoinChannelRequest(entity))
                return entity.id

            run_sync(_do_join(client, username), timeout=30)
            print(f"[AutoJoin] OK {owner_phone} join '{group_name}' (@{username})")
            _log('info', 'join', 'join_success', f"{owner_phone} berhasil join {group_name}", entity_type='group', entity_id=str(group_id), result='success', payload=json.dumps({'phone': owner_phone, 'username': username}, ensure_ascii=False))

            catat_riwayat(owner_phone, group_id, group_name, 'join_success')
            simpan_relasi_akun_grup(owner_phone, [group_id])
            _set_group_state(group_id, join_ready_at=None, join_hold_reason=None, join_status='joined', join_attempt_count=0, assignment_status='managed')

            joined_total += 1
            _set_join_throttle(owner_phone, jeda_detik)

        except Exception as e:
            err = str(e)
            err_lower = err.lower()
            print(f"[AutoJoin] ❌ {owner_phone} gagal join '{group_name}': {err}")

            # Deteksi akun dibekukan Telegram
            _JOIN_BANNED_SIGNALS = ('authkeyunregistered', 'userdeactivated', 'phonenumberbanned', 'sessionrevoked', 'auth key', 'user deactivated', 'account banned', 'your account has been')
            if any(x in err_lower for x in _JOIN_BANNED_SIGNALS):
                if tandai_akun_banned(owner_phone):
                    print(f"[AutoJoin] ⛔ {owner_phone} DIBEKUKAN TELEGRAM — otomatis ditandai banned")
                _clients.pop(owner_phone, None)
                failed_total += 1
                continue

            # Request join terkirim: bukan gagal final, tetap menunggu approval
            if 'successfully requested to join' in err_lower:
                _log('info', 'join', 'join_requested', f"{owner_phone} mengirim request join {group_name}", entity_type='group', entity_id=str(group_id), result='requested', payload=json.dumps({'owner_used_for_join': owner_phone, 'owner_before': row['owner_phone'], 'owner_after': owner_phone}, ensure_ascii=False))
                catat_riwayat(owner_phone, group_id, group_name, 'join_requested', err[:150])
                # Pending approval jangan dicoba berulang tiap siklus. Tahan dulu beberapa jam.
                _set_group_state(group_id, join_hold_reason='approval_pending', join_ready_at=_now_plus(hours=6), join_status='requested')
                skipped_total += 1
                continue

            # FloodWait join: tandai cooldown akun dan status khusus
            if 'floodwait' in err_lower or 'wait of' in err_lower or 'flood' in err_lower:
                _log('warning', 'join', 'join_floodwait', f"{owner_phone} floodwait saat join {group_name}: {err[:120]}", entity_type='group', entity_id=str(group_id), result='cooldown', payload=json.dumps({'owner_used_for_join': owner_phone, 'owner_before': row['owner_phone'], 'owner_after': owner_phone}, ensure_ascii=False))
                import re
                detik = 60
                m = re.search(r'(\d+)', err)
                if m:
                    detik = int(m.group(1))
                hold_until = _now_plus(seconds=max(60, detik + 15))
                try:
                    from utils.database import get_conn as _gc
                    c2 = _gc()
                    c2.execute(
                        "UPDATE akun SET cooldown_until=NOW() + (%s || ' seconds')::INTERVAL WHERE phone=%s",
                        (str(detik), owner_phone)
                    )
                    c2.commit()
                    c2.close()
                    print(f"[AutoJoin] {owner_phone} kena FloodWait {detik}s, cooldown dicatat")
                except Exception:
                    pass
                _set_group_state(group_id, join_hold_reason='join_floodwait', join_ready_at=hold_until, join_status='hold')
                catat_riwayat(owner_phone, group_id, group_name, 'join_floodwait', err[:150])
                _set_join_throttle(owner_phone, detik + 30)
                failed_total += 1
                continue

            # Kalau sudah member → update relasi saja
            if 'already' in err_lower or 'useralreadyparticipant' in err_lower:
                simpan_relasi_akun_grup(owner_phone, [group_id])
                _set_group_state(group_id, join_ready_at=None, join_hold_reason=None, join_status='joined', join_attempt_count=0, assignment_status='managed')
                joined_total += 1
                print(f"[AutoJoin] {owner_phone} sudah member '{group_name}', relasi diperbarui")
                catat_riwayat(owner_phone, group_id, group_name, 'join_success', 'already participant')
                continue

            # Username/link invalid — termasuk 'No user has X as username' dari Telethon
            if ('nobody is using this username' in err_lower
                    or 'username is unacceptable' in err_lower
                    or 'resolveusernamerequest' in err_lower
                    or 'no user has' in err_lower
                    or 'username not found' in err_lower
                    or 'username invalid' in err_lower):
                _log('warning', 'join', 'join_invalid_target', f"{owner_phone} target join tidak valid {group_name}: {err[:120]}", entity_type='group', entity_id=str(group_id), result='invalid_target', payload=json.dumps({'owner_used_for_join': owner_phone, 'owner_before': row['owner_phone'], 'owner_after': owner_phone}, ensure_ascii=False))
                _set_group_state(group_id, broadcast_status='blocked', broadcast_hold_reason='join_invalid_target', join_hold_reason='invalid_target_final', join_ready_at=None, join_status='invalid_final')
                catat_riwayat(owner_phone, group_id, group_name, 'join_invalid_target', err[:150])
                failed_total += 1
                continue

            # Kalau grup tidak bisa diakses (private/banned) → langsung blacklist tanpa retry
            if any(x in err for x in ['ChannelPrivate', 'InviteHashExpired', 'banned', 'Banned']):
                _set_group_state(group_id,
                    broadcast_status='blocked',
                    broadcast_hold_reason='join_failed_private_or_banned',
                    join_hold_reason='blacklisted',
                    join_attempt_count=0,
                )
                _log('warning', 'join', 'join_blacklisted', f"{owner_phone} grup private/banned langsung diblacklist {group_name}: {err[:120]}", entity_type='group', entity_id=str(group_id), result='blacklisted', payload=json.dumps({'owner_used_for_join': owner_phone}, ensure_ascii=False))
                catat_riwayat(owner_phone, group_id, group_name, 'join_blacklisted', err[:150])
                print(f"[AutoJoin] Grup '{group_name}' diblacklist — private/banned")
                failed_total += 1
                continue

            # Gagal umum — cek sudah berapa kali dicoba
            attempt_count = int(row.get('join_attempt_count') or 0) + 1
            if attempt_count >= 2:
                # Sudah 2x gagal → blacklist permanen
                _set_group_state(group_id,
                    broadcast_status='blocked',
                    broadcast_hold_reason='join_failed_max_retry',
                    join_hold_reason='blacklisted',
                    join_ready_at=None,
                    join_attempt_count=0,
                )
                _log('warning', 'join', 'join_blacklisted', f"{owner_phone} grup diblacklist setelah {attempt_count}x gagal join {group_name}: {err[:120]}", entity_type='group', entity_id=str(group_id), result='blacklisted', payload=json.dumps({'owner_used_for_join': owner_phone, 'attempt_count': attempt_count}, ensure_ascii=False))
                catat_riwayat(owner_phone, group_id, group_name, 'join_blacklisted', f'Gagal {attempt_count}x: {err[:120]}')
                print(f"[AutoJoin] Grup '{group_name}' diblacklist setelah {attempt_count}x gagal")
            else:
                # Percobaan ke-1 → tunda ke siklus berikutnya (5 menit)
                _set_group_state(group_id,
                    join_hold_reason='join_retry_wait',
                    join_ready_at=_now_plus(minutes=5),
                    join_attempt_count=attempt_count,
                )
                _log('warning', 'join', 'join_failed', f"{owner_phone} gagal join {group_name} (percobaan {attempt_count}/2): {err[:120]}", entity_type='group', entity_id=str(group_id), result='failed', payload=json.dumps({'owner_used_for_join': owner_phone, 'attempt_count': attempt_count}, ensure_ascii=False))
                catat_riwayat(owner_phone, group_id, group_name, 'join_failed', f'Percobaan {attempt_count}/2: {err[:150]}')
                print(f"[AutoJoin] Grup '{group_name}' gagal join percobaan {attempt_count}/2 — ulangi 5 menit lagi")

            failed_total += 1

    result = {'joined': joined_total, 'skipped': skipped_total, 'failed': failed_total}
    _log('info', 'join', 'auto_join_stage', f"Auto join: {joined_total} berhasil, {skipped_total} skip, {failed_total} gagal", entity_type='join', entity_id='bulk', result='success')

    # Simpan ke _STATE agar bisa ditampilkan di card frontend
    from datetime import date as _date_cls
    stats = _STATE.get('auto_join_stats', {})
    # Reset kalau sudah berganti hari
    last_run = stats.get('last_run_at', '')
    if last_run and not last_run.startswith(str(_date_cls.today())):
        stats['joined_today'] = 0
        stats['failed_today'] = 0
    stats['joined_today'] = (stats.get('joined_today') or 0) + joined_total
    stats['failed_today'] = (stats.get('failed_today') or 0) + failed_total
    stats['last_run_at']  = _now()
    # Hitung grup yang masih menunggu (assigned, belum join)
    try:
        conn2 = get_conn()
        waiting = conn2.execute(
            """SELECT COUNT(*) as n FROM grup
               WHERE assignment_status='assigned' AND owner_phone IS NOT NULL AND status='active'
               AND NOT EXISTS (SELECT 1 FROM akun_grup ag WHERE ag.phone=grup.owner_phone AND ag.grup_id=grup.id)"""
        ).fetchone()
        conn2.close()
        stats['waiting'] = int(waiting['n'] or 0)
    except Exception:
        pass
    _STATE['auto_join_stats'] = stats

    if joined_total > 0:
        _log('info', 'orchestrator', 'stage_auto_join',
             f'Auto join: {joined_total} berhasil, {skipped_total} skip, {failed_total} gagal',
             entity_type='grup', entity_id='bulk', result='success')
    return result


def stage_assignment(limit: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('assignment')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule assignment yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan['effective_action']
    scope = plan['effective_scope']
    limit = limit or int(action.get('limit') or _setting_int('orchestrator_assign_batch', 100))
    valid_permissions = tuple(scope.get('permission_status_in') or sorted(_valid_permission_statuses()))
    assignment_statuses = tuple(scope.get('assignment_status_in') or ['ready_assign', 'retry_wait', 'reassign_pending', 'failed', 'assigned'])
    group_statuses = tuple(scope.get('group_status_in') or ['active'])
    exclude_channels = bool(scope.get('exclude_channels', True))
    prefer_joined_owner = bool(action.get('prefer_joined_owner', True))
    create_recovery_on_no_candidate = bool(action.get('create_recovery_on_no_candidate', True))
    assignment_delay_minutes = _rule_int(action, 'assignment_delay_minutes', 'assignment_broadcast_delay_minutes', 2)
    conn = get_conn()
    where_parts = [
        f"status IN ({','.join('%s' for _ in group_statuses)})",
        f"COALESCE(permission_status,'unknown') IN ({','.join('%s' for _ in valid_permissions)})",
    ]
    values = [*group_statuses, *valid_permissions]
    if exclude_channels:
        where_parts.append("COALESCE(tipe,'group') != 'channel'")
    rows = conn.execute(
        f"""
        SELECT id, nama, owner_phone, assignment_status, permission_status, COALESCE(broadcast_status,'hold') AS broadcast_status, broadcast_ready_at
        FROM grup
        WHERE {' AND '.join(where_parts)}
        ORDER BY score DESC, id DESC
        LIMIT %s
        """,
        (*values, limit * 3),
    ).fetchall()
    conn.close()
    created = 0
    managed = 0
    no_candidate = 0
    checked = 0
    delayed = 0
    now = _now()
    ready_at = _now_plus(minutes=assignment_delay_minutes)
    try:
        for row in rows:
            if checked >= limit:
                break
            checked += 1
            group_id = int(row['id'])
            assignment_status = row['assignment_status'] or 'ready_assign'
            current_broadcast_status = str(row['broadcast_status'] or 'hold')
            owner_phone = row['owner_phone']
            if owner_phone:
                conn = get_conn()
                rel = conn.execute('SELECT 1 FROM akun_grup WHERE phone=%s AND grup_id=%s', (owner_phone, group_id)).fetchone()
                conn.close()
                latest = _latest_assignment_for_group(group_id)
                if rel:
                    if latest and latest.get('status') != 'managed':
                        update_assignment(int(latest['id']), status='managed', last_attempt_at=now)
                    fields = {'assignment_status': 'managed', 'owner_phone': owner_phone}
                    if current_broadcast_status not in {'queued', 'sending', 'cooldown', 'blocked', 'hold_inactive', 'hold_waiting_response'}:
                        fields.update({'broadcast_status': 'stabilization_wait', 'broadcast_hold_reason': 'new_assignment_wait', 'broadcast_ready_at': ready_at})
                        delayed += 1
                    _set_group_state(group_id, **fields)
                    managed += 1
                    continue
                # Penting: jika grup sudah punya owner assigned yang valid di state/assignment,
                # jangan pilih owner baru lagi pada stage_assignment berikutnya. Ini mencegah hasil
                # reassign di stage join ditimpa pada siklus berikutnya.
                latest_owner = str(latest.get('assigned_account_id') or '') if latest else ''
                if assignment_status == 'assigned' and (not latest or latest_owner in {'', str(owner_phone)}):
                    _set_group_state(group_id, assignment_status='assigned', owner_phone=owner_phone, broadcast_status='stabilization_wait', broadcast_hold_reason='waiting_owner_join', broadcast_ready_at=ready_at)
                    delayed += 1
                    continue
            if assignment_status not in assignment_statuses:
                continue
            best, candidates = _choose_candidate(group_id)
            if not prefer_joined_owner and candidates:
                best = candidates[0]
            if not best:
                no_candidate += 1
                if create_recovery_on_no_candidate:
                    create_or_update_recovery_item(
                        'assignment', str(group_id),
                        entity_name=row['nama'], current_status=assignment_status, worker_status='degraded',
                        problem_type='no_candidate', severity='high', recovery_status='recoverable',
                        last_activity_at=now, note='Tidak ada akun kandidat yang memenuhi syarat'
                    )
                continue
            selected = str(best['account_id'])
            snapshot = json.dumps(candidates[:5], ensure_ascii=False)
            conn = get_conn()
            rel = conn.execute('SELECT 1 FROM akun_grup WHERE phone=%s AND grup_id=%s', (selected, group_id)).fetchone()
            conn.close()
            target_status = 'managed' if rel else 'assigned'
            latest = _latest_assignment_for_group(group_id)
            if latest:
                update_assignment(int(latest['id']), assigned_account_id=selected, status=target_status, assign_reason='orchestrator_auto_assign', assign_score_snapshot=snapshot, last_attempt_at=now)
                aid = int(latest['id'])
            else:
                aid = create_assignment(group_id, selected, status=target_status, assign_reason='orchestrator_auto_assign', assign_score_snapshot=snapshot)
            if target_status == 'managed':
                _set_group_state(group_id, assignment_status='managed', broadcast_status='stabilization_wait', broadcast_hold_reason='new_assignment_wait', broadcast_ready_at=ready_at, owner_phone=selected)
                managed += 1
            else:
                _set_group_state(group_id, assignment_status='assigned', broadcast_status='stabilization_wait', broadcast_hold_reason='waiting_owner_join', broadcast_ready_at=ready_at, owner_phone=selected)
                created += 1
            delayed += 1
            create_or_update_recovery_item('assignment', str(aid), recovery_status='recovered', last_recovery_result='assignment_synced', last_recovery_at=now)
        result = {'checked': checked, 'assigned': created, 'managed': managed, 'no_candidate': no_candidate, 'delayed_for_stabilization': delayed, 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
        record_stage_result('assignment', matched_rules, True, result)
        if created or managed:
            _log('info', 'orchestrator', 'stage_assignment', f'Assignment otomatis: {created} assigned, {managed} managed, {delayed} masuk stabilisasi', entity_type='assignment', entity_id='bulk', result='success')
        return result
    except Exception:
        record_stage_result('assignment', matched_rules, False, {'checked': checked, 'assigned': created, 'managed': managed, 'no_candidate': no_candidate, 'delayed_for_stabilization': delayed})
        raise


def stage_campaign_prepare(limit: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('campaign_prepare')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule campaign_prepare yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan['effective_action']
    scope = plan['effective_scope']
    limit = limit or int(action.get('limit') or _setting_int('orchestrator_campaign_batch', 200))
    sender_pool = str(get_setting('campaign_default_sender_pool', 'default') or 'default')
    group_statuses = tuple(scope.get('group_status_in') or ['active'])
    permission_statuses = tuple(scope.get('permission_status_in') or ['valid', 'owned', 'admin', 'partner_approved', 'opt_in'])
    assignment_statuses = tuple(scope.get('assignment_status_in') or ['managed'])
    broadcast_statuses_raw = tuple(scope.get('broadcast_status_in') or ['broadcast_eligible', 'hold', 'queued', 'stabilization_wait', 'cooldown'])
    broadcast_statuses = tuple(status for status in broadcast_statuses_raw if str(status) != 'failed') or ('broadcast_eligible', 'hold', 'queued', 'stabilization_wait', 'cooldown')
    exclude_channels = bool(scope.get('exclude_channels', True))
    exclude_if_already_targeted = bool(scope.get('exclude_if_already_targeted', True))
    reuse_active_campaign = bool(action.get('reuse_active_campaign', True))
    create_if_missing = bool(action.get('create_if_missing', True))
    allow_mid_session_enqueue = _rule_bool(action, 'allow_mid_session_enqueue', 'campaign_allow_mid_session_enqueue', False)
    session_target_limit = _rule_int(action, 'session_target_limit', 'campaign_session_target_limit', 50)
    promote_summary = _promote_broadcast_ready_groups(limit=max(limit * 3, 300))
    available_senders = _available_online_senders()
    if not available_senders:
        result = {
            'campaign_id': None,
            'session_key': None,
            'created_targets': 0,
            'promoted_to_queue': 0,
            'guard_skipped': 0,
            'stabilizing': 0,
            'promotion': promote_summary,
            'guard_reasons': {},
            'rule_ids': [r['id'] for r in matched_rules],
            'context': plan['context'],
            'reason': 'no_sender_capacity',
        }
        record_stage_result('campaign_prepare', matched_rules, True, result)
        _log('info', 'orchestrator', 'stage_campaign_prepare_skipped', 'Campaign prepare dilewati karena semua sender mencapai batas kirim harian', entity_type='campaign', entity_id='none', result='skipped')
        return result

    conn = get_conn()
    where_parts = [
        f"g.status IN ({','.join('%s' for _ in group_statuses)})",
        f"COALESCE(g.permission_status,'unknown') IN ({','.join('%s' for _ in permission_statuses)})",
        f"COALESCE(g.assignment_status,'ready_assign') IN ({','.join('%s' for _ in assignment_statuses)})",
        f"COALESCE(g.broadcast_status,'hold') IN ({','.join('%s' for _ in broadcast_statuses)})",
    ]
    values = [*group_statuses, *permission_statuses, *assignment_statuses, *broadcast_statuses]
    if exclude_channels:
        where_parts.append("COALESCE(g.tipe,'group') != 'channel'")
    if exclude_if_already_targeted:
        where_parts.append("NOT EXISTS (SELECT 1 FROM campaign_target ct JOIN campaign c ON c.id=ct.campaign_id WHERE ct.group_id=g.id AND ct.status IN ('eligible','queued','sending') AND c.status IN ('queued','running','paused'))")
    groups_raw = conn.execute(
        f"""
        SELECT g.id, g.nama, g.owner_phone, g.last_chat, g.last_kirim,
               COALESCE(g.broadcast_status,'hold') AS broadcast_status, g.broadcast_ready_at, g.broadcast_hold_reason
        FROM grup g
        WHERE {' AND '.join(where_parts)}
        ORDER BY g.score DESC, g.id DESC
        LIMIT %s
        """,
        (*values, limit * 5),
    ).fetchall()
    conn.close()

    guard_skipped = 0
    stabilizing = 0
    guard_reasons: dict[str, int] = {}
    groups: list[dict[str, Any]] = []
    for raw in groups_raw:
        row = dict(raw)
        owner = row.get('owner_phone')
        if owner:
            conn_chk = get_conn()
            owner_status = conn_chk.execute("SELECT status FROM akun WHERE phone=%s", (owner,)).fetchone()
            joined_chk = conn_chk.execute("SELECT 1 FROM akun_grup WHERE phone=%s AND grup_id=%s", (owner, int(row['id']))).fetchone()
            conn_chk.close()
            if owner_status and str(owner_status['status']).lower() == 'banned':
                if not joined_chk:
                    _mark_group_hold(int(row['id']), 'hold', 'owner_banned_not_joined')
                    guard_skipped += 1
                    guard_reasons['owner_banned_not_joined'] = guard_reasons.get('owner_banned_not_joined', 0) + 1
                    continue
        ready_at = _parse_dt(row.get('broadcast_ready_at'))
        if row.get('broadcast_status') in {'stabilization_wait', 'cooldown'} and (ready_at and ready_at > datetime.now()):
            stabilizing += 1
            continue
        guard = evaluate_group_send_guard(row, overrides=action)
        persist_group_send_guard(int(row['id']), guard)
        row.update(guard)
        if not guard['send_eligible']:
            guard_skipped += 1
            guard_reasons[guard['send_guard_reason_code']] = guard_reasons.get(guard['send_guard_reason_code'], 0) + 1
            _mark_group_hold(int(row['id']), guard['send_guard_status'], guard['send_guard_reason_code'])
            continue
        groups.append(row)
        if len(groups) >= limit:
            break

    session_note = 'Auto session orchestrator'
    campaign = None
    if reuse_active_campaign or create_if_missing:
        campaign = _resolve_session_campaign(
            sender_pool=sender_pool,
            target_limit=session_target_limit,
            allow_mid_session_enqueue=allow_mid_session_enqueue,
            create_if_missing=create_if_missing,
            note=session_note,
        )
    if not groups or not campaign:
        result = {
            'campaign_id': int(campaign['id']) if campaign else None,
            'session_key': campaign.get('session_key') if campaign else None,
            'created_targets': 0,
            'promoted_to_queue': 0,
            'guard_skipped': guard_skipped,
            'stabilizing': stabilizing,
            'promotion': promote_summary,
            'guard_reasons': guard_reasons,
            'rule_ids': [r['id'] for r in matched_rules],
            'context': plan['context'],
            'reason': 'no_eligible_groups' if not groups else 'no_session_available',
        }
        record_stage_result('campaign_prepare', matched_rules, True, result)
        return result

    campaign_id = int(campaign['id'])
    target_limit_effective = int(campaign.get('session_target_limit') or session_target_limit or 0)
    current_target_count = 0
    conn = get_conn()
    start_pos_row = conn.execute('SELECT MAX(COALESCE(queue_position,0)) AS max_pos, COUNT(*) AS total_count FROM campaign_target WHERE campaign_id=%s', (campaign_id,)).fetchone()
    next_pos = int((start_pos_row['max_pos'] or 0) + 1)
    current_target_count = int(start_pos_row['total_count'] or 0)
    conn.close()
    if target_limit_effective > 0:
        available_slots = max(0, target_limit_effective - current_target_count)
    else:
        available_slots = limit
    groups = groups[:available_slots]
    if not groups:
        result = {
            'campaign_id': campaign_id,
            'session_key': campaign.get('session_key'),
            'created_targets': 0,
            'promoted_to_queue': 0,
            'guard_skipped': guard_skipped,
            'stabilizing': stabilizing,
            'promotion': promote_summary,
            'guard_reasons': guard_reasons,
            'rule_ids': [r['id'] for r in matched_rules],
            'context': plan['context'],
            'reason': 'session_full',
        }
        record_stage_result('campaign_prepare', matched_rules, True, result)
        return result

    payload = []
    session_key = str(campaign.get('session_key') or '')
    staged_at = _now()
    for idx, row in enumerate(groups, start=0):
        payload.append((campaign_id, int(row['id']), row.get('owner_phone'), 'queued', 'orchestrator_managed_group', next_pos + idx, staged_at, session_key))
    conn = get_conn()
    conn.executemany(
        "INSERT INTO campaign_target (campaign_id, group_id, sender_account_id, status, eligibility_reason, queue_position, staged_at, session_key) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        payload,
    )
    conn.commit()
    conn.close()
    for row in groups:
        _set_group_state(int(row['id']), broadcast_status='queued', broadcast_hold_reason=None)
    summary = _refresh_campaign_counts(campaign_id) or {}
    result = {
        'campaign_id': campaign_id,
        'session_key': session_key,
        'created_targets': len(groups),
        'promoted_to_queue': len(groups),
        'guard_skipped': guard_skipped,
        'stabilizing': stabilizing,
        'promotion': promote_summary,
        'guard_reasons': guard_reasons,
        'campaign_summary': summary,
        'session_target_limit': target_limit_effective,
        'rule_ids': [r['id'] for r in matched_rules],
        'context': plan['context'],
    }
    record_stage_result('campaign_prepare', matched_rules, True, result)
    _log('info', 'orchestrator', 'stage_campaign_prepare', f'Orchestrator menambahkan {len(groups)} target ke session campaign #{campaign_id}, menahan {guard_skipped} grup, {stabilizing} masih stabilisasi', entity_type='campaign', entity_id=str(campaign_id), result='success')
    return result


def _get_broadcast_throttle(phone: str) -> dict:
    """Ambil state throttle broadcast per akun dari database."""
    try:
        conn = get_conn()
        row = conn.execute(
            'SELECT * FROM broadcast_throttle_akun WHERE phone=%s', (phone,)
        ).fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}

def _set_broadcast_throttle(phone: str, last_broadcast_at: str, next_allowed_at: str):
    """Simpan state throttle broadcast per akun."""
    try:
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO broadcast_throttle_akun (phone, last_broadcast_at, next_allowed_at)
            VALUES (%s, %s, %s)
            ON CONFLICT(phone) DO UPDATE SET
                last_broadcast_at=excluded.last_broadcast_at,
                next_allowed_at=excluded.next_allowed_at
            """,
            (phone, last_broadcast_at, next_allowed_at)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'[Broadcast] Gagal simpan throttle akun {phone}: {e}')

def _hitung_jeda_broadcast(phone: str) -> float:
    """
    Hitung jeda (menit) antar kirim per akun berdasarkan kuota harian akun tersebut.
    Jeda = (jam_aktif × 60) ÷ kuota_akun
    Dibatasi antara jeda_min dan jeda_max dari pengaturan.
    """
    jeda_min = max(1, int(get_setting('broadcast_jeda_min_menit', 1) or 1))
    jeda_max = max(jeda_min, int(get_setting('broadcast_jeda_max_menit', 10) or 10))
    jam_mulai = int(get_setting('broadcast_jam_mulai', 0) or 0)
    jam_selesai = int(get_setting('broadcast_jam_selesai', 0) or 0)

    # jam_mulai == jam_selesai → 24 jam aktif
    if jam_mulai == jam_selesai:
        jam_aktif = 24
    else:
        jam_aktif = max(1, abs(jam_selesai - jam_mulai))

    # Kuota per akun ini saja (bukan gabungan)
    kuota_akun = max(0, int(_send_quota_snapshot(phone).get('limit', 0) or 0))

    if not kuota_akun or kuota_akun <= 0:
        return float(jeda_max)

    jeda_ideal = (jam_aktif * 60) / kuota_akun

    import random
    variasi = jeda_ideal * 0.2
    jeda_acak = jeda_ideal + random.uniform(-variasi, variasi)

    return max(float(jeda_min), min(120.0, jeda_acak))

def _cek_spambot_akun(phone: str) -> str:
    """
    Cek status akun via SpamBot Telegram secara sinkron.
    Return: 'diblokir' | 'aman' | 'error'
    """
    from services.account_manager import _clients, run_sync
    import asyncio as _asyncio

    client = _clients.get(phone)
    if not client:
        return 'error'

    _BLOCKED_SIGNALS = (
        'blocked', 'limited', 'violations',
        'terms of service', 'moderators',
        'your account has been', 'remain blocked',
    )

    async def _tanya_spambot(c):
        try:
            await c.send_message('SpamBot', '/start')
            await _asyncio.sleep(3)
            msgs = await c.get_messages('SpamBot', limit=2)
            for msg in (msgs or []):
                if msg and msg.message:
                    return msg.message.lower()
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ('peerflood', 'peer_flood', 'flood')):
                return 'blocked'
            return 'error'
        return 'error'

    try:
        reply = run_sync(_tanya_spambot(client), timeout=25)
        reply = str(reply or '').lower()
        if any(x in reply for x in _BLOCKED_SIGNALS):
            return 'diblokir'
        return 'aman'
    except Exception:
        return 'error'


def _broadcast_boleh_kirim_sekarang(phone: str) -> tuple[bool, str]:
    """
    Cek apakah akun tertentu boleh kirim sekarang berdasarkan:
    1. Jam aktif (jam_mulai - jam_selesai)
    2. Jeda sejak terakhir kirim per akun
    Kembalikan (boleh, alasan)
    """
    from datetime import datetime as _dtt

    throttle_enabled = int(get_setting('broadcast_throttle_enabled', 1) or 1)
    if not throttle_enabled:
        return True, 'throttle_disabled'

    try:
        jam_mulai = int(get_setting('broadcast_jam_mulai', 0) or 0)
        jam_selesai = int(get_setting('broadcast_jam_selesai', 0) or 0)
    except (ValueError, TypeError):
        jam_mulai, jam_selesai = 0, 0

    sekarang = _dtt.now()
    jam_sekarang = sekarang.hour

    # jam_mulai == jam_selesai → 24 jam aktif, skip cek jam
    if jam_mulai != jam_selesai and not (jam_mulai <= jam_sekarang < jam_selesai):
        return False, f'di_luar_jam_aktif ({jam_mulai}:00-{jam_selesai}:00)'

    # Cek jeda per akun
    throttle = _get_broadcast_throttle(phone)
    next_allowed = throttle.get('next_allowed_at')
    if next_allowed:
        try:
            next_dt = _dtt.strptime(str(next_allowed)[:19], '%Y-%m-%d %H:%M:%S')
            if sekarang < next_dt:
                sisa = int((next_dt - sekarang).total_seconds() / 60)
                return False, f'jeda_belum_selesai akun {phone} (tunggu {sisa} menit)'
        except Exception:
            pass

    return True, 'ok'

def _hitung_jeda_join(phone: str) -> int:
    """
    Hitung jeda join secara ACAK berbasis sisa kuota hari ini.

    Prinsip:
    - Jeda = sisa waktu hari ini ÷ sisa kuota yang belum dipakai
    - Ditambah faktor acak ±20% agar tidak terlihat robotic
    - Dibatasi antara min dan max per level warming
    """
    import random as _random
    from datetime import datetime as _dtt, timedelta as _td

    try:
        conn_wm = get_conn()
        row_wm = conn_wm.execute(
            "SELECT level_warming FROM akun WHERE phone=%s", (phone,)
        ).fetchone()
        conn_wm.close()
        level = int(row_wm["level_warming"] or 1) if row_wm else 1
    except Exception:
        level = 1

    BATAS = {
        1: {'min': 1800, 'max': 14400},
        2: {'min': 900,  'max': 7200},
        3: {'min': 300,  'max': 5400},
        4: {'min': 120,  'max': 3600},
    }
    batas = BATAS.get(level, BATAS[4])

    try:
        quota = _join_quota_snapshot(phone)
        sisa_kuota = max(1, int(quota.get('remaining') or 1))
        batas_harian = int(quota.get('limit') or 0)
    except Exception:
        sisa_kuota = 1
        batas_harian = 0

    if batas_harian <= 0:
        jeda = batas['min']
        faktor = _random.uniform(0.8, 1.2)
        return max(batas['min'], min(batas['max'], int(jeda * faktor)))

    sekarang = _dtt.now()
    tengah_malam = (sekarang + _td(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    sisa_detik = max(300, int((tengah_malam - sekarang).total_seconds()))

    jeda_ideal = sisa_detik / sisa_kuota

    faktor = _random.uniform(0.8, 1.2)
    jeda_final = int(jeda_ideal * faktor)

    jeda_final = max(batas['min'], min(batas['max'], jeda_final))

    return jeda_final


def _join_boleh_sekarang(phone: str) -> tuple[bool, str]:
    """Cek apakah akun boleh join sekarang berdasarkan next_join_at throttle."""
    next_join = get_next_join_at(phone)
    if not next_join:
        return True, ''
    now_str = _now()
    if now_str >= next_join:
        return True, ''
    return False, f'throttle join hingga {next_join}'


def _set_join_throttle(phone: str, jeda_detik: int) -> None:
    """Set next_join_at untuk throttle join akun."""
    set_next_join_at(phone, _now_plus(seconds=jeda_detik))


def _shuffle_putaran_broadcast(campaign_id: int):
    """
    Acak ulang urutan queue_position semua target queued/eligible/skipped dalam campaign.
    Dipanggil saat putaran baru dimulai (semua target sudah terkirim/skip).
    """
    import random as _rand
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id FROM campaign_target WHERE campaign_id=%s AND status IN ('queued','eligible','skipped')",
            (campaign_id,)
        ).fetchall()
        if not rows:
            return
        ids = [r['id'] for r in rows]
        _rand.shuffle(ids)
        for urutan, target_id in enumerate(ids, start=1):
            conn.execute('UPDATE campaign_target SET queue_position=%s WHERE id=%s', (urutan, target_id))
        conn.commit()
        print(f'[Broadcast] Putaran baru — {len(ids)} grup dikocok ulang secara acak')
    except Exception as e:
        print(f'[Broadcast] Gagal kocok putaran: {e}')
    finally:
        conn.close()

def stage_delivery(limit: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('delivery')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule delivery yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan['effective_action']
    scope = plan['effective_scope']
    limit = limit or int(action.get('limit') or _setting_int('orchestrator_delivery_batch', 10))
    retry_delay_minutes = int(action.get('retry_delay_minutes') or _setting_int('campaign_retry_delay_minutes', 10))
    blocked_terms = [str(x).lower() for x in (action.get('blocked_terms') or ['tidak punya izin', 'banned', 'private'])]
    target_statuses_raw = tuple(scope.get('target_status_in') or ['queued', 'eligible'])
    allow_failed_targets = bool(action.get('allow_failed_targets', False))
    target_statuses = tuple(status for status in target_statuses_raw if allow_failed_targets or str(status) != 'failed') or ('queued', 'eligible')
    campaign_statuses = tuple(scope.get('campaign_status_in') or ['queued', 'running'])
    require_active_draft = _rule_bool(action, 'require_active_draft', 'broadcast_hanya_pakai_draft_aktif', True)
    require_online_sender = bool(action.get('require_online_sender', True))
    session_per_sender_limit = _rule_int(action, 'session_per_sender_limit', 'campaign_session_per_sender_limit', 5)
    group_cooldown_hours = _rule_int(action, 'group_cooldown_hours', 'campaign_group_cooldown_hours', 0)
    group_cooldown_minutes = _rule_int(action, 'group_cooldown_minutes', 'campaign_group_cooldown_minutes', 1)
    requeue_sender_missing = _rule_bool(action, 'requeue_sender_missing', 'campaign_requeue_sender_missing', True)
    max_attempts = _max_delivery_attempts()
    draft = get_draft_aktif()
    if require_active_draft and (not draft or not draft.get('isi')):
        _log('warning', 'delivery', 'stage_delivery_skipped', 'Delivery dilewati karena tidak ada draft aktif', entity_type='campaign', entity_id='none', result='skipped')
        return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': 'no_active_draft', 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
    if require_online_sender and not _clients:
        _log('warning', 'delivery', 'stage_delivery_skipped', 'Delivery dilewati karena tidak ada akun online', entity_type='campaign', entity_id='none', result='skipped')
        return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': 'no_online_accounts', 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
    available_senders = _available_online_senders()
    if require_online_sender and not available_senders:
        _log('info', 'delivery', 'stage_delivery_skipped', 'Delivery dilewati karena semua sender mencapai batas kirim harian', entity_type='campaign', entity_id='none', result='skipped')
        return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': 'no_sender_capacity', 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}

    # Cek throttle per akun — cari sender yang tersedia dan jedanya sudah selesai
    boleh_kirim = False
    alasan_throttle = 'semua_sender_dalam_jeda'
    for _sender_phone in available_senders:
        _boleh, _alasan = _broadcast_boleh_kirim_sekarang(_sender_phone)
        if _boleh:
            boleh_kirim = True
            break
        alasan_throttle = _alasan
    if not boleh_kirim:
        _log('info', 'delivery', 'stage_delivery_skipped', f'Delivery dilewati: {alasan_throttle}', entity_type='campaign', entity_id='none', result='skipped')
        return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': alasan_throttle, 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}

    sessions = [_row_to_dict(s) for s in _session_candidates()]
    chosen_campaign = next((s for s in sessions if _row_get(s, 'status') == 'running'), None) or next((s for s in sessions if _row_get(s, 'status') == 'queued'), None)
    chosen_campaign = _row_to_dict(chosen_campaign) if chosen_campaign else None
    if not chosen_campaign:
        _log('info', 'delivery', 'stage_delivery_skipped', 'Delivery dilewati karena tidak ada session campaign aktif', entity_type='campaign', entity_id='none', result='skipped')
        return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': 'no_active_session', 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}

    campaign_id = int(chosen_campaign['id'])
    if _row_get(chosen_campaign, 'status') == 'queued':
        update_campaign(campaign_id, status='running', started_at=_row_get(chosen_campaign, 'started_at') or _now(), session_status='running', session_started_at=_row_get(chosen_campaign, 'session_started_at') or _now())

    # Auto-complete campaign yang semua targetnya sudah selesai
    # Ini mencegah campaign running yang kosong memblokir campaign berikutnya
    _conn_check = get_conn()
    _remaining = _conn_check.execute(
        """SELECT COUNT(*) as c FROM campaign_target
           WHERE campaign_id=%s AND status IN ('queued','eligible','sending')""",
        (campaign_id,)
    ).fetchone()['c']
    _conn_check.close()
    if _remaining == 0 and _row_get(chosen_campaign, 'status') == 'running':
        # Campaign kosong → auto-complete dan cari campaign berikutnya
        _refresh_campaign_counts(campaign_id)
        _log('info', 'delivery', 'campaign_auto_completed',
             f'Campaign #{campaign_id} otomatis diselesaikan karena semua target sudah selesai',
             entity_type='campaign', entity_id=str(campaign_id), result='completed')
        print(f'[Broadcast] Campaign #{campaign_id} selesai otomatis — beralih ke campaign berikutnya')
        # Cari campaign berikutnya
        _sessions_next = [_row_to_dict(s) for s in _session_candidates()]
        chosen_campaign = next((s for s in _sessions_next if _row_get(s, 'status') == 'running' and int(_row_get(s, 'id')) != campaign_id), None) or                          next((s for s in _sessions_next if _row_get(s, 'status') == 'queued'), None)
        chosen_campaign = _row_to_dict(chosen_campaign) if chosen_campaign else None
        if not chosen_campaign:
            _log('info', 'delivery', 'stage_delivery_skipped', 'Tidak ada campaign aktif berikutnya', entity_type='campaign', entity_id='none', result='skipped')
            return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': 'no_next_campaign', 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
        campaign_id = int(chosen_campaign['id'])
        if _row_get(chosen_campaign, 'status') == 'queued':
            update_campaign(campaign_id, status='running', started_at=_now(), session_status='running', session_started_at=_now())

    # Throttle aktif → kirim sebanyak akun yang jedanya sudah selesai di siklus ini
    throttle_enabled = int(_setting_int('broadcast_throttle_enabled', 1))
    if throttle_enabled:
        _senders_siap = [
            p for p in available_senders
            if _broadcast_boleh_kirim_sekarang(p)[0]
        ]
        limit = max(1, len(_senders_siap))

    # Hitung total kuota kirim semua sender untuk jeda otomatis
    _total_kuota_hari_ini = sum(
        max(0, int(_send_quota_snapshot(p).get('limit', 0) or 0))
        for p in list(_clients.keys())
    )
    # Proteksi: kalau kuota 0 jangan lanjut delivery
    if throttle_enabled and _total_kuota_hari_ini <= 0:
        _log('info', 'delivery', 'stage_delivery_skipped', 'Delivery dilewati: total kuota 0', entity_type='campaign', entity_id='none', result='skipped')
        return {'processed': 0, 'sent': 0, 'failed': 0, 'blocked': 0, 'skipped': 1, 'reason': 'no_quota', 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
    # Cek apakah semua target putaran ini sudah selesai → kocok ulang
    _conn_sisa = get_conn()
    total_sisa = _conn_sisa.execute(
        "SELECT COUNT(*) as c FROM campaign_target WHERE campaign_id=%s AND status IN ('queued','eligible')",
        (campaign_id,)
    ).fetchone()['c']
    _conn_sisa.close()
    if total_sisa == 0:
        _shuffle_putaran_broadcast(campaign_id)
        # Reset status skipped → queued agar masuk putaran baru
        try:
            conn_reset = get_conn()
            conn_reset.execute(
                "UPDATE campaign_target SET status='queued', next_attempt_at=NULL WHERE campaign_id=%s AND status='skipped'",
                (campaign_id,)
            )
            conn_reset.commit()
            conn_reset.close()
            print(f'[Broadcast] Reset putaran — semua skipped dikembalikan ke antrian')
        except Exception as _e:
            print(f'[Broadcast] Gagal reset putaran: {_e}')

    conn = get_conn()
    rows = conn.execute(
        f"""
        SELECT ct.*, c.name as campaign_name, c.session_key as campaign_session_key, g.nama as group_name, g.owner_phone, g.last_chat, g.last_kirim,
               COALESCE(g.broadcast_status,'hold') AS broadcast_status, g.broadcast_ready_at,
               COALESCE(g.broadcast_attempt_count, 0) AS broadcast_attempt_count,
               g.broadcast_last_sender
        FROM campaign_target ct
        JOIN campaign c ON c.id=ct.campaign_id
        JOIN grup g ON g.id=ct.group_id
        WHERE ct.campaign_id=%s
          AND c.status IN ({','.join('%s' for _ in campaign_statuses)})
          AND ct.status IN ({','.join('%s' for _ in target_statuses)})
          AND (ct.next_attempt_at IS NULL OR ct.next_attempt_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
        ORDER BY COALESCE(ct.queue_position, ct.id) ASC
        LIMIT %s
        """,
        # Throttle aktif: fetch lebih banyak kandidat agar ada pengganti jika skip guard
        (campaign_id, *campaign_statuses, *target_statuses, max(limit * 5, limit) if not throttle_enabled else max(50, limit * 5)),
    ).fetchall()
    conn.close()
    rows = [_row_to_dict(r) for r in rows]

    def _row_val(r, key, default=None):
        return _row_get(r, key, default)

    processed = sent = failed = blocked = skipped = 0
    sender_counts: dict[str, int] = defaultdict(int)
    _log('info', 'delivery', 'stage_delivery_start', f'Delivery stage mulai: campaign={campaign_id}, candidates={len(rows)}, limit={limit}', entity_type='campaign', entity_id=str(campaign_id), result='running')
    try:
        for row in rows:
            if processed >= limit:
                break
            target_id = int(row['id'])
            preferred_sender = _row_val(row, 'sender_account_id') or _row_val(row, 'owner_phone')
            resolved_sender, sender_candidates = _resolve_sender_for_group(int(row['group_id']), preferred_sender, require_online_sender=require_online_sender)
            if resolved_sender and session_per_sender_limit > 0 and sender_counts[resolved_sender] >= session_per_sender_limit:
                continue
            processed += 1
            attempts = int(_row_val(row, 'attempt_count') or 0)
            row_dict = dict(row) if not isinstance(row, dict) else row
            guard = evaluate_group_send_guard(row_dict, overrides=action)
            persist_group_send_guard(int(_row_val(row, 'group_id')), guard)
            if not guard['send_eligible']:
                skip_payload = json.dumps({'status': 'skipped', 'reason': guard['send_guard_reason_code'], 'detail': guard['send_guard_reason']}, ensure_ascii=False)
                update_queue_target(target_id, status='skipped', sender_account_id=resolved_sender or preferred_sender, delivery_result=skip_payload, failure_reason=guard['send_guard_reason_code'], hold_reason=guard['send_guard_reason_code'], finalized_at=None, last_outcome_code='guard_skipped')
                _mark_group_hold(int(_row_val(row, 'group_id')), guard['send_guard_status'], guard['send_guard_reason_code'])
                skipped += 1
                _log('info', 'delivery', 'target_skipped_guard', f"Target {target_id} dilewati guard: {guard['send_guard_reason_code']}", entity_type='campaign_target', entity_id=str(target_id), result='skipped')
                # Throttle aktif + skip karena guard → kuota tidak terbuang
                # Kurangi processed agar limit tidak berkurang — cari pengganti di iterasi berikut
                # Tapi batasi max skip berturut-turut agar tidak infinite loop
                if throttle_enabled:
                    processed = max(0, processed - 1)
                    if skipped >= len(rows):
                        # Semua kandidat di-skip guard → hentikan loop
                        break
                continue
            if not resolved_sender:
                next_attempt_at = _now_plus(minutes=retry_delay_minutes)
                if requeue_sender_missing and attempts + 1 < max_attempts:
                    update_queue_target(target_id, status='queued', sender_account_id=preferred_sender, attempt_count=attempts + 1, last_attempt_at=_now(), next_attempt_at=next_attempt_at, failure_reason='sender_missing', hold_reason='sender_missing', last_outcome_code='sender_missing')
                    _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='queued', broadcast_hold_reason='sender_missing')
                    _log('warning', 'delivery', 'sender_inactive', f'Target {target_id} diantrekan ulang karena sender tidak siap', entity_type='campaign_target', entity_id=str(target_id), result='queued')
                else:
                    update_queue_target(target_id, status='failed', sender_account_id=preferred_sender, attempt_count=attempts + 1, last_attempt_at=_now(), next_attempt_at=None, failure_reason='sender_missing', hold_reason='sender_missing', finalized_at=_now(), last_outcome_code='sender_missing')
                    _mark_group_hold(int(_row_val(row, 'group_id')), 'failed', 'sender_missing')
                    failed += 1
                    _log('error', 'delivery', 'send_failed', f'Target {target_id} gagal final karena sender tidak siap', entity_type='campaign_target', entity_id=str(target_id), result='failed')
                create_or_update_recovery_item('campaign', str(campaign_id), entity_name=_row_val(row, 'campaign_name'), current_status='failed', worker_status='degraded', problem_type='sender_missing', severity='medium', recovery_status='recoverable', last_activity_at=_now(), note=f'Target {target_id} tidak punya sender online yang layak')
                continue

            sender_counts[resolved_sender] += 1
            update_queue_target(target_id, status='sending', sender_account_id=resolved_sender, attempt_count=attempts + 1, last_attempt_at=_now(), reserved_at=_now(), session_key=str(_row_val(row, 'campaign_session_key') or _row_val(row, 'session_key') or ''), dispatch_slot=sender_counts[resolved_sender])
            try:
                _log('info', 'delivery', 'target_dispatch', f'Target {target_id} dikirim oleh {resolved_sender}', entity_type='campaign_target', entity_id=str(target_id), result='dispatching')
                result_send = run_sync(kirim_pesan_manual(str(resolved_sender), int(_row_val(row, 'group_id')), str(draft['isi'])), timeout=180)
            except Exception as exc:
                result_send = {'status': 'error', 'pesan': str(exc)}
            status = str(result_send.get('status') or '').lower()
            payload = json.dumps(result_send, ensure_ascii=False)
            message = str(result_send.get('pesan') or result_send.get('message') or '')
            if status == 'berhasil':
                update_queue_target(target_id, status='sent', sender_account_id=resolved_sender, delivery_result=payload, failure_reason=None, next_attempt_at=None, finalized_at=_now(), last_outcome_code='sent')
                tandai_grup_masa_istirahat(int(_row_val(row, 'group_id')), cooldown_hours=group_cooldown_hours, cooldown_minutes=group_cooldown_minutes)
                sent += 1
                _log('info', 'delivery', 'send_success', f'Target {target_id} berhasil dikirim oleh {resolved_sender}', entity_type='campaign_target', entity_id=str(target_id), result='sent')
                # Update throttle per akun — hitung next_allowed_at dari jeda otomatis akun ini
                if throttle_enabled:
                    jeda_menit = _hitung_jeda_broadcast(resolved_sender)
                    next_allowed = _now_plus(minutes=jeda_menit)
                    _set_broadcast_throttle(resolved_sender, _now(), next_allowed)
                    print(f'[Broadcast] Terkirim ke {_row_val(row, "group_name")} oleh {resolved_sender} — jeda {jeda_menit:.1f} menit, next: {next_allowed[:16]}')
            else:
                msg_lower = message.lower()
                blocked_reason_code = None
                daily_limit_exhausted = 'batas harian tercapai' in msg_lower or 'harian tercapai' in msg_lower

                _AKUN_BANNED_SIGNALS = ('authkeyunregistered','userdeactivated','phonenumberbanned','sessionrevoked','auth key unregistered','user deactivated','account suspended','account banned')
                if any(x in msg_lower for x in _AKUN_BANNED_SIGNALS):
                    if tandai_akun_banned(resolved_sender):
                        print(f"[Broadcast] ⛔ {resolved_sender} DIBEKUKAN TELEGRAM — otomatis ditandai banned")
                    _clients.pop(resolved_sender, None)
                    update_queue_target(target_id, status='queued', sender_account_id=None, delivery_result=payload, failure_reason=message, hold_reason='sender_account_banned', next_attempt_at=_now_plus(minutes=5), finalized_at=None, last_outcome_code='sender_account_banned')
                    _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='queued', broadcast_hold_reason='sender_account_banned')
                    skipped += 1
                    continue

                # Deteksi FloodWait saat kirim
                flood_wait = 'flood wait' in msg_lower or 'floodwait' in msg_lower
                if flood_wait:
                    import re as _re
                    _fw_match = _re.search(r'(\d+)', message)
                    fw_detik = int(_fw_match.group(1)) if _fw_match else 60
                    fw_detik = max(60, fw_detik)
                    retry_at = _now_plus(seconds=fw_detik + 10)
                    update_queue_target(target_id, status='queued', sender_account_id=resolved_sender,
                                      delivery_result=payload, failure_reason=f'flood_wait_{fw_detik}s',
                                      hold_reason='flood_wait', next_attempt_at=retry_at, finalized_at=None,
                                      last_outcome_code='flood_wait')
                    _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='queued', broadcast_hold_reason='flood_wait')
                    skipped += 1
                    _log('warning', 'delivery', 'send_floodwait',
                         f'FloodWait {fw_detik}s saat kirim ke {_row_val(row, "group_name")} oleh {resolved_sender}',
                         entity_type='campaign_target', entity_id=str(target_id), result='queued')
                    print(f'[Broadcast] ⏳ FloodWait {fw_detik}s — target ditunda')
                    continue

                if 'chat_send_plain_forbidden' in msg_lower or 'chat_write_forbidden' in msg_lower or 'write_forbidden' in msg_lower:
                    blocked_reason_code = 'send_forbidden'
                elif 'topic_closed' in msg_lower:
                    blocked_reason_code = 'topic_closed'
                elif any(term in msg_lower for term in blocked_terms):
                    blocked_reason_code = 'delivery_blocked'

                if blocked_reason_code:
                    # Klasifikasi penyebab blocked
                    akun_diblokir_di_grup = (
                        'dibanned di grup' in msg_lower or
                        'user banned' in msg_lower or
                        'userbannedinchannel' in msg_lower or
                        'you were kicked' in msg_lower
                    )
                    grup_sementara_restricted = blocked_reason_code in ('send_forbidden', 'topic_closed')
                    izin_tidak_ada = 'tidak punya izin' in msg_lower or 'write_forbidden' in msg_lower

                    # Cek apakah ada akun lain yang bisa kirim ke grup ini
                    akun_lain_tersedia = len([
                        c for c in sender_candidates
                        if str(c.get('phone')) != resolved_sender
                        and str(c.get('phone')) in _clients
                    ]) > 0

                    # === SISTEM 2x PERCOBAAN + BLACKLIST ===
                    # Ambil riwayat percobaan — gunakan data dari row yang sudah di-fetch
                    _attempt_count = int((row.get('broadcast_attempt_count') if hasattr(row, 'get') else 0) or 0)
                    _last_sender = str((row.get('broadcast_last_sender') if hasattr(row, 'get') else '') or '')

                    # Error yang tidak dihitung sebagai percobaan (bukan salah grup/akun)
                    _is_transient_error = (
                        'flood wait' in msg_lower or 'floodwait' in msg_lower or
                        'timeout' in msg_lower or 'network' in msg_lower or
                        'batas harian' in msg_lower or 'harian tercapai' in msg_lower or
                        ('sender' in msg_lower and 'tidak tersedia' in msg_lower)
                    )

                    if not _is_transient_error:
                        # Sender berganti dari sebelumnya%s Reset hitungan sender
                        if _last_sender and _last_sender != resolved_sender:
                            _new_attempt = 1
                        else:
                            _new_attempt = _attempt_count + 1

                        # Update hitungan percobaan via _set_group_state (aman, tidak buka koneksi baru)
                        _set_group_state(int(_row_val(row, 'group_id')),
                                        broadcast_attempt_count=_new_attempt,
                                        broadcast_last_sender=resolved_sender)

                        # Percobaan ke-2 dengan akun yang sama → coba akun lain dulu
                        if _new_attempt == 2 and akun_lain_tersedia:
                            retry_at = _now_plus(hours=24)
                            update_queue_target(target_id, status='queued', sender_account_id=None,
                                              delivery_result=payload, failure_reason=message,
                                              hold_reason='percobaan2_coba_akun_lain',
                                              next_attempt_at=retry_at, finalized_at=None,
                                              last_outcome_code='percobaan2_ganti_akun')
                            failed += 1
                            _log('warning', 'delivery', 'broadcast_ganti_akun',
                                 f'Percobaan ke-2 gagal oleh {resolved_sender} di {_row_val(row, "group_name")} — coba akun lain',
                                 entity_type='campaign_target', entity_id=str(target_id), result='retry')
                            print(f'[Broadcast] ⚠️ Percobaan ke-2 gagal — cari akun lain untuk {_row_val(row, "group_name")}')
                            continue

                        # Percobaan ke-2 tanpa akun lain → cek SpamBot dulu
                        if _new_attempt >= 2 and not akun_lain_tersedia:
                            print(f'[Broadcast] Percobaan ke-2 gagal — cek SpamBot {resolved_sender}...')
                            status_spam = _cek_spambot_akun(resolved_sender)

                            if status_spam == 'diblokir':
                                from utils.storage_db import tandai_akun_restricted
                                from services.account_manager import _clients
                                if tandai_akun_restricted(resolved_sender,
                                    'Terdeteksi diblokir moderator setelah 2x gagal kirim — SpamBot'):
                                    print(f'[Broadcast] ⛔ {resolved_sender} DIBLOKIR MODERATOR — dikeluarkan dari otomasi')
                                _clients.pop(resolved_sender, None)

                                try:
                                    hasil_cleanup = _cleanup_banned_accounts(max_reassign=50)
                                    print(f'[Broadcast] Cleanup: sender_reset={hasil_cleanup.get("sender_reset",0)}, '
                                          f'managed_reset={hasil_cleanup.get("managed_reset",0)}, '
                                          f'owner_reassigned={hasil_cleanup.get("owner_reassigned",0)}')
                                except Exception as _ce:
                                    print(f'[Broadcast] Cleanup gagal: {_ce}')

                                update_queue_target(
                                    target_id, status='queued',
                                    sender_account_id=None,
                                    delivery_result=payload,
                                    failure_reason='sender_diblokir_spambot',
                                    hold_reason='sender_diblokir_spambot',
                                    next_attempt_at=_now_plus(minutes=10),
                                    finalized_at=None,
                                    last_outcome_code='sender_diblokir_spambot'
                                )
                                _set_group_state(
                                    int(_row_val(row, 'group_id')),
                                    broadcast_status='queued',
                                    broadcast_hold_reason='sender_diblokir_spambot',
                                    broadcast_attempt_count=0,
                                )
                                skipped += 1
                                _log('warning', 'delivery', 'sender_diblokir_spambot',
                                     f'{resolved_sender} dikeluarkan dari otomasi setelah SpamBot konfirmasi diblokir',
                                     entity_type='campaign_target', entity_id=str(target_id), result='sender_removed')
                            else:
                                update_queue_target(
                                    target_id, status='blocked',
                                    sender_account_id=resolved_sender,
                                    delivery_result=payload,
                                    failure_reason=message,
                                    hold_reason='broadcast_blacklisted',
                                    finalized_at=_now(),
                                    last_outcome_code='blacklisted_max_attempt'
                                )
                                _set_group_state(
                                    int(_row_val(row, 'group_id')),
                                    broadcast_status='blocked',
                                    broadcast_hold_reason='broadcast_blacklisted_max_attempt',
                                    broadcast_attempt_count=0,
                                )
                                blocked += 1
                                _log('warning', 'delivery', 'broadcast_blacklisted',
                                     f'Grup {_row_val(row, "group_name")} diblacklist — akun {resolved_sender} aman (SpamBot)',
                                     entity_type='campaign_target', entity_id=str(target_id), result='blacklisted')
                                print(f'[Broadcast] ⛔ BLACKLIST grup: {_row_val(row, "group_name")} (akun aman, grup bermasalah)')
                            continue
                    # === AKHIR SISTEM 2x PERCOBAAN ===

                    if akun_diblokir_di_grup and akun_lain_tersedia:
                        # Akun ini diblokir di grup tapi ada akun lain → coba akun lain
                        retry_at = _now_plus(minutes=2)
                        update_queue_target(target_id, status='queued', sender_account_id=None,
                                          delivery_result=payload, failure_reason=message,
                                          hold_reason='sender_banned_in_group',
                                          next_attempt_at=retry_at, finalized_at=None,
                                          last_outcome_code='sender_banned_in_group')
                        _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='queued',
                                        broadcast_hold_reason='sender_banned_in_group')
                        failed += 1
                        _log('warning', 'delivery', 'sender_banned_in_group',
                             f'Akun {resolved_sender} diblokir di grup {_row_val(row, "group_name")} — coba akun lain',
                             entity_type='campaign_target', entity_id=str(target_id), result='retry')
                        print(f'[Broadcast] ⚠️ {resolved_sender} diblokir di grup — cari akun lain')

                    elif akun_diblokir_di_grup and not akun_lain_tersedia:
                        # Semua akun sudah dicoba / tidak ada akun lain → hold 6 jam lalu coba lagi
                        hold_until = _now_plus(hours=6)
                        update_queue_target(target_id, status='queued', sender_account_id=None,
                                          delivery_result=payload, failure_reason=message,
                                          hold_reason='all_senders_banned', next_attempt_at=hold_until,
                                          finalized_at=None, last_outcome_code='all_senders_banned')
                        _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='hold',
                                        broadcast_hold_reason='all_senders_banned',
                                        broadcast_ready_at=hold_until)
                        blocked += 1
                        _log('warning', 'delivery', 'all_senders_banned',
                             f'Semua akun diblokir di grup {_row_val(row, "group_name")} — hold 6 jam',
                             entity_type='campaign_target', entity_id=str(target_id), result='hold')
                        print(f'[Broadcast] ⛔ Semua akun diblokir di grup — hold 6 jam')

                    elif grup_sementara_restricted or izin_tidak_ada:
                        # Grup sementara tidak bisa dikirim → hold 24 jam, coba lagi besok
                        hold_until = _now_plus(hours=24)
                        update_queue_target(target_id, status='queued', sender_account_id=resolved_sender,
                                          delivery_result=payload, failure_reason=message,
                                          hold_reason=blocked_reason_code, next_attempt_at=hold_until,
                                          finalized_at=None, last_outcome_code=blocked_reason_code)
                        _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='hold',
                                        broadcast_hold_reason=blocked_reason_code,
                                        broadcast_ready_at=hold_until)
                        blocked += 1
                        _log('warning', 'delivery', 'send_blocked',
                             f'Grup {_row_val(row, "group_name")} restricted ({blocked_reason_code}) — hold 24 jam',
                             entity_type='campaign_target', entity_id=str(target_id), result='hold')
                        print(f'[Broadcast] ⛔ Grup restricted ({blocked_reason_code}) — hold 24 jam')

                    else:
                        # Blocked permanen — grup memang tidak bisa dikirim sama sekali
                        update_queue_target(target_id, status='blocked', sender_account_id=resolved_sender,
                                          delivery_result=payload, blocked_reason=message,
                                          failure_reason=message, finalized_at=_now(),
                                          hold_reason=blocked_reason_code, last_outcome_code=blocked_reason_code)
                        _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='blocked',
                                        broadcast_hold_reason=blocked_reason_code)
                        blocked += 1
                        _log('warning', 'delivery', 'send_blocked',
                             f'Target {target_id} diblok final: {(message or blocked_reason_code)[:120]}',
                             entity_type='campaign_target', entity_id=str(target_id), result='blocked')
                        print(f'[Broadcast] 🚫 Grup diblok permanen: {blocked_reason_code}')
                elif daily_limit_exhausted:
                    _mark_sender_delivery_exhausted(str(resolved_sender))
                    sender_counts[str(resolved_sender)] = max(sender_counts.get(str(resolved_sender), 0), session_per_sender_limit or 9999)
                    retry_at = _now_plus(minutes=max(5, retry_delay_minutes))
                    update_queue_target(target_id, status='queued', sender_account_id=resolved_sender, delivery_result=payload, failure_reason='daily_limit_exhausted', hold_reason='sender_daily_limit', next_attempt_at=retry_at, finalized_at=None, last_outcome_code='daily_limit_exhausted')
                    _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='queued', broadcast_hold_reason='sender_daily_limit')
                    skipped += 1
                    _log('warning', 'delivery', 'sender_daily_limit', f'Target {target_id} ditunda karena sender {resolved_sender} mencapai batas harian', entity_type='campaign_target', entity_id=str(target_id), result='queued')
                else:
                    retry_at = _now_plus(minutes=retry_delay_minutes)
                    terminal_fail = attempts + 1 >= max_attempts
                    update_queue_target(target_id, status='failed' if terminal_fail else 'queued', sender_account_id=resolved_sender, delivery_result=payload, failure_reason=message or 'delivery_failed', next_attempt_at=None if terminal_fail else retry_at, finalized_at=_now() if terminal_fail else None, last_outcome_code='delivery_failed')
                    if terminal_fail:
                        _mark_group_hold(int(_row_val(row, 'group_id')), 'failed', 'delivery_failed')
                    else:
                        _set_group_state(int(_row_val(row, 'group_id')), broadcast_status='queued', broadcast_hold_reason='delivery_retry')
                    failed += 1
                    _log('warning' if not terminal_fail else 'error', 'delivery', 'send_failed', f'Target {target_id} gagal kirim: {(message or 'delivery_failed')[:120]}', entity_type='campaign_target', entity_id=str(target_id), result='retry' if not terminal_fail else 'failed')
                    if not terminal_fail:
                        create_or_update_recovery_item('campaign', str(campaign_id), entity_name=_row_val(row, 'campaign_name'), current_status='failed', worker_status='degraded', problem_type='delivery_failed', severity='medium', recovery_status='recoverable', last_activity_at=_now(), note=(message or 'delivery_failed')[:300])
        summary = _refresh_campaign_counts(campaign_id) or {}
        result = {'campaign_id': campaign_id, 'session_key': _row_get(chosen_campaign, 'session_key'), 'processed': processed, 'sent': sent, 'failed': failed, 'blocked': blocked, 'skipped': skipped, 'campaign_summary': summary, 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
        record_stage_result('delivery', matched_rules, True, result)
        if sent or failed or blocked or skipped:
            _log('info', 'orchestrator', 'stage_delivery', f'Delivery session #{campaign_id}: sent={sent}, failed={failed}, blocked={blocked}, skipped={skipped}', entity_type='campaign_target', entity_id='bulk', result='success')
        return result
    except Exception as exc:
        _log('error', 'delivery', 'stage_delivery_exception', f'Delivery exception: {type(exc).__name__}: {str(exc)[:180]}', entity_type='campaign', entity_id=str(campaign_id), result='failed')
        record_stage_result('delivery', matched_rules, False, {'processed': processed, 'sent': sent, 'failed': failed, 'blocked': blocked, 'skipped': skipped, 'campaign_id': campaign_id, 'error': str(exc)[:200]})
        raise


def scan_recovery_items(limit: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('recovery_scan')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule recovery_scan yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan['effective_action']
    scope = plan['effective_scope']
    watch_entities = set(scope.get('watch_entities') or ['scrape_job', 'assignment', 'campaign'])
    limit = limit or int(action.get('limit') or _setting_int('orchestrator_recovery_batch', 50))
    scrape_threshold = int(action.get('scrape_threshold_minutes') or _setting_int('recovery_stuck_scrape_threshold', 30))
    assign_threshold = int(action.get('assignment_threshold_minutes') or _setting_int('recovery_stuck_assignment_threshold', 30))
    campaign_threshold = int(action.get('campaign_threshold_minutes') or _setting_int('recovery_stuck_campaign_threshold', 30))
    resume_on_restart = _setting_bool('recovery_resume_on_restart', True)
    mark_partial_if_worker_missing = _setting_bool('recovery_mark_partial_if_worker_missing', True)
    created = {'scrape_job': 0, 'assignment': 0, 'campaign': 0}
    try:
        if 'scrape_job' in watch_entities:
            for job in get_scrape_jobs(limit=limit * 2):
                if job.get('status') not in {'queued', 'running', 'paused'}:
                    continue
                age = _minutes_since(job.get('dibuat')) or 0
                worker = _ACTIVE_THREADS.get(int(job['id']))
                worker_alive = bool(worker and worker.is_alive())
                if age >= scrape_threshold and (job.get('status') != 'running' or not worker_alive):
                    worker_status = 'alive' if worker_alive else 'missing'
                    recovery_status = 'recoverable'
                    if worker_status == 'missing' and not resume_on_restart and mark_partial_if_worker_missing:
                        recovery_status = 'partial'
                    create_or_update_recovery_item(
                        'scrape_job', str(job['id']),
                        entity_name=job.get('job_name') or f"Scrape Job #{job['id']}",
                        current_status=job.get('status'),
                        worker_status=worker_status,
                        problem_type='stuck_scrape_job', severity='high' if job.get('status') == 'running' else 'medium',
                        recovery_status=recovery_status, last_activity_at=job.get('dibuat'), heartbeat_at=job.get('selesai'),
                        note=f"processed={job.get('processed_keywords')}/{job.get('total_keywords')}"
                    )
                    created['scrape_job'] += 1
        if 'assignment' in watch_entities:
            conn = get_conn()
            rows = conn.execute(
                "SELECT * FROM group_assignment WHERE status IN ('assigned','retry_wait','reassign_pending','failed') ORDER BY id DESC LIMIT %s",
                (limit * 2,),
            ).fetchall()
            conn.close()
            for row in rows:
                age = _minutes_since(row['updated_at'] if row['updated_at'] else row['created_at']) or 0
                if age < assign_threshold:
                    continue
                severity = 'high' if row['status'] == 'failed' else 'medium'
                create_or_update_recovery_item(
                    'assignment', str(row['id']),
                    entity_name=f"Assignment #{row['id']}", current_status=row['status'], worker_status='missing',
                    problem_type='stuck_assignment', severity=severity, recovery_status='recoverable',
                    last_activity_at=row['updated_at'] or row['created_at'], note=row['failure_reason']
                )
                created['assignment'] += 1
        if 'campaign' in watch_entities:
            conn = get_conn()
            rows = conn.execute(
                """
                SELECT c.*,
                    (SELECT MAX(ct2.updated_at) FROM campaign_target ct2 WHERE ct2.campaign_id=c.id) AS latest_target_update
                FROM campaign c
                WHERE c.status IN ('queued','running','paused')
                ORDER BY c.id DESC
                LIMIT %s
                """,
                (limit * 2,),
            ).fetchall()
            conn.close()
            for row in rows:
                ref = row['latest_target_update'] or row['started_at'] or row['created_at']
                age = _minutes_since(ref) or 0
                if age < campaign_threshold:
                    continue
                worker_status = 'alive' if _clients else 'missing'
                create_or_update_recovery_item(
                    'campaign', str(row['id']),
                    entity_name=row['name'], current_status=row['status'], worker_status=worker_status,
                    problem_type='stuck_campaign', severity='high' if row['status'] == 'running' else 'medium',
                    recovery_status='recoverable', last_activity_at=ref,
                    note='Tidak ada kemajuan queue dalam ambang waktu recovery'
                )
                created['campaign'] += 1

        if 'campaign' in watch_entities:
            try:
                conn_bg = get_conn()
                stuck_grup = conn_bg.execute(
                    """
                    SELECT g.id, g.nama, g.broadcast_status,
                           g.broadcast_hold_reason, g.broadcast_ready_at,
                           g.diupdate
                    FROM grup g
                    WHERE g.assignment_status = 'managed'
                      AND g.status = 'active'
                      AND g.broadcast_status IN ('queued', 'hold')
                      AND g.broadcast_hold_reason NOT IN (
                          'sender_daily_limit', 'daily_limit_exhausted',
                          'flood_wait', 'join_floodwait',
                          'approval_pending', 'new_assignment_wait',
                          'stabilization_wait', 'recovered_assignment_wait',
                          'sender_diblokir_spambot'
                      )
                      AND (
                          g.broadcast_ready_at IS NULL
                          OR g.broadcast_ready_at <= TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')
                      )
                      AND g.diupdate IS NOT NULL
                      AND g.diupdate <= TO_CHAR(
                          NOW() - INTERVAL '60 minutes',
                          'YYYY-MM-DD HH24:MI:SS'
                      )
                    LIMIT %s
                    """,
                    (limit,)
                ).fetchall()
                conn_bg.close()

                for row in stuck_grup:
                    create_or_update_recovery_item(
                        'grup_broadcast', str(row['id']),
                        entity_name=row['nama'] or str(row['id']),
                        current_status=str(row['broadcast_status']),
                        worker_status='degraded',
                        problem_type='stuck_broadcast_queued',
                        severity='medium',
                        recovery_status='recoverable',
                        last_activity_at=row['diupdate'],
                        note=f"broadcast_hold_reason: {row['broadcast_hold_reason'] or '-'}"
                    )
                    created['campaign'] += 1
            except Exception as _bg_exc:
                print(f'[RecoveryScan] Gagal scan grup broadcast stuck: {_bg_exc}')

        total = sum(created.values())
        result = {'created': created, 'total': total, 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
        record_stage_result('recovery_scan', matched_rules, True, result)
        if total:
            _log('info', 'orchestrator', 'recovery_scan', f'Recovery scan menemukan {total} item', entity_type='recovery_item', entity_id='bulk', result='success', payload=json.dumps(created))
        return result
    except Exception:
        record_stage_result('recovery_scan', matched_rules, False, {'created': created, 'total': sum(created.values())})
        raise


def execute_recovery_safe(limit: int | None = None) -> dict[str, Any]:
    plan = resolve_stage_rules('recovery_execute')
    matched_rules = plan['matched_rules']
    if not plan['enabled']:
        return {
            'skipped': True,
            'reason': 'Tidak ada rule recovery_execute yang aktif/match',
            'rules': [r['id'] for r in plan['all_rules']],
            'context': plan['context'],
        }
    action = plan['effective_action']
    scope = plan['effective_scope']
    limit = limit or int(action.get('limit') or _setting_int('orchestrator_recovery_batch', 25))
    max_failed_targets_to_requeue = int(action.get('max_failed_targets_to_requeue') or 50)
    max_recovery_attempts = int(action.get('max_recovery_attempts') or 5)
    assignment_delay_minutes = _rule_int(action, 'assignment_delay_minutes', 'assignment_broadcast_delay_minutes', 2)
    resume_on_restart = _setting_bool('recovery_resume_on_restart', True)
    mark_partial_if_worker_missing = _setting_bool('recovery_mark_partial_if_worker_missing', True)
    allowed_entity_types = set(scope.get('entity_types') or ['scrape_job', 'assignment', 'campaign', 'grup_broadcast'])
    items, _ = get_recovery_items(status='recoverable', page=1, page_size=limit)
    recovered = 0
    partial = 0
    processed = 0
    try:
        for item in items:
            entity_type = item['entity_type']
            entity_id = str(item['entity_id'])
            if entity_type not in allowed_entity_types:
                continue
            if int(item.get('recovery_attempt_count') or 0) >= max_recovery_attempts:
                create_or_update_recovery_item(entity_type, entity_id, recovery_status='ignored', last_recovery_result='max_attempts_reached')
                partial += 1
                processed += 1
                continue
            processed += 1
            try:
                if entity_type == 'scrape_job':
                    worker_missing = str(item.get('worker_status') or '') == 'missing'
                    if worker_missing and not resume_on_restart:
                        create_or_update_recovery_item('scrape_job', entity_id, recovery_status='partial', last_recovery_at=_now(), last_recovery_result='resume_on_restart_disabled', recovery_attempt_count=int(item.get('recovery_attempt_count') or 0) + 1)
                        partial += 1
                        continue
                    job_id = int(entity_id)
                    job = next((j for j in get_scrape_jobs(limit=500) if int(j['id']) == job_id), None)
                    if job and job.get('status') in {'paused', 'failed', 'queued'}:
                        control_scrape_job(job_id, 'resume')
                    elif job and job.get('status') == 'running':
                        set_scrape_job_status(job_id, 'queued')
                        control_scrape_job(job_id, 'resume')
                    create_or_update_recovery_item('scrape_job', entity_id, recovery_status='recovered', last_recovery_at=_now(), last_recovery_result='scrape_resumed', recovery_attempt_count=int(item.get('recovery_attempt_count') or 0) + 1)
                    recovered += 1
                elif entity_type == 'assignment':
                    row = get_assignment(int(entity_id))
                    if not row:
                        create_or_update_recovery_item('assignment', entity_id, recovery_status='ignored', last_recovery_result='assignment_missing')
                        partial += 1
                        continue
                    best, candidates = _choose_candidate(int(row['group_id']))
                    if not best:
                        create_or_update_recovery_item('assignment', entity_id, recovery_status='partial', last_recovery_at=_now(), last_recovery_result='no_candidate')
                        partial += 1
                        continue
                    update_assignment(int(entity_id), assigned_account_id=str(best['account_id']), status='assigned', reassign_count=int((dict(row).get('reassign_count') if not isinstance(row, dict) else row.get('reassign_count')) or 0) + 1, failure_reason=None, last_attempt_at=_now(), assign_reason='recovered_by_orchestrator', assign_score_snapshot=json.dumps(candidates[:5], ensure_ascii=False))
                    _set_group_state(int(row['group_id']), assignment_status='assigned', owner_phone=str(best['account_id']), broadcast_status='stabilization_wait', broadcast_hold_reason='recovered_assignment_wait', broadcast_ready_at=_now_plus(minutes=assignment_delay_minutes))
                    create_or_update_recovery_item('assignment', entity_id, recovery_status='recovered', last_recovery_at=_now(), last_recovery_result='assignment_reassigned', recovery_attempt_count=int(item.get('recovery_attempt_count') or 0) + 1)
                    recovered += 1
                elif entity_type == 'campaign':
                    if mark_partial_if_worker_missing and not _clients:
                        create_or_update_recovery_item('campaign', entity_id, recovery_status='partial', last_recovery_at=_now(), last_recovery_result='sender_worker_missing', recovery_attempt_count=int(item.get('recovery_attempt_count') or 0) + 1)
                        partial += 1
                        continue
                    campaign = get_campaign(int(entity_id))
                    if not campaign:
                        create_or_update_recovery_item('campaign', entity_id, recovery_status='ignored', last_recovery_result='campaign_missing')
                        partial += 1
                        continue
                    conn = get_conn()
                    failed_targets = conn.execute(
                        "SELECT id FROM campaign_target WHERE campaign_id=%s AND status='failed' AND COALESCE(last_outcome_code,'') IN ('sender_missing') AND (next_attempt_at IS NULL OR next_attempt_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')) LIMIT %s",
                        (int(entity_id), max_failed_targets_to_requeue),
                    ).fetchall()
                    conn.close()
                    for target in failed_targets:
                        update_queue_target(int(target['id']), status='queued', failure_reason=None, hold_reason=None, next_attempt_at=None, finalized_at=None)
                    update_campaign(int(entity_id), status='queued', session_status='queued')
                    _refresh_campaign_counts(int(entity_id))
                    create_or_update_recovery_item('campaign', entity_id, recovery_status='recovered', last_recovery_at=_now(), last_recovery_result='targets_requeued', recovery_attempt_count=int(item.get('recovery_attempt_count') or 0) + 1)
                    recovered += 1
                elif entity_type == 'grup_broadcast':
                    try:
                        grup_id = int(entity_id)
                        conn_gb = get_conn()
                        row_gb = conn_gb.execute(
                            """SELECT id, nama, broadcast_status, assignment_status
                               FROM grup WHERE id=%s AND status='active'
                               AND assignment_status='managed'""",
                            (grup_id,)
                        ).fetchone()
                        conn_gb.close()

                        if not row_gb:
                            create_or_update_recovery_item(
                                'grup_broadcast', entity_id,
                                recovery_status='ignored',
                                last_recovery_result='grup_tidak_ditemukan_atau_bukan_managed'
                            )
                            partial += 1
                            continue

                        _set_group_state(
                            grup_id,
                            broadcast_status='broadcast_eligible',
                            broadcast_hold_reason=None,
                            broadcast_ready_at=None,
                        )
                        create_or_update_recovery_item(
                            'grup_broadcast', entity_id,
                            recovery_status='recovered',
                            last_recovery_at=_now(),
                            last_recovery_result='broadcast_status_direset_ke_eligible',
                            recovery_attempt_count=int(
                                item.get('recovery_attempt_count') or 0
                            ) + 1
                        )
                        recovered += 1
                        print(f"[Recovery] Grup '{row_gb['nama']}' broadcast direset ke eligible")
                    except Exception as _gb_exc:
                        create_or_update_recovery_item(
                            'grup_broadcast', entity_id,
                            recovery_status='partial',
                            last_recovery_at=_now(),
                            last_recovery_result=str(_gb_exc)[:200],
                            recovery_attempt_count=int(
                                item.get('recovery_attempt_count') or 0
                            ) + 1
                        )
                        partial += 1
                else:
                    create_or_update_recovery_item(entity_type, entity_id, recovery_status='ignored', last_recovery_result='unsupported_entity')
                    partial += 1
            except Exception as exc:
                create_or_update_recovery_item(entity_type, entity_id, recovery_status='partial', last_recovery_at=_now(), last_recovery_result=str(exc)[:200], recovery_attempt_count=int(item.get('recovery_attempt_count') or 0) + 1)
                partial += 1
        result = {'recovered': recovered, 'partial': partial, 'processed': processed, 'rule_ids': [r['id'] for r in matched_rules], 'context': plan['context']}
        record_stage_result('recovery_execute', matched_rules, True, result)
        if recovered or partial:
            _log('info', 'orchestrator', 'recovery_execute', f'Recovery execute: recovered={recovered}, partial={partial}', entity_type='recovery_item', entity_id='bulk', result='success')
        return result
    except Exception:
        record_stage_result('recovery_execute', matched_rules, False, {'recovered': recovered, 'partial': partial, 'processed': processed})
        raise


def run_full_cycle(trigger: str = 'manual') -> dict[str, Any]:
    if not _LOCK.acquire(blocking=False):
        return {
            'ok': False,
            'busy': True,
            'message': 'Orchestrator sedang berjalan',
            'state': get_orchestrator_status(),
        }
    started = _now()
    _log('info', 'cycle', 'cycle_started', f'Orchestrator mulai siklus trigger={trigger}', entity_type='orchestrator', entity_id=started, result='running')
    _STATE['current_stage'] = 'starting'
    _STATE['last_started_at'] = started
    _STATE['last_trigger'] = trigger
    stage_results: dict[str, Any] = {}
    stage_errors: dict[str, str] = {}
    try:
        # Auto-reset target queued yang next_attempt_at sudah lewat setiap siklus
        try:
            conn_ar = get_conn()
            n_reset = conn_ar.execute(
                """UPDATE campaign_target
                   SET next_attempt_at=NULL, hold_reason=NULL
                   WHERE status='queued'
                     AND next_attempt_at IS NOT NULL
                     AND next_attempt_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')"""
            ).rowcount
            conn_ar.commit()
            conn_ar.close()
            if n_reset > 0:
                print(f'[Orchestrator] Auto-reset {n_reset} target queued yang sudah waktunya')
        except Exception as _ar_exc:
            print(f'[Orchestrator] Gagal auto-reset queued: {_ar_exc}')

        try:
            conn_sr = get_conn()
            n_sending = conn_sr.execute(
                """UPDATE campaign_target
                   SET status='queued',
                       hold_reason='reset_dari_sending_stuck',
                       next_attempt_at=NULL,
                       sender_account_id=NULL
                   WHERE status='sending'
                     AND last_attempt_at IS NOT NULL
                     AND last_attempt_at <= TO_CHAR(
                         NOW() - INTERVAL '10 minutes',
                         'YYYY-MM-DD HH24:MI:SS'
                     )"""
            ).rowcount
            conn_sr.commit()
            conn_sr.close()
            if n_sending > 0:
                print(f'[Orchestrator] Reset {n_sending} target stuck di sending > 10 menit')
        except Exception as _sr_exc:
            print(f'[Orchestrator] Gagal reset sending stuck: {_sr_exc}')

        try:
            conn_sd = get_conn()
            n_spambot = conn_sd.execute(
                """UPDATE campaign_target
                   SET status='queued',
                       hold_reason=NULL,
                       sender_account_id=NULL,
                       next_attempt_at=NULL
                   WHERE status='queued'
                     AND hold_reason='sender_diblokir_spambot'
                     AND last_attempt_at <= TO_CHAR(
                         NOW() - INTERVAL '10 minutes',
                         'YYYY-MM-DD HH24:MI:SS'
                     )"""
            ).rowcount
            conn_sd.commit()
            conn_sd.close()
            if n_spambot > 0:
                print(f'[Orchestrator] Reset {n_spambot} target dari akun yang diblokir SpamBot')
        except Exception as _sd_exc:
            print(f'[Orchestrator] Gagal reset spambot target: {_sd_exc}')

        try:
            _cleanup_banned_accounts(max_reassign=20)
        except Exception as _cb_exc:
            print(f'[Orchestrator] Gagal cleanup banned: {_cb_exc}')

        try:
            _heal_abandoned_groups(limit=50)
        except Exception as _sh_exc:
            print(f'[Orchestrator] Gagal self-heal: {_sh_exc}')

        stages = [
            ('import',          'auto_import_enabled',   stage_import),
            ('permission',      'auto_permission_enabled', stage_permission),
            ('sync_join',       'auto_join_enabled',     stage_sync_join),
            ('auto_join',       'auto_join_enabled',     stage_auto_join),
            ('assignment',      'auto_assign_enabled',   stage_assignment),
            ('campaign_prepare','auto_campaign_enabled', stage_campaign_prepare),
            ('delivery',        'auto_campaign_enabled', stage_delivery),
            ('recovery_scan',   'auto_recovery_enabled', scan_recovery_items),
            ('recovery_execute','auto_recovery_enabled', execute_recovery_safe),
        ]
        for stage_name, setting_key, func in stages:
            _STATE['current_stage'] = stage_name
            if not _automation_allowed(setting_key, 0 if setting_key != 'auto_recovery_enabled' else 1):
                stage_results[stage_name] = {'skipped': True, 'reason': f'{setting_key}=0 or maintenance/pause aktif'}
                continue
            try:
                stage_results[stage_name] = func()
            except Exception as exc:
                stage_errors[stage_name] = str(exc)
                stage_results[stage_name] = {'ok': False, 'error': str(exc)}
                _STATE['failure_count'] += 1
                _log('error', 'orchestrator', 'stage_failed', f'Stage {stage_name} gagal: {exc}', entity_type='orchestrator_stage', entity_id=stage_name, result='failed')
                continue
        _STATE['run_count'] += 1
        _STATE['last_result'] = {'stages': stage_results, 'stage_errors': stage_errors}
        _STATE['last_finished_at'] = _now()
        _STATE['current_stage'] = None
        _log('info', 'cycle', 'cycle_finished', f'Orchestrator selesai: ok={not bool(stage_errors)}', entity_type='orchestrator', entity_id=_STATE['last_finished_at'], result='success' if not stage_errors else 'partial')
        return {'ok': not bool(stage_errors), 'started_at': started, 'finished_at': _STATE['last_finished_at'], 'trigger': trigger, 'stages': stage_results, 'stage_errors': stage_errors}
    finally:
        _STATE['current_stage'] = None
        _LOCK.release()


def get_orchestrator_status() -> dict[str, Any]:
    state = dict(_STATE)
    state['flow'] = FLOW
    state['online_accounts'] = list(_clients.keys())
    state['settings'] = {
        'auto_import_enabled': _automation_allowed('auto_import_enabled', 0),
        'auto_permission_enabled': _automation_allowed('auto_permission_enabled', 0),
        'auto_assign_enabled': _automation_allowed('auto_assign_enabled', 0),
        'auto_join_enabled': _automation_allowed('auto_join_enabled', 0),
        'auto_campaign_enabled': _automation_allowed('auto_campaign_enabled', 0),
        'auto_recovery_enabled': _automation_allowed('auto_recovery_enabled', 1),
        'maintenance_mode': bool(int(get_setting('maintenance_mode', 0) or 0)),
        'pause_all_automation': bool(int(get_setting('pause_all_automation', 0) or 0)),
        'interval_seconds': _setting_int('orchestrator_interval_seconds', 30),
        'assignment_broadcast_delay_minutes': _setting_int('assignment_broadcast_delay_minutes', 120),
        'campaign_session_target_limit': _setting_int('campaign_session_target_limit', 50),
        'campaign_session_per_sender_limit': _setting_int('campaign_session_per_sender_limit', 5),
        'campaign_allow_mid_session_enqueue': _setting_bool('campaign_allow_mid_session_enqueue', False),
        'campaign_group_cooldown_hours': _setting_int('campaign_group_cooldown_hours', 72),
    }
    state['rule_overview'] = get_rule_overview()
    join_stats = get_auto_join_summary()
    state['auto_join_stats'] = {
        'joined_today': int(join_stats.get('joined_today') or 0),
        'failed_today': int(join_stats.get('failed_today') or 0),
        'waiting': int(join_stats.get('waiting') or 0),
        'last_run_at': _STATE.get('auto_join_stats', {}).get('last_run_at'),
    }
    sessions = _session_candidates()
    state['campaign_sessions'] = sessions[:5]
    conn = get_conn()
    state['group_state_counts'] = {
        'stabilization_wait': int((conn.execute("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='stabilization_wait'").fetchone()[0]) or 0),
        'broadcast_eligible': int((conn.execute("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='broadcast_eligible'").fetchone()[0]) or 0),
        'queued': int((conn.execute("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='queued'").fetchone()[0]) or 0),
        'cooldown': int((conn.execute("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='cooldown'").fetchone()[0]) or 0),
        'hold_waiting_response': int((conn.execute("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='hold_waiting_response'").fetchone()[0]) or 0),
        'hold_inactive': int((conn.execute("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='hold_inactive'").fetchone()[0]) or 0),
    }
    conn.close()
    return state


def start_orchestrator_worker() -> bool:
    global _WORKER_THREAD
    if _WORKER_THREAD and _WORKER_THREAD.is_alive():
        return False

    def _loop_worker():
        _STATE['worker_running'] = True
        time.sleep(8)
        while True:
            try:
                run_full_cycle(trigger='background')
            except Exception as exc:
                _STATE['failure_count'] += 1
                _log('error', 'orchestrator', 'worker_loop_error', f'Orchestrator worker loop error: {exc}', entity_type='orchestrator', entity_id='global', result='failed')
            time.sleep(max(5, _setting_int('orchestrator_interval_seconds', 30)))

    _WORKER_THREAD = threading.Thread(target=_loop_worker, daemon=True, name='orchestrator_full')
    _WORKER_THREAD.start()
    return True