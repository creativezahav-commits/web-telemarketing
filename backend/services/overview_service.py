from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
import os

import config
from services.account_manager import _clients
from core.warming import get_info_warming
from utils.database import get_conn, PG_DB
from utils.settings_manager import get
from utils.storage_db import get_draft_aktif
from core.warming import get_daily_capacity


def _count(sql: str, params: tuple[Any, ...] = ()) -> int:
    conn = get_conn()
    row = conn.execute(sql, params).fetchone()
    conn.close()
    return int((row[0] if row else 0) or 0)


def get_overview_summary() -> dict[str, Any]:
    today = datetime.now().strftime('%Y-%m-%d') + '%'
    return {
        'total_accounts': _count('SELECT COUNT(*) FROM akun'),
        'online_accounts': len(_clients),
        'running_scrape_jobs': _count("SELECT COUNT(*) FROM scrape_job WHERE status IN ('queued','running','paused')"),
        'results_ready_import': _count("SELECT COUNT(*) FROM scrape_result WHERE imported=0 AND recommended=1 AND already_in_db=0"),
        'groups_ready_assign': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(assignment_status,'ready_assign')='ready_assign' AND status='active'"),
        'managed_groups': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(assignment_status,'')='managed'"),
        'broadcast_eligible_groups': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='broadcast_eligible'"),
        'groups_stabilizing': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='stabilization_wait'"),
        'groups_cooldown': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='cooldown'"),
        'failed_targets_today': _count("SELECT COUNT(*) FROM riwayat WHERE status IN ('gagal','send_failed','join_failed') AND waktu LIKE %s", (today,)),
        'recovery_needed': _count("SELECT COUNT(*) FROM recovery_item WHERE recovery_status IN ('recovery_needed','recoverable')"),
        'active_campaigns': _count("SELECT COUNT(*) FROM campaign WHERE status IN ('queued','running','paused')"),
    }


def _automation_state() -> str:
    if bool(get('maintenance_mode', 0)):
        return 'maintenance'
    if bool(get('pause_all_automation', 0)):
        return 'paused'
    return 'running'




def _setting_bool(key: str, default: bool = False) -> bool:
    value = get(key, 1 if default else 0)
    if isinstance(value, bool):
        return value
    try:
        return bool(int(value or 0))
    except Exception:
        return str(value or '').strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _setting_int(key: str, default: int = 0) -> int:
    try:
        return int(get(key, default) or 0)
    except Exception:
        return int(default)

def get_overview_health() -> dict[str, Any]:
    db_exists = True  # PostgreSQL - selalu dianggap ada jika server berjalan
    return {
        'backend_status': 'healthy',
        'database_status': 'ready' if db_exists else 'missing',
        'automation_status': _automation_state(),
        'worker_health': {
            'telegram_clients_online': len(_clients),
            'recovery_engine': 'enabled' if bool(get('auto_recovery_enabled', 1)) else 'paused',
            'scraper_engine': 'enabled',
            'campaign_engine': 'enabled' if bool(get('auto_campaign_enabled', 0)) else 'paused',
        },
        'queue_depth': {
            'scrape_jobs': _count("SELECT COUNT(*) FROM scrape_job WHERE status='queued'"),
            'manual_queue': _count("SELECT COUNT(*) FROM antrian WHERE status='menunggu'"),
            'campaign_targets': _count("SELECT COUNT(*) FROM campaign_target WHERE status IN ('eligible','queued','sending')"),
            'queued_sessions': _count("SELECT COUNT(*) FROM campaign WHERE status='queued'"),
        },
        'error_rate_24h': _calc_error_rate_24h(),
        'last_refresh_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'db_file': f'postgresql://{PG_DB}',
        'frontend_dir': config.get_frontend_dir(),
    }


def _calc_error_rate_24h() -> dict[str, Any]:
    conn = get_conn()
    since = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('berhasil','send_success','join','join_success') THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN status IN ('gagal','send_failed','join_failed') THEN 1 ELSE 0 END) AS fail_count,
            COUNT(*) AS total_count
        FROM riwayat
        WHERE waktu >= %s
        """,
        (since,),
    ).fetchone()
    conn.close()
    total = int((row['total_count'] if row else 0) or 0)
    fail_count = int((row['fail_count'] if row else 0) or 0)
    return {
        'fail_count': fail_count,
        'total_count': total,
        'rate': round((fail_count / total) * 100, 2) if total else 0.0,
    }


def get_active_processes() -> list[dict[str, Any]]:
    conn = get_conn()
    scrape_jobs = conn.execute(
        "SELECT id, phone, status, processed_keywords, total_keywords, dibuat FROM scrape_job WHERE status IN ('queued','running','paused') ORDER BY id DESC LIMIT 10"
    ).fetchall()
    campaigns = conn.execute(
        "SELECT id, name, status, total_targets, sent_count, created_at FROM campaign WHERE status IN ('queued','running','paused') ORDER BY id DESC LIMIT 10"
    ).fetchall()
    conn.close()
    items: list[dict[str, Any]] = []
    for row in scrape_jobs:
        total = int(row['total_keywords'] or 0)
        done = int(row['processed_keywords'] or 0)
        items.append({
            'process_id': row['id'],
            'process_type': 'scrape_job',
            'name': f"Scrape #{row['id']}",
            'status': row['status'],
            'progress_percent': round((done / total) * 100, 2) if total else 0.0,
            'related_account': row['phone'],
            'last_activity_at': row['dibuat'],
        })
    for row in campaigns:
        total = int(row['total_targets'] or 0)
        sent = int(row['sent_count'] or 0)
        items.append({
            'process_id': row['id'],
            'process_type': 'campaign',
            'name': row['name'],
            'status': row['status'],
            'progress_percent': round((sent / total) * 100, 2) if total else 0.0,
            'related_account': row.get('sender_pool') if hasattr(row, 'keys') and 'sender_pool' in row.keys() else None,
            'last_activity_at': row['created_at'],
        })
    return items


def get_attention_items() -> dict[str, list[dict[str, Any]]]:
    conn = get_conn()
    limited = conn.execute("SELECT phone, nama, status, score FROM akun WHERE status IN ('limited','banned','disabled') ORDER BY score ASC, phone ASC LIMIT 10").fetchall()
    unassigned = conn.execute("SELECT id, nama, username FROM grup WHERE COALESCE(assignment_status,'ready_assign')='ready_assign' AND status='active' ORDER BY score DESC, nama ASC LIMIT 10").fetchall()
    blocked = conn.execute("SELECT id, nama, username, broadcast_status FROM grup WHERE COALESCE(broadcast_status,'')='broadcast_blocked' ORDER BY diupdate DESC LIMIT 10").fetchall()
    recovery = conn.execute("SELECT id, entity_type, entity_name, problem_type FROM recovery_item WHERE recovery_status IN ('recovery_needed','recoverable') ORDER BY id DESC LIMIT 10").fetchall()
    conn.close()
    return {
        'limited_accounts': [dict(r) for r in limited],
        'expired_permissions': [],
        'unassigned_groups': [dict(r) for r in unassigned],
        'failed_assignments': [dict(r) for r in recovery if r['entity_type'] == 'assignment'],
        'blocked_targets': [dict(r) for r in blocked],
    }


def get_trends(range_key: str = '7d') -> dict[str, Any]:
    days = 7
    if range_key == 'today':
        days = 1
    elif range_key == '30d':
        days = 30

    conn = get_conn()
    rows = conn.execute(
        """
        SELECT LEFT(waktu, 10) AS hari,
               SUM(CASE WHEN status IN ('berhasil','send_success') THEN 1 ELSE 0 END) AS berhasil,
               SUM(CASE WHEN status IN ('gagal','send_failed') THEN 1 ELSE 0 END) AS gagal,
               COUNT(*) AS total
        FROM riwayat
        WHERE waktu >= NOW() + (%s || ' day')::INTERVAL
        GROUP BY LEFT(waktu, 10)
        ORDER BY hari ASC
        """,
        (f'-{days} day',),
    ).fetchall()
    scrape_rows = conn.execute(
        """
        SELECT LEFT(dibuat, 10) AS hari, COUNT(*) AS total
        FROM scrape_job
        WHERE dibuat >= NOW() + (%s || ' day')::INTERVAL
        GROUP BY LEFT(dibuat, 10)
        ORDER BY hari ASC
        """,
        (f'-{days} day',),
    ).fetchall()
    import_rows = conn.execute(
        """
        SELECT LEFT(ditemukan, 10) AS hari, COUNT(*) AS total
        FROM scrape_result
        WHERE imported=1 AND ditemukan >= NOW() + (%s || ' day')::INTERVAL
        GROUP BY LEFT(ditemukan, 10)
        ORDER BY hari ASC
        """,
        (f'-{days} day',),
    ).fetchall()
    assignment_rows = conn.execute(
        """
        SELECT LEFT(created_at, 10) AS hari, COUNT(*) AS total
        FROM group_assignment
        WHERE created_at >= NOW() + (%s || ' day')::INTERVAL
        GROUP BY LEFT(created_at, 10)
        ORDER BY hari ASC
        """,
        (f'-{days} day',),
    ).fetchall()
    conn.close()

    return {
        'scrape_trend': [dict(r) for r in scrape_rows],
        'delivery_trend': [dict(r) for r in rows],
        'import_trend': [dict(r) for r in import_rows],
        'assignment_trend': [dict(r) for r in assignment_rows],
    }



def get_pipeline_flow() -> dict[str, Any]:
    conn = get_conn()
    stages = {
        'scrape_results_ready_import': _count("SELECT COUNT(*) FROM scrape_result WHERE imported=0 AND already_in_db=0"),
        'groups_missing_permission': _count("SELECT COUNT(*) FROM grup WHERE status='active' AND COALESCE(permission_status,'unknown') IN ('unknown','pending')"),
        'groups_ready_assign': _count("SELECT COUNT(*) FROM grup WHERE status='active' AND COALESCE(permission_status,'unknown')='valid' AND COALESCE(assignment_status,'ready_assign')='ready_assign'"),
        'groups_managed': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(assignment_status,'')='managed'"),
        'groups_broadcast_eligible': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='broadcast_eligible'"),
        'groups_stabilization_wait': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='stabilization_wait'"),
        'groups_cooldown': _count("SELECT COUNT(*) FROM grup WHERE COALESCE(broadcast_status,'')='cooldown'"),
        'campaign_targets_waiting': _count("SELECT COUNT(*) FROM campaign_target WHERE status IN ('eligible','queued','sending')"),
        'recovery_needed': _count("SELECT COUNT(*) FROM recovery_item WHERE recovery_status IN ('recovery_needed','recoverable')"),
    }
    toggles = {
        'auto_import_enabled': bool(get('auto_import_enabled', 0)),
        'auto_assign_enabled': bool(get('auto_assign_enabled', 0)),
        'auto_campaign_enabled': bool(get('auto_campaign_enabled', 0)),
        'auto_recovery_enabled': bool(get('auto_recovery_enabled', 1)),
        'maintenance_mode': bool(get('maintenance_mode', 0)),
        'pause_all_automation': bool(get('pause_all_automation', 0)),
    }
    conn.close()
    return {
        'state': _automation_state(),
        'toggles': toggles,
        'stages': stages,
        'flow_notes': [
            'scrape_result -> import -> permission -> assignment -> stabilization -> campaign session queue -> delivery -> cooldown/hold -> recovery',
            'Jika maintenance_mode atau pause_all_automation aktif, worker background tidak akan melanjutkan stage berikutnya.',
            'groups_missing_permission tinggi berarti Auto Permission belum mengejar backlog.',
            'groups_ready_assign tinggi berarti permission sudah valid tetapi owner akun belum dipasang.',
            'groups_stabilization_wait menunjukkan grup baru assigned yang sedang ditahan sebelum boleh dikirim.',
            'groups_broadcast_eligible tinggi berarti grup siap masuk sesi broadcast berikutnya.',
            'groups_cooldown menunjukkan grup yang baru saja dikirimi dan sedang menunggu jeda aman.',
        ],
    }



STATE_LANE_META = {
    'stabilization': {
        'label': 'Stabilization',
        'icon': '⏳',
        'description': 'Grup baru di-assign dan sedang masa tunggu sebelum masuk sesi broadcast.',
    },
    'eligible': {
        'label': 'Eligible',
        'icon': '✅',
        'description': 'Grup siap masuk sesi broadcast berikutnya.',
    },
    'queued': {
        'label': 'Queued',
        'icon': '📥',
        'description': 'Sudah masuk queue/sesi dan menunggu atau sedang diproses sender.',
    },
    'cooldown': {
        'label': 'Cooldown',
        'icon': '🧊',
        'description': 'Baru selesai dikirim dan sedang jeda aman sebelum bisa diproses lagi.',
    },
    'hold': {
        'label': 'Hold',
        'icon': '🛑',
        'description': 'Grup ditahan oleh guard, izin, assignment, atau alasan operasional lain.',
    },
    'failed': {
        'label': 'Failed',
        'icon': '❌',
        'description': 'Ada kegagalan assignment atau delivery yang belum pulih.',
    },
    'recovery': {
        'label': 'Recovery',
        'icon': '🛠️',
        'description': 'Sudah terdeteksi butuh tindakan recovery atau pemulihan lanjutan.',
    },
}


def _to_dt(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(raw[:19] if 'T' in raw or len(raw) >= 19 else raw, fmt)
        except Exception:
            continue
    return None


def _state_sort_ts(item: dict[str, Any]) -> datetime:
    for key in ('recovery_updated_at', 'target_updated_at', 'broadcast_ready_at', 'diupdate', 'last_chat', 'last_kirim'):
        dt = _to_dt(item.get(key))
        if dt:
            return dt
    return datetime.min


def _resolve_group_lane(group_row: dict[str, Any], target_row: dict[str, Any] | None, recovery_row: dict[str, Any] | None) -> tuple[str, str]:
    status = str(group_row.get('status') or 'active')
    permission_status = str(group_row.get('permission_status') or 'unknown')
    assignment_status = str(group_row.get('assignment_status') or 'ready_assign')
    broadcast_status = str(group_row.get('broadcast_status') or 'hold')
    guard_status = str(group_row.get('send_guard_status') or 'unknown')
    hold_reason = str(group_row.get('broadcast_hold_reason') or '').strip()
    guard_reason = str(group_row.get('send_guard_reason') or '').strip()
    target_status = str((target_row or {}).get('status') or '')

    if recovery_row:
        reason = str(recovery_row.get('problem_type') or recovery_row.get('note') or 'Perlu recovery lanjutan')
        return 'recovery', reason

    if assignment_status == 'failed' or broadcast_status == 'failed' or target_status == 'failed':
        reason = str((target_row or {}).get('failure_reason') or hold_reason or guard_reason or 'Assignment atau delivery gagal')
        return 'failed', reason

    if broadcast_status == 'queued' or target_status in {'eligible', 'queued', 'sending'}:
        reason = str((target_row or {}).get('campaign_name') or 'Sudah masuk queue sesi broadcast')
        return 'queued', reason

    if broadcast_status == 'cooldown':
        return 'cooldown', hold_reason or 'Menunggu cooldown selesai'

    if broadcast_status == 'stabilization_wait':
        return 'stabilization', hold_reason or 'Menunggu masa stabilisasi selesai'

    if broadcast_status == 'broadcast_eligible':
        return 'eligible', 'Siap dipilih ke sesi broadcast berikutnya'

    hold_reason_text = (
        hold_reason
        or guard_reason
        or ('Permission belum valid' if permission_status not in {'valid', 'owned', 'admin', 'partner_approved', 'opt_in'} else '')
        or ('Menunggu assignment owner' if assignment_status in {'ready_assign', 'assigned', 'retry_wait', 'reassign_pending'} else '')
        or ('Grup nonaktif/arsip' if status != 'active' else '')
        or 'Ditahan oleh aturan operasional'
    )
    return 'hold', hold_reason_text


def get_group_state_dashboard(search: str = '', focus_state: str = '', limit_per_state: int = 20, include_archived: bool = False) -> dict[str, Any]:
    search = (search or '').strip().lower()
    focus_state = (focus_state or '').strip().lower()
    limit_per_state = max(1, min(int(limit_per_state or 20), 100))

    conn = get_conn()
    group_rows = conn.execute(
        """
        SELECT id, nama, username, status, permission_status, assignment_status, broadcast_status,
               owner_phone, source_keyword, notes, diupdate, last_chat, last_kirim,
               send_guard_status, send_guard_reason, broadcast_ready_at, broadcast_hold_reason,
               idle_days, label, score
        FROM grup
        WHERE (%s = 1 OR COALESCE(status, 'active') != 'archived')
        ORDER BY COALESCE(diupdate, '1970-01-01 00:00:00')::TIMESTAMP DESC, id DESC
        """,
        (1 if include_archived else 0,),
    ).fetchall()

    target_rows = conn.execute(
        """
        SELECT ct.id, ct.group_id, ct.campaign_id, ct.sender_account_id, ct.status, ct.failure_reason,
               ct.hold_reason, ct.updated_at, ct.created_at, ct.last_outcome_code,
               c.name AS campaign_name, c.status AS campaign_status, c.session_status
        FROM campaign_target ct
        LEFT JOIN campaign c ON c.id = ct.campaign_id
        ORDER BY COALESCE(ct.updated_at, ct.created_at, '1970-01-01 00:00:00')::TIMESTAMP DESC, ct.id DESC
        """
    ).fetchall()

    latest_target_by_group: dict[int, dict[str, Any]] = {}
    target_group_by_id: dict[str, int] = {}
    for row in target_rows:
        data = dict(row)
        gid = int(data.get('group_id') or 0)
        if gid and gid not in latest_target_by_group:
            latest_target_by_group[gid] = data
        tid = data.get('id')
        if tid is not None:
            target_group_by_id[str(tid)] = gid

    assignment_map = {
        str(r['id']): int(r['group_id'])
        for r in conn.execute('SELECT id, group_id FROM group_assignment').fetchall()
    }

    recovery_rows = conn.execute(
        """
        SELECT id, entity_type, entity_id, entity_name, problem_type, recovery_status, note, updated_at, last_recovery_result
        FROM recovery_item
        WHERE recovery_status IN ('recovery_needed', 'recoverable', 'partial')
        ORDER BY COALESCE(updated_at, created_at, '1970-01-01 00:00:00')::TIMESTAMP DESC, id DESC
        """
    ).fetchall()
    conn.close()

    recovery_by_group: dict[int, dict[str, Any]] = {}
    unresolved_campaign_recovery = 0
    for row in recovery_rows:
        rec = dict(row)
        etype = str(rec.get('entity_type') or '')
        entity_id = str(rec.get('entity_id') or '')
        gid = None
        if etype == 'group':
            try:
                gid = int(entity_id)
            except Exception:
                gid = None
        elif etype == 'assignment':
            gid = assignment_map.get(entity_id)
        elif etype == 'campaign_target':
            gid = target_group_by_id.get(entity_id)
        elif etype == 'campaign':
            unresolved_campaign_recovery += 1
        if gid and gid not in recovery_by_group:
            recovery_by_group[gid] = rec

    lanes: dict[str, dict[str, Any]] = {
        key: {
            **meta,
            'key': key,
            'count': 0,
            'items': [],
        }
        for key, meta in STATE_LANE_META.items()
    }

    total_visible = 0
    for row_obj in group_rows:
        row = dict(row_obj)
        if search:
            haystack = ' '.join(
                str(row.get(key) or '')
                for key in ('nama', 'username', 'owner_phone', 'source_keyword', 'notes', 'broadcast_hold_reason', 'send_guard_reason')
            ).lower()
            if search not in haystack:
                continue

        gid = int(row['id'])
        target = latest_target_by_group.get(gid)
        recovery = recovery_by_group.get(gid)
        lane, lane_reason = _resolve_group_lane(row, target, recovery)
        total_visible += 1
        lanes[lane]['count'] += 1
        if focus_state and lane != focus_state:
            continue
        if len(lanes[lane]['items']) >= limit_per_state:
            continue

        target_status = str((target or {}).get('status') or '')
        item = {
            'id': gid,
            'group_name': row.get('nama') or f'Group #{gid}',
            'group_username': row.get('username'),
            'owner_phone': row.get('owner_phone'),
            'permission_status': row.get('permission_status') or 'unknown',
            'assignment_status': row.get('assignment_status') or 'ready_assign',
            'broadcast_status': row.get('broadcast_status') or 'hold',
            'send_guard_status': row.get('send_guard_status') or 'unknown',
            'send_guard_reason': row.get('send_guard_reason'),
            'broadcast_hold_reason': row.get('broadcast_hold_reason'),
            'state': lane,
            'state_reason': lane_reason,
            'last_chat': row.get('last_chat'),
            'last_kirim': row.get('last_kirim'),
            'broadcast_ready_at': row.get('broadcast_ready_at'),
            'idle_days': int(row.get('idle_days') or 0),
            'label': row.get('label') or 'Normal',
            'score': int(row.get('score') or 0),
            'diupdate': row.get('diupdate'),
            'source_keyword': row.get('source_keyword'),
            'target_id': (target or {}).get('id'),
            'target_status': target_status or None,
            'target_sender': (target or {}).get('sender_account_id'),
            'target_failure_reason': (target or {}).get('failure_reason') or (target or {}).get('hold_reason'),
            'target_updated_at': (target or {}).get('updated_at') or (target or {}).get('created_at'),
            'campaign_id': (target or {}).get('campaign_id'),
            'campaign_name': (target or {}).get('campaign_name'),
            'campaign_status': (target or {}).get('campaign_status'),
            'session_status': (target or {}).get('session_status'),
            'recovery_item_id': (recovery or {}).get('id'),
            'recovery_problem_type': (recovery or {}).get('problem_type'),
            'recovery_status': (recovery or {}).get('recovery_status'),
            'recovery_note': (recovery or {}).get('note'),
            'recovery_updated_at': (recovery or {}).get('updated_at'),
        }
        lanes[lane]['items'].append(item)

    for lane in lanes.values():
        lane['items'].sort(key=_state_sort_ts, reverse=True)
        lane['has_more'] = lane['count'] > len(lane['items'])

    pipeline = get_pipeline_flow()
    try:
        from services.orchestrator_service import get_orchestrator_status
        orch = get_orchestrator_status()
    except Exception:
        orch = {'campaign_sessions': [], 'current_stage': None, 'is_running': False, 'group_state_counts': {}}

    active_sessions = []
    for session in orch.get('campaign_sessions') or []:
        if str(session.get('status') or '').lower() in {'queued', 'running', 'paused'} or str(session.get('session_status') or '').lower() in {'queued', 'running', 'paused'}:
            active_sessions.append(session)

    bottlenecks: list[str] = []
    if lanes['recovery']['count'] > 0:
        bottlenecks.append(f"{lanes['recovery']['count']} grup berada di lane recovery dan perlu tindakan lanjutan.")
    if lanes['stabilization']['count'] >= max(10, lanes['eligible']['count'] * 2 if lanes['eligible']['count'] else 10):
        bottlenecks.append('Stabilization lebih tinggi dari eligible — cek delay assignment dan sinkronisasi owner join.')
    if lanes['queued']['count'] > 0 and not active_sessions:
        bottlenecks.append('Ada grup queued tetapi tidak ada sesi broadcast aktif — cek auto campaign atau draft aktif.')
    if lanes['hold']['count'] > max(20, lanes['eligible']['count'] * 2 if lanes['eligible']['count'] else 20):
        bottlenecks.append('Lane hold sangat tinggi — review send guard, permission, atau assignment yang menahan grup.')
    if unresolved_campaign_recovery:
        bottlenecks.append(f'{unresolved_campaign_recovery} recovery level campaign belum bisa dipetakan langsung ke grup tertentu.')

    lane_reason_counts: dict[str, list[dict[str, Any]]] = {}
    for lane_key, lane in lanes.items():
        counter: dict[str, int] = {}
        for item in lane.get('items') or []:
            reason = str(item.get('broadcast_hold_reason') or item.get('target_failure_reason') or item.get('recovery_problem_type') or item.get('state_reason') or 'lainnya').strip()
            counter[reason] = counter.get(reason, 0) + 1
        lane_reason_counts[lane_key] = [
            {'reason': reason, 'count': count}
            for reason, count in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:6]
        ]

    active_draft = get_draft_aktif()
    automation = {
        'auto_import_enabled': _setting_bool('auto_import_enabled', False),
        'auto_assign_enabled': _setting_bool('auto_assign_enabled', False),
        'auto_campaign_enabled': _setting_bool('auto_campaign_enabled', False),
        'auto_recovery_enabled': _setting_bool('auto_recovery_enabled', True),
        'maintenance_mode': _setting_bool('maintenance_mode', False),
        'pause_all_automation': _setting_bool('pause_all_automation', False),
        'broadcast_jadwal_aktif': _setting_bool('broadcast_jadwal_aktif', True),
    }
    state_settings = {
        'stabilization_delay_minutes': _setting_int('assignment_broadcast_delay_minutes', 120),
        'eligible_require_valid_permission': _setting_bool('campaign_valid_permission_required', True),
        'eligible_require_managed': _setting_bool('campaign_managed_required', True),
        'queued_session_target_limit': _setting_int('campaign_session_target_limit', 50),
        'queued_per_sender_limit': _setting_int('campaign_session_per_sender_limit', 5),
        'queued_allow_mid_session_enqueue': _setting_bool('campaign_allow_mid_session_enqueue', False),
        'cooldown_hours': _setting_int('campaign_group_cooldown_hours', 72),
        'hold_skip_inactive_enabled': _setting_bool('campaign_skip_inactive_groups_enabled', True),
        'hold_inactive_threshold_days': _setting_int('campaign_inactive_threshold_days', 14),
        'hold_skip_if_last_chat_is_ours': _setting_bool('campaign_skip_if_last_chat_is_ours', True),
    }
    diagnostics_messages: list[str] = []
    if automation['maintenance_mode']:
        diagnostics_messages.append('Maintenance mode aktif: seluruh otomasi berhenti.')
    if automation['pause_all_automation']:
        diagnostics_messages.append('Pause all automation aktif: orchestrator tidak akan melanjutkan stage berikutnya.')
    if not automation['auto_assign_enabled']:
        diagnostics_messages.append('Auto assign masih nonaktif, sehingga grup valid tidak akan otomatis mendapat owner.')
    if not automation['auto_campaign_enabled']:
        diagnostics_messages.append('Auto campaign masih nonaktif, sehingga grup eligible tidak akan otomatis dibuatkan sesi broadcast.')
    if not automation['broadcast_jadwal_aktif']:
        diagnostics_messages.append('Broadcast jadwal aktif sedang nonaktif, sehingga sesi kirim tidak bergerak otomatis.')
    if not active_draft:
        diagnostics_messages.append('Tidak ada draft aktif; delivery tidak akan mengirim walaupun ada target queued.')
    if lanes['eligible']['count'] == 0 and lanes['stabilization']['count'] > 0:
        diagnostics_messages.append('Belum ada grup eligible karena banyak grup masih berada di stabilization. Periksa delay stabilisasi dan status owner join.')
    if lanes['eligible']['count'] == 0 and lanes['hold']['count'] > 0:
        diagnostics_messages.append('Belum ada grup eligible karena banyak grup tertahan di hold. Periksa permission, send guard, dan owner assignment.')

    summary = {
        'total_visible_groups': total_visible,
        'total_groups': len(group_rows),
        'active_sessions': len(active_sessions),
        'queued_targets': int(pipeline.get('stages', {}).get('campaign_targets_waiting') or 0),
        'recovery_needed': int(pipeline.get('stages', {}).get('recovery_needed') or 0),
        'state_counts': {key: lanes[key]['count'] for key in STATE_LANE_META.keys()},
    }

    return {
        'summary': summary,
        'lanes': lanes,
        'lane_reason_counts': lane_reason_counts,
        'active_sessions': active_sessions[:5],
        'pipeline': pipeline,
        'bottlenecks': bottlenecks,
        'diagnostics': {
            'automation': automation,
            'active_draft': bool(active_draft),
            'active_draft_name': (active_draft or {}).get('nama') if active_draft else None,
            'state_settings': state_settings,
            'messages': diagnostics_messages,
        },
    }


def get_automation_diagnostics() -> dict[str, Any]:
    dashboard = get_group_state_dashboard(limit_per_state=10)
    conn = get_conn()
    try:
        today = datetime.now().strftime('%Y-%m-%d') + '%'
        failure_rows = conn.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(pesan_error),''), status) AS reason, COUNT(*) AS total
            FROM riwayat
            WHERE status IN ('send_failed','gagal','join_failed') AND waktu LIKE %s
            GROUP BY COALESCE(NULLIF(TRIM(pesan_error),''), status)
            ORDER BY total DESC, reason ASC
            LIMIT 8
            """,
            (today,),
        ).fetchall()
        account_rows = conn.execute(
            """
            SELECT phone, COALESCE(nama, phone) AS nama,
                   COALESCE(status,'active') AS status,
                   COALESCE(level_warming,1) AS level_warming,
                   COALESCE(daily_new_group_cap,0) AS daily_new_group_cap,
                   COALESCE(fresh_login_grace_minutes,15) AS fresh_login_grace_minutes
            FROM akun
            ORDER BY COALESCE(priority_weight,100) DESC, COALESCE(nama, phone) ASC
            """
        ).fetchall()
    finally:
        conn.close()

    account_limits = []
    for row in account_rows:
        phone = str(row['phone'])
        try:
            info = get_info_warming(phone)
            cap = get_daily_capacity(phone)
            sudah_kirim = int(cap.get('kirim', {}).get('used') or 0)
            sudah_join = int(cap.get('join', {}).get('used') or 0)
            maks_kirim = int(cap.get('kirim', {}).get('limit') or info.get('maks_kirim') or 0)
            maks_join = int(cap.get('join', {}).get('limit') or info.get('maks_join') or 0)
        except Exception:
            sudah_kirim = sudah_join = maks_kirim = maks_join = 0
        account_limits.append({
            'phone': phone,
            'nama': row['nama'],
            'status': row['status'],
            'level_warming': int(row['level_warming'] or 1),
            'kirim': {'used': sudah_kirim, 'limit': maks_kirim, 'remaining': max(0, maks_kirim - sudah_kirim)},
            'join': {'used': sudah_join, 'limit': maks_join, 'remaining': max(0, maks_join - sudah_join)},
            'fresh_login_grace_minutes': int(row['fresh_login_grace_minutes'] or 0),
        })

    messages = list((dashboard.get('diagnostics') or {}).get('messages') or [])
    if any(item['join']['remaining'] <= 0 for item in account_limits if item['join']['limit'] > 0):
        messages.append('Ada akun yang kuota join hari ini habis. Auto Join akan berhenti lebih cepat pada akun tersebut.')
    if any(item['kirim']['remaining'] <= 0 for item in account_limits if item['kirim']['limit'] > 0):
        messages.append('Ada akun yang kuota kirim hari ini habis. Delivery broadcast akan memakai akun tersisa atau menunggu hari berikutnya.')
    lanes = dashboard.get('summary', {}).get('state_counts', {})
    if int(lanes.get('stabilization', 0) or 0) > 0:
        messages.append('Masih ada grup di stabilization. Pada profil cepat sekarang delay dibuat 0 menit agar grup lebih cepat lanjut.')

    return {
        'messages': messages,
        'failure_reasons_today': [dict(r) for r in failure_rows],
        'accounts': account_limits,
        'dashboard': dashboard,
    }
