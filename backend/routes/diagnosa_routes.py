from __future__ import annotations

from datetime import datetime
from flask import Blueprint, request

from utils.api import ok
from utils.database import get_conn

bp = Blueprint('diagnosa', __name__, url_prefix='/api/v2')


def _now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


@bp.get('/diagnosa')
def diagnosa():
    """
    Endpoint diagnosa lengkap: mendeteksi grup & akun yang macet/stuck,
    beserta alasan spesifik kenapa mereka tidak bisa maju ke tahap berikutnya.
    """
    conn = get_conn()
    now = _now_str()
    hasil = {}

    # ── 1. GRUP STUCK DI WAITING JOIN ──────────────────────────────────────
    # Grup sudah assigned, punya owner, tapi owner belum join
    grup_waiting_join = conn.execute("""
        SELECT
            g.id, g.nama, g.username, g.owner_phone,
            g.join_status, g.join_hold_reason, g.join_ready_at,
            g.broadcast_status, g.assignment_status,
            g.score, g.status,
            a.status AS akun_status,
            a.cooldown_until,
            a.level_warming,
            CASE WHEN ag.phone IS NOT NULL THEN 1 ELSE 0 END AS sudah_join,
            CASE WHEN a.phone IS NULL THEN 'akun_tidak_ada'
                 WHEN a.status != 'active' THEN 'akun_nonaktif'
                 WHEN ag.phone IS NOT NULL THEN 'sudah_join_tapi_belum_diupdate'
                 WHEN g.username IS NULL OR g.username = '' THEN 'tidak_ada_username'
                 WHEN g.join_hold_reason = 'approval_pending' THEN 'menunggu_approval_admin'
                 WHEN g.join_hold_reason = 'join_floodwait' THEN 'floodwait_join'
                 WHEN g.join_hold_reason = 'invalid_target_final' THEN 'username_invalid'
                 WHEN g.broadcast_status = 'blocked' THEN 'grup_diblokir'
                 WHEN g.join_ready_at > TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') THEN 'dalam_cooldown'
                 ELSE 'menunggu_giliran'
            END AS penyebab_macet
        FROM grup g
        LEFT JOIN akun a ON a.phone = g.owner_phone
        LEFT JOIN akun_grup ag ON ag.phone = g.owner_phone AND ag.grup_id = g.id
        WHERE g.assignment_status = 'assigned'
          AND g.owner_phone IS NOT NULL
          AND g.status = 'active'
          AND COALESCE(g.broadcast_status,'hold') != 'blocked'
          AND COALESCE(g.join_hold_reason,'') != 'invalid_target_final'
          AND ag.phone IS NULL
        ORDER BY g.score DESC, g.id DESC
        LIMIT 100
    """).fetchall()

    hasil['waiting_join'] = {
        'jumlah': len(grup_waiting_join),
        'items': [dict(r) for r in grup_waiting_join]
    }

    # ── 2. GRUP STUCK DI BROADCAST / TIDAK PERNAH DIKIRIM ──────────────────
    grup_broadcast_macet = conn.execute("""
        SELECT
            g.id, g.nama, g.username, g.owner_phone,
            g.broadcast_status, g.broadcast_hold_reason, g.broadcast_ready_at,
            g.assignment_status, g.join_status,
            g.last_kirim, g.score,
            CASE
                WHEN g.broadcast_status = 'blocked' THEN 'grup_diblokir_permanen'
                WHEN g.broadcast_status = 'cooldown' THEN 'dalam_masa_istirahat'
                WHEN g.broadcast_hold_reason IS NOT NULL THEN g.broadcast_hold_reason
                WHEN g.owner_phone IS NULL THEN 'belum_ada_owner'
                WHEN g.assignment_status != 'managed' THEN 'owner_belum_join'
                ELSE 'tidak_diketahui'
            END AS penyebab_macet
        FROM grup g
        WHERE g.status = 'active'
          AND COALESCE(g.broadcast_status,'hold') IN ('blocked','cooldown','hold')
          AND g.assignment_status = 'managed'
        ORDER BY g.id DESC
        LIMIT 100
    """).fetchall()

    hasil['broadcast_macet'] = {
        'jumlah': len(grup_broadcast_macet),
        'items': [dict(r) for r in grup_broadcast_macet]
    }

    # ── 3. AKUN BERMASALAH ─────────────────────────────────────────────────
    akun_bermasalah = conn.execute("""
        SELECT
            a.phone, a.nama, a.status, a.level_warming,
            a.score, a.cooldown_until, a.total_flood, a.total_banned,
            COUNT(ag.grup_id) AS jumlah_grup,
            CASE
                WHEN a.status = 'banned' THEN 'akun_banned'
                WHEN a.status = 'suspended' THEN 'akun_disuspend'
                WHEN a.status = 'session_expired' THEN 'session_expired_login_ulang'
                WHEN a.cooldown_until > TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') THEN 'dalam_cooldown_floodwait'
                WHEN a.level_warming < 2 THEN 'level_warming_terlalu_rendah'
                ELSE 'tidak_aktif'
            END AS penyebab_masalah
        FROM akun a
        LEFT JOIN akun_grup ag ON ag.phone = a.phone
        WHERE a.status != 'active'
           OR a.cooldown_until > TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
           OR a.level_warming < 2
        GROUP BY a.phone
        ORDER BY a.status, a.phone
    """).fetchall()

    hasil['akun_bermasalah'] = {
        'jumlah': len(akun_bermasalah),
        'items': [dict(r) for r in akun_bermasalah]
    }

    # ── 4. CAMPAIGN TARGET STUCK ────────────────────────────────────────────
    target_stuck = conn.execute("""
        SELECT
            ct.id, ct.campaign_id, ct.group_id,
            g.nama AS nama_grup, g.username,
            ct.sender_account_id AS akun,
            ct.status, ct.hold_reason,
            ct.attempt_count, ct.last_attempt_at,
            CASE
                WHEN ct.status = 'failed' THEN 'gagal_kirim_permanen'
                WHEN ct.status = 'blocked' THEN 'diblokir_di_grup_ini'
                WHEN ct.hold_reason IS NOT NULL THEN ct.hold_reason
                WHEN ct.attempt_count >= 3 THEN 'sudah_3x_coba_gagal'
                ELSE 'menunggu_pengiriman'
            END AS penyebab_macet
        FROM campaign_target ct
        LEFT JOIN grup g ON g.id = ct.group_id
        WHERE ct.status IN ('failed','blocked','retry','hold')
        ORDER BY ct.id DESC
        LIMIT 100
    """).fetchall()

    hasil['campaign_target_stuck'] = {
        'jumlah': len(target_stuck),
        'items': [dict(r) for r in target_stuck]
    }

    # ── 5. RINGKASAN RECOVERY ITEM ─────────────────────────────────────────
    recovery_summary = conn.execute("""
        SELECT
            ri.item_type, ri.recovery_status, ri.severity,
            ri.entity_id, ri.entity_name,
            ri.reason, ri.last_attempt_at, ri.attempt_count,
            ri.created_at
        FROM recovery_item ri
        WHERE ri.recovery_status IN ('pending','partial','failed')
        ORDER BY ri.severity DESC, ri.created_at DESC
        LIMIT 50
    """).fetchall()

    hasil['recovery_stuck'] = {
        'jumlah': len(recovery_summary),
        'items': [dict(r) for r in recovery_summary]
    }

    # ── 6. STATISTIK CEPAT ─────────────────────────────────────────────────
    stats = conn.execute("""
        SELECT
            COUNT(*) AS total_grup,
            SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS grup_aktif,
            SUM(CASE WHEN assignment_status='managed' THEN 1 ELSE 0 END) AS grup_managed,
            SUM(CASE WHEN assignment_status='assigned' THEN 1 ELSE 0 END) AS grup_assigned,
            SUM(CASE WHEN broadcast_status='blocked' THEN 1 ELSE 0 END) AS grup_blocked,
            SUM(CASE WHEN broadcast_status='cooldown' THEN 1 ELSE 0 END) AS grup_cooldown,
            SUM(CASE WHEN join_hold_reason='approval_pending' THEN 1 ELSE 0 END) AS menunggu_approval,
            SUM(CASE WHEN join_hold_reason='join_floodwait' THEN 1 ELSE 0 END) AS kena_floodwait,
            SUM(CASE WHEN join_hold_reason='invalid_target_final' THEN 1 ELSE 0 END) AS username_invalid
        FROM grup WHERE status='active'
    """).fetchone()

    akun_stats = conn.execute("""
        SELECT
            COUNT(*) AS total_akun,
            SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS akun_aktif,
            SUM(CASE WHEN status='banned' THEN 1 ELSE 0 END) AS akun_banned,
            SUM(CASE WHEN status='session_expired' THEN 1 ELSE 0 END) AS session_expired,
            SUM(CASE WHEN cooldown_until > TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') THEN 1 ELSE 0 END) AS dalam_cooldown
        FROM akun
    """).fetchone()

    conn.close()

    hasil['statistik'] = {
        'grup': dict(stats) if stats else {},
        'akun': dict(akun_stats) if akun_stats else {},
        'digenerate_pada': now
    }

    return ok(hasil)


@bp.get('/diagnosa/grup/<int:grup_id>')
def diagnosa_grup(grup_id: int):
    """Detail log lengkap untuk satu grup tertentu."""
    conn = get_conn()

    grup = conn.execute("SELECT * FROM grup WHERE id=%s", (grup_id,)).fetchone()
    if not grup:
        conn.close()
        return ok({'error': 'Grup tidak ditemukan'}), 404

    # Riwayat aksi untuk grup ini
    riwayat = conn.execute("""
        SELECT phone, nama_grup, status, pesan_error, waktu
        FROM riwayat WHERE grup_id=%s
        ORDER BY id DESC LIMIT 50
    """, (grup_id,)).fetchall()

    # Audit log terkait grup ini
    audit = conn.execute("""
        SELECT level, module, action, result, message, payload, created_at
        FROM audit_log
        WHERE entity_id=%s OR message LIKE %s
        ORDER BY id DESC LIMIT 50
    """, (str(grup_id), f'%{grup_id}%')).fetchall()

    # Akun yang join grup ini
    akun_join = conn.execute("""
        SELECT a.phone, a.nama, a.status, a.level_warming, a.score, a.cooldown_until
        FROM akun_grup ag
        JOIN akun a ON a.phone = ag.phone
        WHERE ag.grup_id=%s
    """, (grup_id,)).fetchall()

    # Assignment history
    assignments = conn.execute("""
        SELECT assigned_account_id, status, created_at, attempt_count, hold_reason
        FROM group_assignment WHERE group_id=%s
        ORDER BY id DESC LIMIT 10
    """, (grup_id,)).fetchall()

    # Campaign targets
    targets = conn.execute("""
        SELECT ct.id, ct.campaign_id, ct.sender_account_id, ct.status,
               ct.hold_reason, ct.attempt_count, ct.last_attempt_at
        FROM campaign_target ct WHERE ct.group_id=%s
        ORDER BY ct.id DESC LIMIT 20
    """, (grup_id,)).fetchall()

    conn.close()

    return ok({
        'grup': dict(grup),
        'riwayat': [dict(r) for r in riwayat],
        'audit_log': [dict(r) for r in audit],
        'akun_join': [dict(r) for r in akun_join],
        'assignments': [dict(r) for r in assignments],
        'campaign_targets': [dict(r) for r in targets],
    })


@bp.get('/diagnosa/akun/<path:phone>')
def diagnosa_akun(phone: str):
    """Detail log lengkap untuk satu akun tertentu."""
    conn = get_conn()

    akun = conn.execute("SELECT * FROM akun WHERE phone=%s", (phone,)).fetchone()
    if not akun:
        conn.close()
        return ok({'error': 'Akun tidak ditemukan'}), 404

    riwayat = conn.execute("""
        SELECT grup_id, nama_grup, status, pesan_error, waktu
        FROM riwayat WHERE phone=%s
        ORDER BY id DESC LIMIT 50
    """, (phone,)).fetchall()

    audit = conn.execute("""
        SELECT level, module, action, result, message, payload, created_at
        FROM audit_log WHERE message LIKE %s
        ORDER BY id DESC LIMIT 50
    """, (f'%{phone}%',)).fetchall()

    grup_dipegang = conn.execute("""
        SELECT g.id, g.nama, g.username, g.status, g.assignment_status,
               g.broadcast_status, g.join_status, g.score
        FROM grup g
        WHERE g.owner_phone=%s
        ORDER BY g.score DESC LIMIT 50
    """, (phone,)).fetchall()

    conn.close()

    return ok({
        'akun': dict(akun),
        'riwayat': [dict(r) for r in riwayat],
        'audit_log': [dict(r) for r in audit],
        'grup_dipegang': [dict(r) for r in grup_dipegang],
    })
