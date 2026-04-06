from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

from services.account_manager import _clients
from utils.database import get_conn
from utils.storage_db import (
    create_automation_rule,
    get_automation_rule,
    get_automation_rules,
    update_automation_rule,
)

_CANONICAL_BY_ALIAS = {
    'import': 'import',
    'auto_import': 'import',
    'scraper_import': 'import',
    'permission': 'permission',
    'permission_check': 'permission',
    'auto_permission': 'permission',
    'assignment': 'assignment',
    'auto_assign': 'assignment',
    'campaign_prepare': 'campaign_prepare',
    'broadcast_prepare': 'campaign_prepare',
    'campaign': 'campaign_prepare',
    'delivery': 'delivery',
    'broadcast': 'delivery',
    'auto_campaign': 'delivery',
    'recovery_scan': 'recovery_scan',
    'recovery_execute': 'recovery_execute',
    'recovery': 'recovery_scan',
    'auto_recovery': 'recovery_scan',
}

_STAGE_ALIASES = {
    'import': {'import', 'auto_import', 'scraper_import'},
    'permission': {'permission', 'permission_check', 'auto_permission'},
    'assignment': {'assignment', 'auto_assign'},
    'campaign_prepare': {'campaign_prepare', 'broadcast_prepare', 'campaign'},
    'delivery': {'delivery', 'broadcast', 'auto_campaign'},
    'recovery_scan': {'recovery_scan', 'recovery', 'auto_recovery'},
    'recovery_execute': {'recovery_execute'},
}

_STAGE_DEFAULT_CONFIG = {
    'import': {
        'action': {'limit_jobs': 10, 'mode': 'all_new'},
        'scope': {'job_status_in': ['done'], 'require_unimported_results': True},
        'condition': {'pending_count_gte': 1},
    },
    'permission': {
        'action': {'limit': 100, 'permission_basis': 'opt_in', 'approval_source': 'auto_orchestrator', 'approved_by': 'system', 'status': 'valid', 'notes': 'Diset otomatis oleh orchestrator'},
        'scope': {'group_status_in': ['active'], 'permission_status_in': ['unknown'], 'exclude_channels': True},
        'condition': {'pending_count_gte': 1},
    },
    'assignment': {
        'action': {'limit': 150, 'prefer_joined_owner': True, 'mark_broadcast_eligible_when_managed': True, 'create_recovery_on_no_candidate': True, 'assignment_delay_minutes': 0},
        'scope': {'group_status_in': ['active'], 'permission_status_in': ['valid', 'owned', 'admin', 'partner_approved', 'opt_in'], 'assignment_status_in': ['ready_assign', 'retry_wait', 'reassign_pending', 'failed', 'assigned'], 'exclude_channels': True},
        'condition': {'pending_count_gte': 1},
    },
    'campaign_prepare': {
        'action': {'limit': 150, 'sender_source': 'owner_phone', 'reuse_active_campaign': True, 'create_if_missing': True, 'skip_inactive_groups': False, 'inactive_threshold_days': 14, 'skip_if_last_chat_is_ours': False, 'assignment_delay_minutes': 0, 'session_target_limit': 200, 'allow_mid_session_enqueue': True},
        'scope': {'group_status_in': ['active'], 'permission_status_in': ['valid', 'owned', 'admin', 'partner_approved', 'opt_in'], 'assignment_status_in': ['managed'], 'broadcast_status_in': ['broadcast_eligible', 'hold', 'queued', 'stabilization_wait', 'cooldown'], 'exclude_channels': True, 'exclude_if_already_targeted': True},
        'condition': {'pending_count_gte': 1},
    },
    'delivery': {
        'action': {'limit': 40, 'retry_delay_minutes': 1, 'require_active_draft': True, 'require_online_sender': True, 'blocked_terms': ['tidak punya izin', 'banned', 'private'], 'skip_inactive_groups': False, 'inactive_threshold_days': 14, 'skip_if_last_chat_is_ours': False, 'session_per_sender_limit': 20, 'group_cooldown_hours': 0, 'group_cooldown_minutes': 1, 'requeue_sender_missing': True},
        'scope': {'target_status_in': ['queued', 'eligible'], 'campaign_status_in': ['queued', 'running']},
        'condition': {'pending_count_gte': 1, 'online_accounts_gte': 1, 'active_draft_required': True},
    },
    'recovery_scan': {
        'action': {'limit': 50, 'scrape_threshold_minutes': 30, 'assignment_threshold_minutes': 30, 'campaign_threshold_minutes': 30},
        'scope': {'watch_entities': ['scrape_job', 'assignment', 'campaign']},
        'condition': {},
    },
    'recovery_execute': {
        'action': {'limit': 25, 'max_failed_targets_to_requeue': 50, 'max_recovery_attempts': 5},
        'scope': {'entity_types': ['scrape_job', 'assignment', 'campaign']},
        'condition': {'pending_count_gte': 1},
    },
}


_STAGE_TITLES = {
    'import': 'Import hasil scraper',
    'permission': 'Pemberian permission otomatis',
    'assignment': 'Penentuan owner grup',
    'campaign_prepare': 'Persiapan target campaign',
    'delivery': 'Pengiriman broadcast',
    'recovery_scan': 'Deteksi item macet',
    'recovery_execute': 'Eksekusi pemulihan',
}

_STAGE_EDITOR_HELP = {
    'import': {
        'summary': 'Rule ini mengatur kapan hasil scrape yang sudah selesai diimpor ke tabel grup.',
        'condition': {
            'pending_count_gte': 'Minimal jumlah scrape job selesai yang masih punya hasil belum diimpor.',
            'pending_count_lte': 'Batas atas job yang boleh menunggu sebelum rule diizinkan berjalan.',
        },
        'action': {
            'limit_jobs': 'Jumlah maksimum scrape job yang diproses per siklus.',
            'mode': 'Strategi impor, misalnya all_new untuk hanya memasukkan hasil baru.',
        },
        'scope': {
            'job_status_in': 'Status scrape job yang boleh diambil.',
            'require_unimported_results': 'Bila true, hanya job dengan hasil belum diimpor yang diproses.',
        },
    },
    'permission': {
        'summary': 'Rule ini memberi permission otomatis ke grup baru sesuai basis izin yang ditentukan.',
        'condition': {'pending_count_gte': 'Minimal jumlah grup yang masih menunggu permission.'},
        'action': {
            'limit': 'Jumlah maksimum grup yang diberi permission per siklus.',
            'permission_basis': 'Basis izin yang akan ditulis ke grup, misalnya opt_in atau owned.',
            'approval_source': 'Sumber approval yang dicatat ke audit.',
            'approved_by': 'Nama pengguna/sistem yang dicatat sebagai pemberi approval.',
            'status': 'Status permission akhir yang ditetapkan ke grup.',
            'notes': 'Catatan operasional yang disimpan bersama perubahan permission.',
        },
        'scope': {
            'group_status_in': 'Status grup yang boleh dipertimbangkan.',
            'permission_status_in': 'Hanya grup dengan permission status ini yang akan diproses.',
            'exclude_channels': 'Bila true, channel tidak ikut diproses.',
        },
    },
    'assignment': {
        'summary': 'Rule ini memilih akun owner terbaik untuk grup yang sudah lolos permission.',
        'condition': {'pending_count_gte': 'Minimal jumlah grup siap assign agar rule berjalan.'},
        'action': {
            'limit': 'Jumlah maksimum grup yang di-assign per siklus.',
            'prefer_joined_owner': 'Utamakan akun yang memang sudah join grup.',
            'mark_broadcast_eligible_when_managed': 'Bila managed berhasil, otomatis tandai siap broadcast.',
            'create_recovery_on_no_candidate': 'Buat recovery item saat tidak ada kandidat owner.',
            'assignment_delay_minutes': 'Tahan grup beberapa menit setelah assign sebelum boleh masuk broadcast.',
        },
        'scope': {
            'group_status_in': 'Status grup yang boleh di-assign.',
            'permission_status_in': 'Status permission yang diizinkan untuk assign.',
            'assignment_status_in': 'Status assignment yang masih boleh diproses ulang.',
            'exclude_channels': 'Bila true, channel tidak ikut diproses.',
        },
    },
    'campaign_prepare': {
        'summary': 'Rule ini memasukkan grup managed ke antrian campaign broadcast.',
        'condition': {'pending_count_gte': 'Minimal jumlah grup eligible agar rule berjalan.'},
        'action': {
            'limit': 'Jumlah maksimum target campaign yang dibuat per siklus.',
            'sender_source': 'Sumber nomor pengirim, misalnya owner_phone.',
            'reuse_active_campaign': 'Pakai campaign aktif yang masih ada sebelum membuat campaign baru.',
            'create_if_missing': 'Buat campaign baru bila belum ada campaign aktif.',
            'skip_inactive_groups': 'Tahan grup yang last chat-nya terlalu lama agar tidak boros kuota.',
            'inactive_threshold_days': 'Jumlah hari tanpa chat baru sebelum grup dianggap tidak aktif.',
            'skip_if_last_chat_is_ours': 'Tahan grup bila chat terakhir masih pesan kita sendiri.',
            'assignment_delay_minutes': 'Minimal umur assignment sebelum grup baru boleh masuk sesi broadcast.',
            'session_target_limit': 'Maksimum jumlah target yang boleh berada di satu sesi campaign.',
            'allow_mid_session_enqueue': 'Bila false, grup baru hanya masuk ke sesi berikutnya saat sesi aktif sedang berjalan.',
        },
        'scope': {
            'group_status_in': 'Status grup yang boleh dimasukkan ke campaign.',
            'permission_status_in': 'Status permission yang diizinkan.',
            'assignment_status_in': 'Status assignment yang dianggap siap campaign.',
            'broadcast_status_in': 'Status broadcast grup yang boleh masuk queue.',
            'exclude_channels': 'Bila true, channel diabaikan.',
            'exclude_if_already_targeted': 'Bila true, grup yang masih punya target aktif tidak digandakan.',
        },
    },
    'delivery': {
        'summary': 'Rule ini mengirim pesan ke target campaign yang sudah queued.',
        'condition': {
            'pending_count_gte': 'Minimal target queued yang menunggu pengiriman.',
            'online_accounts_gte': 'Minimal jumlah akun online agar delivery boleh berjalan.',
            'active_draft_required': 'Bila true, rule hanya jalan saat ada draft aktif.',
        },
        'action': {
            'limit': 'Jumlah maksimum target yang dicoba kirim per siklus.',
            'retry_delay_minutes': 'Jeda sebelum target gagal dicoba lagi.',
            'require_active_draft': 'Paksa berhenti bila tidak ada draft aktif.',
            'require_online_sender': 'Paksa berhenti bila tidak ada sender online.',
            'blocked_terms': 'Daftar kata kunci error yang dianggap blocked, bukan sekadar failed.',
            'skip_inactive_groups': 'Jangan kirim ke grup yang sudah lama sepi.',
            'inactive_threshold_days': 'Batas hari grup dianggap sepi/tidak aktif.',
            'skip_if_last_chat_is_ours': 'Jangan kirim lagi bila chat terakhir di grup masih pesan kita.',
            'session_per_sender_limit': 'Maksimum target yang boleh diambil satu akun dalam satu siklus delivery.',
            'group_cooldown_hours': 'Jeda cooldown grup dalam jam setelah pesan berhasil terkirim.',
            'group_cooldown_minutes': 'Jeda cooldown grup dalam menit setelah pesan berhasil terkirim. Dipakai bila diisi lebih dari 0.',
            'requeue_sender_missing': 'Bila true, target tanpa sender/ sender offline diantrekan ulang, bukan langsung digagalkan permanen.',
        },
        'scope': {
            'target_status_in': 'Status target yang boleh dicoba dikirim.',
            'campaign_status_in': 'Status campaign yang dianggap aktif untuk delivery.',
        },
    },
    'recovery_scan': {
        'summary': 'Rule ini memindai job, assignment, dan campaign yang macet lalu membuat recovery item.',
        'condition': {},
        'action': {
            'limit': 'Jumlah maksimum recovery item yang dibuat/diperbarui per siklus scan.',
            'scrape_threshold_minutes': 'Batas menit job scraper dianggap macet.',
            'assignment_threshold_minutes': 'Batas menit assignment dianggap macet.',
            'campaign_threshold_minutes': 'Batas menit campaign dianggap macet.',
        },
        'scope': {'watch_entities': 'Daftar entity yang dipantau, misalnya scrape_job, assignment, campaign.'},
    },
    'recovery_execute': {
        'summary': 'Rule ini mencoba memulihkan recovery item yang sudah dianggap recoverable.',
        'condition': {'pending_count_gte': 'Minimal recovery item recoverable agar rule berjalan.'},
        'action': {
            'limit': 'Jumlah maksimum recovery item yang dieksekusi per siklus.',
            'max_failed_targets_to_requeue': 'Jumlah maksimum target failed yang dikembalikan ke queue.',
            'max_recovery_attempts': 'Batas percobaan pemulihan per item sebelum dianggap terlalu berisiko.',
        },
        'scope': {'entity_types': 'Jenis entity recovery yang boleh dieksekusi.'},
    },
}

_STAGE_FIELD_TYPES = {
    'import': {'condition': {'pending_count_gte': 'int', 'pending_count_lte': 'int'}, 'action': {'limit_jobs': 'int', 'mode': 'string'}, 'scope': {'job_status_in': 'string_list', 'require_unimported_results': 'bool'}},
    'permission': {'condition': {'pending_count_gte': 'int'}, 'action': {'limit': 'int', 'permission_basis': 'string', 'approval_source': 'string', 'approved_by': 'string', 'status': 'string', 'notes': 'string'}, 'scope': {'group_status_in': 'string_list', 'permission_status_in': 'string_list', 'exclude_channels': 'bool'}},
    'assignment': {'condition': {'pending_count_gte': 'int'}, 'action': {'limit': 'int', 'prefer_joined_owner': 'bool', 'mark_broadcast_eligible_when_managed': 'bool', 'create_recovery_on_no_candidate': 'bool', 'assignment_delay_minutes': 'int'}, 'scope': {'group_status_in': 'string_list', 'permission_status_in': 'string_list', 'assignment_status_in': 'string_list', 'exclude_channels': 'bool'}},
    'campaign_prepare': {'condition': {'pending_count_gte': 'int'}, 'action': {'limit': 'int', 'sender_source': 'string', 'reuse_active_campaign': 'bool', 'create_if_missing': 'bool', 'skip_inactive_groups': 'bool', 'inactive_threshold_days': 'int', 'skip_if_last_chat_is_ours': 'bool', 'assignment_delay_minutes': 'int', 'session_target_limit': 'int', 'allow_mid_session_enqueue': 'bool'}, 'scope': {'group_status_in': 'string_list', 'permission_status_in': 'string_list', 'assignment_status_in': 'string_list', 'broadcast_status_in': 'string_list', 'exclude_channels': 'bool', 'exclude_if_already_targeted': 'bool'}},
    'delivery': {'condition': {'pending_count_gte': 'int', 'online_accounts_gte': 'int', 'active_draft_required': 'bool'}, 'action': {'limit': 'int', 'retry_delay_minutes': 'int', 'require_active_draft': 'bool', 'require_online_sender': 'bool', 'blocked_terms': 'string_list', 'skip_inactive_groups': 'bool', 'inactive_threshold_days': 'int', 'skip_if_last_chat_is_ours': 'bool', 'session_per_sender_limit': 'int', 'group_cooldown_hours': 'int', 'group_cooldown_minutes': 'int', 'requeue_sender_missing': 'bool'}, 'scope': {'target_status_in': 'string_list', 'campaign_status_in': 'string_list'}},
    'recovery_scan': {'condition': {}, 'action': {'limit': 'int', 'scrape_threshold_minutes': 'int', 'assignment_threshold_minutes': 'int', 'campaign_threshold_minutes': 'int'}, 'scope': {'watch_entities': 'string_list'}},
    'recovery_execute': {'condition': {'pending_count_gte': 'int'}, 'action': {'limit': 'int', 'max_failed_targets_to_requeue': 'int', 'max_recovery_attempts': 'int'}, 'scope': {'entity_types': 'string_list'}},
}

_DEFAULT_RULES = [
    {
        'name': '[SYSTEM] Import done scrape jobs',
        'rule_type': 'import',
        'priority': 100,
        'cooldown_seconds': 15,
        'condition_json': {'pending_count_gte': 1},
        'action_json': {'limit_jobs': 10, 'mode': 'all_new'},
        'scope_json': {'job_status_in': ['done'], 'require_unimported_results': True},
    },
    {
        'name': '[SYSTEM] Grant permission to new groups',
        'rule_type': 'permission',
        'priority': 100,
        'cooldown_seconds': 15,
        'condition_json': {'pending_count_gte': 1},
        'action_json': {'limit': 100, 'permission_basis': 'opt_in', 'approval_source': 'auto_orchestrator', 'approved_by': 'system', 'status': 'valid', 'notes': 'Diset otomatis oleh orchestrator'},
        'scope_json': {'group_status_in': ['active'], 'permission_status_in': ['unknown'], 'exclude_channels': True},
    },
    {
        'name': '[SYSTEM] Assign best owner to permitted groups',
        'rule_type': 'assignment',
        'priority': 100,
        'cooldown_seconds': 20,
        'condition_json': {'pending_count_gte': 1},
        'action_json': {'limit': 150, 'prefer_joined_owner': True, 'mark_broadcast_eligible_when_managed': True, 'create_recovery_on_no_candidate': True, 'assignment_delay_minutes': 0},
        'scope_json': {'group_status_in': ['active'], 'permission_status_in': ['valid', 'owned', 'admin', 'partner_approved', 'opt_in'], 'assignment_status_in': ['ready_assign', 'retry_wait', 'reassign_pending', 'failed', 'assigned'], 'exclude_channels': True},
    },
    {
        'name': '[SYSTEM] Queue managed groups into campaign',
        'rule_type': 'campaign_prepare',
        'priority': 100,
        'cooldown_seconds': 20,
        'condition_json': {'pending_count_gte': 1},
        'action_json': {'limit': 150, 'sender_source': 'owner_phone', 'reuse_active_campaign': True, 'create_if_missing': True, 'skip_inactive_groups': False, 'inactive_threshold_days': 14, 'skip_if_last_chat_is_ours': False, 'assignment_delay_minutes': 0, 'session_target_limit': 200, 'allow_mid_session_enqueue': True},
        'scope_json': {'group_status_in': ['active'], 'permission_status_in': ['valid', 'owned', 'admin', 'partner_approved', 'opt_in'], 'assignment_status_in': ['managed'], 'broadcast_status_in': ['broadcast_eligible', 'hold', 'queued', 'failed', 'stabilization_wait', 'cooldown'], 'exclude_channels': True, 'exclude_if_already_targeted': True},
    },
    {
        'name': '[SYSTEM] Deliver queued broadcast targets',
        'rule_type': 'delivery',
        'priority': 100,
        'cooldown_seconds': 10,
        'condition_json': {'pending_count_gte': 1, 'online_accounts_gte': 1, 'active_draft_required': True},
        'action_json': {'limit': 40, 'retry_delay_minutes': 1, 'require_active_draft': True, 'require_online_sender': True, 'blocked_terms': ['tidak punya izin', 'banned', 'private', 'restricted', 'chat_restricted', 'you are not allowed', 'sendmessagerequest'], 'skip_inactive_groups': False, 'inactive_threshold_days': 14, 'skip_if_last_chat_is_ours': False, 'session_per_sender_limit': 20, 'group_cooldown_hours': 0, 'group_cooldown_minutes': 1, 'requeue_sender_missing': True},
        'scope_json': {'target_status_in': ['queued', 'eligible', 'failed'], 'campaign_status_in': ['queued', 'running']},
    },
    {
        'name': '[SYSTEM] Detect stuck automation items',
        'rule_type': 'recovery_scan',
        'priority': 100,
        'cooldown_seconds': 30,
        'condition_json': {},
        'action_json': {'limit': 50, 'scrape_threshold_minutes': 30, 'assignment_threshold_minutes': 30, 'campaign_threshold_minutes': 30},
        'scope_json': {'watch_entities': ['scrape_job', 'assignment', 'campaign']},
    },
    {
        'name': '[SYSTEM] Recover recoverable items safely',
        'rule_type': 'recovery_execute',
        'priority': 100,
        'cooldown_seconds': 30,
        'condition_json': {'pending_count_gte': 1},
        'action_json': {'limit': 25, 'max_failed_targets_to_requeue': 50, 'max_recovery_attempts': 5},
        'scope_json': {'entity_types': ['scrape_job', 'assignment', 'campaign']},
    },
]


def canonical_rule_type(rule_type: str | None) -> str:
    raw = (rule_type or '').strip().lower()
    return _CANONICAL_BY_ALIAS.get(raw, raw or 'unknown')


def _stage_aliases(stage: str) -> set[str]:
    canonical = canonical_rule_type(stage)
    return _STAGE_ALIASES.get(canonical, {canonical})


def _safe_json(value: Any, fallback: dict | list | None = None):
    if isinstance(value, (dict, list)):
        return value
    if value in (None, '', 'null'):
        return deepcopy(fallback) if fallback is not None else {}
    try:
        parsed = json.loads(value)
        if isinstance(parsed, (dict, list)):
            return parsed
    except Exception:
        pass
    return deepcopy(fallback) if fallback is not None else {}


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


def _is_in_cooldown(rule: dict[str, Any]) -> tuple[bool, int]:
    cooldown = int(rule.get('cooldown_seconds') or 0)
    if cooldown <= 0:
        return False, 0
    last = _parse_dt(rule.get('last_triggered_at'))
    if not last:
        return False, 0
    next_allowed = last + timedelta(seconds=cooldown)
    remaining = int((next_allowed - datetime.now()).total_seconds())
    return remaining > 0, max(0, remaining)


def _merge_dict(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _stage_help(stage: str) -> dict[str, Any]:
    canonical = canonical_rule_type(stage)
    help_item = deepcopy(_STAGE_EDITOR_HELP.get(canonical, {'summary': '', 'condition': {}, 'action': {}, 'scope': {}}))
    return {
        'stage': canonical,
        'title': _STAGE_TITLES.get(canonical, canonical.replace('_', ' ').title()),
        'summary': help_item.get('summary') or '',
        'condition': help_item.get('condition') or {},
        'action': help_item.get('action') or {},
        'scope': help_item.get('scope') or {},
        'default_config': get_stage_default_config(canonical),
        'field_types': deepcopy(_STAGE_FIELD_TYPES.get(canonical, {'condition': {}, 'action': {}, 'scope': {}})),
    }


def get_rule_editor_meta(stage: str | None = None) -> dict[str, Any]:
    if stage:
        canonical = canonical_rule_type(stage)
        return {'requested_stage': canonical, 'stages': {canonical: _stage_help(canonical)}}
    return {'requested_stage': None, 'stages': {name: _stage_help(name) for name in _STAGE_DEFAULT_CONFIG.keys()}}


def _validate_value_type(value: Any, expected: str) -> bool:
    if expected == 'int':
        return isinstance(value, bool) is False and isinstance(value, int)
    if expected == 'bool':
        return isinstance(value, bool) or value in (0, 1)
    if expected == 'string':
        return isinstance(value, str)
    if expected == 'string_list':
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
    return True


def validate_rule_payload(stage: str, *, condition: Any, action: Any, scope: Any) -> list[str]:
    canonical = canonical_rule_type(stage)
    errors: list[str] = []
    sections = {'condition': condition, 'action': action, 'scope': scope}
    types = _STAGE_FIELD_TYPES.get(canonical, {'condition': {}, 'action': {}, 'scope': {}})
    for section_name, section_value in sections.items():
        if not isinstance(section_value, dict):
            errors.append(f"{section_name}_json harus berupa objek JSON, bukan array atau nilai tunggal")
            continue
        for key, expected in (types.get(section_name) or {}).items():
            if key in section_value and not _validate_value_type(section_value[key], expected):
                errors.append(f"{section_name}.{key} harus bertipe {expected}")
        for key, value in section_value.items():
            if key in {'pending_count_gte','pending_count_lte','online_accounts_gte','limit','limit_jobs','retry_delay_minutes','inactive_threshold_days','scrape_threshold_minutes','assignment_threshold_minutes','campaign_threshold_minutes','max_failed_targets_to_requeue','max_recovery_attempts','assignment_delay_minutes','session_target_limit','session_per_sender_limit','group_cooldown_hours','group_cooldown_minutes'}:
                try:
                    if int(value) < 0:
                        errors.append(f"{section_name}.{key} tidak boleh negatif")
                except Exception:
                    errors.append(f"{section_name}.{key} harus berupa angka bulat")
    return errors


def sync_system_rules_to_fast_profile() -> dict[str, Any]:
    """Paksa rule [SYSTEM] selaras dengan profil cepat di settings default.

    Tujuannya agar UI popup dan engine backend tidak bercabang: bila settings cepat
    sudah diaktifkan, rule sistem bawaan ikut memakai delay/cooldown yang sama.
    """
    rows, _ = get_automation_rules(page=1, page_size=1000)
    by_name = {str(r.get('name') or ''): r for r in rows}
    updated = 0
    for template in _DEFAULT_RULES:
        row = by_name.get(template['name'])
        if not row:
            continue
        action_json = json.dumps(template['action_json'], ensure_ascii=False)
        scope_json = json.dumps(template['scope_json'], ensure_ascii=False)
        condition_json = json.dumps(template['condition_json'], ensure_ascii=False)
        need_update = (str(row.get('action_json') or '') != action_json or str(row.get('scope_json') or '') != scope_json or str(row.get('condition_json') or '') != condition_json or int(row.get('cooldown_seconds') or 0) != int(template['cooldown_seconds']))
        if not need_update:
            continue
        update_automation_rule(int(row['id']), action_json=action_json, scope_json=scope_json, condition_json=condition_json, cooldown_seconds=int(template['cooldown_seconds']))
        updated += 1
    return {'updated': updated, 'required': len(_DEFAULT_RULES)}


def ensure_default_rules() -> dict[str, Any]:
    rows, _ = get_automation_rules(page=1, page_size=1000)
    existing_names = {str(r.get('name') or '') for r in rows}
    created = 0
    for rule in _DEFAULT_RULES:
        if rule['name'] in existing_names:
            continue
        create_automation_rule(
            name=rule['name'],
            rule_type=rule['rule_type'],
            enabled=1,
            priority=rule['priority'],
            condition_json=json.dumps(rule['condition_json'], ensure_ascii=False),
            action_json=json.dumps(rule['action_json'], ensure_ascii=False),
            cooldown_seconds=rule['cooldown_seconds'],
            scope_json=json.dumps(rule['scope_json'], ensure_ascii=False),
        )
        created += 1
    return {'created': created, 'required': len(_DEFAULT_RULES)}




def get_stage_default_config(stage: str) -> dict[str, Any]:
    canonical = canonical_rule_type(stage)
    return deepcopy(_STAGE_DEFAULT_CONFIG.get(canonical, {'condition': {}, 'action': {}, 'scope': {}}))


def _check_akun_aktif(phone: str) -> bool:
    """Return True hanya kalau akun active/online DAN auto_send_enabled=1."""
    try:
        from utils.database import get_conn as _gc
        c = _gc()
        row = c.execute(
            "SELECT status, auto_send_enabled FROM akun WHERE phone=%s", (phone,)
        ).fetchone()
        c.close()
        if not row:
            return False
        return (
            str(row['status'] or '').lower() in ('active', 'online')
            and int(row['auto_send_enabled'] or 1) == 1
        )
    except Exception:
        return False


def get_stage_context(stage: str) -> dict[str, Any]:
    canonical = canonical_rule_type(stage)
    conn = get_conn()
    try:
        if canonical == 'import':
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT sj.id) AS pending_count
                FROM scrape_job sj
                JOIN scrape_result sr ON sr.job_id=sj.id
                WHERE sj.status='done' AND COALESCE(sr.imported,0)=0
                """
            ).fetchone()
            return {'stage': canonical, 'pending_count': int((row['pending_count'] if row else 0) or 0)}
        if canonical == 'permission':
            row = conn.execute(
                """
                SELECT COUNT(*) AS pending_count
                FROM grup g
                WHERE g.status='active'
                  AND COALESCE(g.permission_status,'unknown')='unknown'
                  AND COALESCE(g.tipe,'group') != 'channel'
                """
            ).fetchone()
            return {'stage': canonical, 'pending_count': int((row['pending_count'] if row else 0) or 0)}
        if canonical == 'assignment':
            row = conn.execute(
                """
                SELECT COUNT(*) AS pending_count
                FROM grup g
                WHERE g.status='active'
                  AND COALESCE(g.permission_status,'unknown') IN ('valid','owned','admin','partner_approved','opt_in')
                  AND COALESCE(g.assignment_status,'ready_assign') IN ('ready_assign','retry_wait','reassign_pending','failed','assigned')
                  AND COALESCE(g.tipe,'group') != 'channel'
                """
            ).fetchone()
            return {'stage': canonical, 'pending_count': int((row['pending_count'] if row else 0) or 0)}
        if canonical == 'campaign_prepare':
            row = conn.execute(
                """
                SELECT COUNT(*) AS pending_count
                FROM grup g
                WHERE g.status='active'
                  AND COALESCE(g.permission_status,'unknown') IN ('valid','owned','admin','partner_approved','opt_in')
                  AND COALESCE(g.assignment_status,'ready_assign')='managed'
                  AND COALESCE(g.broadcast_status,'hold') IN ('broadcast_eligible','hold','queued','stabilization_wait','cooldown')
                  AND COALESCE(g.tipe,'group') != 'channel'
                  AND NOT EXISTS (
                    SELECT 1 FROM campaign_target ct
                    JOIN campaign c ON c.id=ct.campaign_id
                    WHERE ct.group_id=g.id AND ct.status IN ('eligible','queued','sending') AND c.status IN ('queued','running','paused')
                  )
                """
            ).fetchone()
            return {'stage': canonical, 'pending_count': int((row['pending_count'] if row else 0) or 0)}
        if canonical == 'delivery':
            row = conn.execute(
                """
                SELECT COUNT(*) AS pending_count
                FROM campaign_target ct
                JOIN campaign c ON c.id=ct.campaign_id
                WHERE c.status IN ('queued','running')
                  AND ct.status IN ('queued','eligible')
                  AND (ct.next_attempt_at IS NULL OR ct.next_attempt_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
                """
            ).fetchone()
            draft = conn.execute("SELECT 1 FROM draft WHERE aktif=1 AND COALESCE(isi,'') != '' LIMIT 1").fetchone()
            return {
                'stage': canonical,
                'pending_count': int((row['pending_count'] if row else 0) or 0),
                'online_accounts': sum(
                    1 for phone in list(_clients.keys())
                    if _check_akun_aktif(phone)
                ),
                'active_draft': bool(draft),
            }
        if canonical == 'recovery_scan':
            return {'stage': canonical, 'pending_count': 1}
        if canonical == 'recovery_execute':
            row = conn.execute(
                "SELECT COUNT(*) AS pending_count FROM recovery_item WHERE recovery_status='recoverable'"
            ).fetchone()
            return {'stage': canonical, 'pending_count': int((row['pending_count'] if row else 0) or 0)}
    finally:
        conn.close()
    return {'stage': canonical, 'pending_count': 0}


def _condition_matches(rule: dict[str, Any], context: dict[str, Any]) -> tuple[bool, list[str]]:
    cond = _safe_json(rule.get('condition_json'), {})
    reasons: list[str] = []
    pending_count = int(context.get('pending_count') or 0)
    if 'pending_count_gte' in cond and pending_count < int(cond['pending_count_gte'] or 0):
        reasons.append(f"pending_count {pending_count} < {int(cond['pending_count_gte'] or 0)}")
    if 'pending_count_lte' in cond and pending_count > int(cond['pending_count_lte'] or 0):
        reasons.append(f"pending_count {pending_count} > {int(cond['pending_count_lte'] or 0)}")
    if cond.get('active_draft_required') and not bool(context.get('active_draft')):
        reasons.append('draft aktif tidak tersedia')
    if 'online_accounts_gte' in cond and int(context.get('online_accounts') or 0) < int(cond['online_accounts_gte'] or 0):
        reasons.append(f"online_accounts {int(context.get('online_accounts') or 0)} < {int(cond['online_accounts_gte'] or 0)}")
    metric_gte = cond.get('metric_gte') or {}
    if isinstance(metric_gte, dict):
        for key, threshold in metric_gte.items():
            if float(context.get(key) or 0) < float(threshold or 0):
                reasons.append(f"{key} {context.get(key) or 0} < {threshold}")
    metric_lte = cond.get('metric_lte') or {}
    if isinstance(metric_lte, dict):
        for key, threshold in metric_lte.items():
            if float(context.get(key) or 0) > float(threshold or 0):
                reasons.append(f"{key} {context.get(key) or 0} > {threshold}")
    return (not reasons), reasons


def _normalize_rule(rule: dict[str, Any]) -> dict[str, Any]:
    item = dict(rule)
    item['canonical_stage'] = canonical_rule_type(item.get('rule_type'))
    item['condition'] = _safe_json(item.get('condition_json'), {})
    item['action'] = _safe_json(item.get('action_json'), {})
    item['scope'] = _safe_json(item.get('scope_json'), {})
    item['explanation'] = explain_rule(item)
    return item


def get_normalized_rules(rule_type: str | None = None, enabled: bool | None = None, *, page: int = 1, page_size: int = 25):
    rows, _ = get_automation_rules(enabled=enabled, page=1, page_size=5000)
    normalized = [_normalize_rule(row) for row in rows]
    if rule_type:
        wanted = canonical_rule_type(rule_type)
        normalized = [row for row in normalized if row.get('canonical_stage') == wanted or str(row.get('rule_type') or '') == rule_type]
    total = len(normalized)
    start = max(0, (page - 1) * page_size)
    end = start + page_size
    return normalized[start:end], total


def get_normalized_rule(rule_id: int) -> dict[str, Any] | None:
    row = get_automation_rule(rule_id)
    return _normalize_rule(row) if row else None


def resolve_stage_rules(stage: str, context: dict[str, Any] | None = None, *, preloaded_rows: list[dict[str, Any]] | None = None, ensure_defaults: bool = True) -> dict[str, Any]:
    if ensure_defaults:
        ensure_default_rules()
    canonical = canonical_rule_type(stage)
    stage_context = context or get_stage_context(canonical)
    rows = preloaded_rows
    if rows is None:
        rows, _ = get_automation_rules(page=1, page_size=1000)
    candidates = []
    for row in rows:
        if canonical_rule_type(row.get('rule_type')) not in {canonical}:
            # special compatibility: recovery rules can feed both scan and execute only if exact mapping is absent
            if canonical in {'recovery_scan', 'recovery_execute'} and canonical_rule_type(row.get('rule_type')) == 'recovery_scan' and canonical == 'recovery_execute':
                continue
            continue
        normalized = _normalize_rule(row)
        if not bool(normalized.get('enabled')):
            normalized['match'] = False
            normalized['match_reason'] = ['rule nonaktif']
            candidates.append(normalized)
            continue
        cooldown, remaining = _is_in_cooldown(normalized)
        if cooldown:
            normalized['match'] = False
            normalized['match_reason'] = [f'cooldown aktif {remaining}s']
            normalized['cooldown_remaining'] = remaining
            candidates.append(normalized)
            continue
        match, reasons = _condition_matches(normalized, stage_context)
        normalized['match'] = match
        normalized['match_reason'] = reasons if reasons else ['matched']
        candidates.append(normalized)
    matched = [r for r in candidates if r.get('match')]
    # Higher priority = lower numeric value. Merge low precedence first.
    matched_for_merge = sorted(matched, key=lambda r: (int(r.get('priority') or 100), int(r.get('id') or 0)), reverse=True)
    base = deepcopy(_STAGE_DEFAULT_CONFIG.get(canonical, {'action': {}, 'scope': {}, 'condition': {}}))
    action = dict(base.get('action') or {})
    scope = dict(base.get('scope') or {})
    condition = dict(base.get('condition') or {})
    for rule in matched_for_merge:
        action = _merge_dict(action, rule.get('action') or {})
        scope = _merge_dict(scope, rule.get('scope') or {})
        condition = _merge_dict(condition, rule.get('condition') or {})
    return {
        'stage': canonical,
        'context': stage_context,
        'matched_rules': sorted(matched, key=lambda r: (int(r.get('priority') or 100), int(r.get('id') or 0))),
        'all_rules': sorted(candidates, key=lambda r: (int(r.get('priority') or 100), int(r.get('id') or 0))),
        'effective_action': action,
        'effective_scope': scope,
        'effective_condition': condition,
        'enabled': bool(matched),
        'default_config': deepcopy(_STAGE_DEFAULT_CONFIG.get(canonical, {'action': {}, 'scope': {}, 'condition': {}})),
    }


def record_stage_result(stage: str, matched_rules: list[dict[str, Any]], ok: bool, result: dict[str, Any] | None = None):
    for rule in matched_rules:
        row = get_automation_rule(int(rule['id']))
        if not row:
            continue
        payload = {
            'last_triggered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        if ok:
            payload['success_count'] = int(row.get('success_count') or 0) + 1
        else:
            payload['fail_count'] = int(row.get('fail_count') or 0) + 1
        update_automation_rule(int(rule['id']), **payload)


def explain_rule(rule: dict[str, Any]) -> str:
    canonical = canonical_rule_type(rule.get('rule_type'))
    cond = _safe_json(rule.get('condition_json'), {})
    action = _safe_json(rule.get('action_json'), {})
    scope = _safe_json(rule.get('scope_json'), {})
    parts: list[str] = []
    if canonical == 'import':
        parts.append('Memindahkan hasil scrape yang sudah selesai ke tabel grup')
        if scope.get('job_status_in'):
            parts.append(f"untuk job berstatus {', '.join(scope['job_status_in'])}")
        if action.get('mode'):
            parts.append(f"dengan mode import {action['mode']}")
        if action.get('limit_jobs'):
            parts.append(f"maksimal {action['limit_jobs']} job per siklus")
    elif canonical == 'permission':
        parts.append('Memberikan izin otomatis pada grup baru yang belum punya permission')
        if action.get('permission_basis'):
            parts.append(f"basis izin {action['permission_basis']}")
        if scope.get('exclude_channels'):
            parts.append('channel dilewati')
        if action.get('limit'):
            parts.append(f"maksimal {action['limit']} grup per siklus")
    elif canonical == 'assignment':
        parts.append('Menentukan owner akun terbaik untuk grup yang sudah lolos permission')
        if action.get('prefer_joined_owner'):
            parts.append('mendahulukan akun yang memang sudah join grup')
        if action.get('create_recovery_on_no_candidate'):
            parts.append('membuat item recovery bila kandidat tidak ditemukan')
        if action.get('limit'):
            parts.append(f"maksimal {action['limit']} grup per siklus")
    elif canonical == 'campaign_prepare':
        parts.append('Mendorong grup managed ke antrian campaign broadcast')
        if action.get('reuse_active_campaign'):
            parts.append('menggunakan campaign aktif yang masih berjalan bila ada')
        if action.get('sender_source'):
            parts.append(f"sender diambil dari {action['sender_source']}")
        if action.get('limit'):
            parts.append(f"maksimal {action['limit']} target per siklus")
    elif canonical == 'delivery':
        parts.append('Mengirim broadcast ke target yang sudah masuk queue')
        if cond.get('active_draft_required') or action.get('require_active_draft'):
            parts.append('hanya berjalan bila draft aktif tersedia')
        if cond.get('online_accounts_gte') or action.get('require_online_sender'):
            parts.append('hanya memakai akun pengirim yang online')
        if action.get('retry_delay_minutes'):
            parts.append(f"gagal kirim akan dijadwalkan ulang {action['retry_delay_minutes']} menit")
    elif canonical == 'recovery_scan':
        parts.append('Mendeteksi job, assignment, atau campaign yang macet')
        if action.get('scrape_threshold_minutes'):
            parts.append(f"scrape macet bila lebih dari {action['scrape_threshold_minutes']} menit")
        if action.get('assignment_threshold_minutes'):
            parts.append(f"assignment macet bila lebih dari {action['assignment_threshold_minutes']} menit")
        if action.get('campaign_threshold_minutes'):
            parts.append(f"campaign macet bila lebih dari {action['campaign_threshold_minutes']} menit")
    elif canonical == 'recovery_execute':
        parts.append('Mencoba memulihkan item yang statusnya recoverable')
        if scope.get('entity_types'):
            parts.append(f"mencakup {', '.join(scope['entity_types'])}")
        if action.get('max_recovery_attempts'):
            parts.append(f"maksimal {action['max_recovery_attempts']} kali upaya per item")
    if 'pending_count_gte' in cond:
        parts.append(f"rule aktif bila ada minimal {cond['pending_count_gte']} item yang layak diproses")
    cooldown = int(rule.get('cooldown_seconds') or 0)
    if cooldown:
        parts.append(f"cooldown {cooldown} detik antar-trigger")
    return '. '.join(parts) + ('.' if parts else '')


def evaluate_rule(rule_id: int) -> dict[str, Any]:
    rule = get_normalized_rule(rule_id)
    if not rule:
        return {'matched': False, 'reason': 'rule_not_found'}
    stage = canonical_rule_type(rule.get('rule_type'))
    context = get_stage_context(stage)
    cooldown, remaining = _is_in_cooldown(rule)
    if cooldown:
        return {
            'matched': False,
            'canonical_stage': stage,
            'reason': f'cooldown aktif {remaining}s',
            'cooldown_remaining': remaining,
            'context': context,
            'effective_action': rule.get('action') or {},
            'effective_scope': rule.get('scope') or {},
            'explanation': rule.get('explanation') or '',
        }
    matched, reasons = _condition_matches(rule, context)
    return {
        'matched': matched,
        'canonical_stage': stage,
        'reason': 'matched' if matched else '; '.join(reasons),
        'context': context,
        'effective_action': rule.get('action') or {},
        'effective_scope': rule.get('scope') or {},
        'explanation': rule.get('explanation') or '',
    }


def get_rule_overview() -> dict[str, Any]:
    ensure_default_rules()
    raw_rows, _ = get_automation_rules(page=1, page_size=1000)
    rows = [_normalize_rule(row) for row in raw_rows]
    by_stage: dict[str, dict[str, int]] = {}
    for row in rows:
        stage = row.get('canonical_stage') or 'unknown'
        bucket = by_stage.setdefault(stage, {'total': 0, 'enabled': 0, 'disabled': 0})
        bucket['total'] += 1
        bucket['enabled' if row.get('enabled') else 'disabled'] += 1
    effective = {stage: resolve_stage_rules(stage, preloaded_rows=raw_rows, ensure_defaults=False) for stage in ['import', 'permission', 'assignment', 'campaign_prepare', 'delivery', 'recovery_scan', 'recovery_execute']}
    return {
        'by_stage': by_stage,
        'effective': {
            stage: {
                'enabled': data['enabled'],
                'matched_rule_ids': [int(r['id']) for r in data['matched_rules']],
                'effective_action': data['effective_action'],
                'effective_scope': data['effective_scope'],
                'context': data['context'],
            }
            for stage, data in effective.items()
        }
    }
