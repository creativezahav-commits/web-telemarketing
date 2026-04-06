from __future__ import annotations

"""Pusat registry setting.

File ini menyimpan default value, label, tipe, dan pengelompokan key agar:
- popup per fungsi bisa mengambil key yang konsisten
- backend bisa restore default per scope tanpa tumpang tindih
- key lama tetap hidup untuk kompatibilitas
"""

from collections import OrderedDict


def _rows(*items: tuple[str, str, str, str]) -> list[tuple[str, str, str, str]]:
    return list(items)


DEFAULT_SETTINGS: list[tuple[str, str, str, str]] = []

# ── Warming level akun (tetap dipakai engine lama) ───────────────────────────
DEFAULT_SETTINGS += _rows(
    ('w1_hari_min', '0', 'Level 1: Mulai hari ke', 'number'),
    ('w1_hari_max', '7', 'Level 1: Sampai hari ke', 'number'),
    ('w1_maks_join', '10', 'Level 1: Maks join/hari', 'number'),
    ('w1_maks_kirim', '20', 'Level 1: Maks kirim/hari', 'number'),
    ('w1_jeda_join', '15', 'Level 1: Jeda join (detik)', 'number'),
    ('w1_jeda_kirim', '5', 'Level 1: Jeda kirim (detik)', 'number'),
    ('w2_hari_min', '8', 'Level 2: Mulai hari ke', 'number'),
    ('w2_hari_max', '30', 'Level 2: Sampai hari ke', 'number'),
    ('w2_maks_join', '25', 'Level 2: Maks join/hari', 'number'),
    ('w2_maks_kirim', '25', 'Level 2: Maks kirim/hari', 'number'),
    ('w2_jeda_join', '12', 'Level 2: Jeda join (detik)', 'number'),
    ('w2_jeda_kirim', '5', 'Level 2: Jeda kirim (detik)', 'number'),
    ('w3_hari_min', '31', 'Level 3: Mulai hari ke', 'number'),
    ('w3_hari_max', '90', 'Level 3: Sampai hari ke', 'number'),
    ('w3_maks_join', '25', 'Level 3: Maks join/hari', 'number'),
    ('w3_maks_kirim', '30', 'Level 3: Maks kirim/hari', 'number'),
    ('w3_jeda_join', '4', 'Level 3: Jeda join (detik)', 'number'),
    ('w3_jeda_kirim', '4', 'Level 3: Jeda kirim (detik)', 'number'),
    ('w4_hari_min', '91', 'Level 4: Mulai hari ke', 'number'),
    ('w4_hari_max', '9999', 'Level 4: Sampai hari ke', 'number'),
    ('w4_maks_join', '30', 'Level 4: Maks join/hari', 'number'),
    ('w4_maks_kirim', '50', 'Level 4: Maks kirim/hari', 'number'),
    ('w4_jeda_join', '3', 'Level 4: Jeda join (detik)', 'number'),
    ('w4_jeda_kirim', '3', 'Level 4: Jeda kirim (detik)', 'number'),
    # Kuota khusus akun waspada (soft limit dari Telegram)
    ('waspada_maks_kirim',  '5',   'Akun Waspada: Maks kirim/hari', 'number'),
    ('waspada_maks_join',   '2',   'Akun Waspada: Maks join/hari',  'number'),
    ('waspada_jeda_kirim',  '90',  'Akun Waspada: Jeda kirim (detik)', 'number'),
    ('waspada_jeda_join',   '300', 'Akun Waspada: Jeda join (detik)',  'number'),
)

# ── Popup Penilaian Akun ────────────────────────────────────────────────────
DEFAULT_SETTINGS += _rows(
    ('score_akun_bobot_umur', '25', 'Penilaian Akun: Bobot umur', 'number'),
    ('score_akun_bobot_kesehatan', '45', 'Penilaian Akun: Bobot kesehatan', 'number'),
    ('score_akun_bobot_performa', '30', 'Penilaian Akun: Bobot performa', 'number'),
    ('score_akun_batas_terpercaya', '80', 'Penilaian Akun: Batas Terpercaya', 'number'),
    ('score_akun_batas_baik', '60', 'Penilaian Akun: Batas Baik', 'number'),
    ('score_akun_batas_perlu_perhatian', '40', 'Penilaian Akun: Batas Perlu Perhatian', 'number'),
    ('score_akun_batas_umur_baru_hari', '7', 'Penilaian Akun: Batas umur baru (hari)', 'number'),
    ('score_akun_batas_umur_berkembang_hari', '30', 'Penilaian Akun: Batas umur berkembang (hari)', 'number'),
    ('score_akun_batas_umur_matang_hari', '90', 'Penilaian Akun: Batas umur matang (hari)', 'number'),
    ('score_akun_nilai_umur_baru', '20', 'Penilaian Akun: Nilai umur baru', 'number'),
    ('score_akun_nilai_umur_berkembang', '50', 'Penilaian Akun: Nilai umur berkembang', 'number'),
    ('score_akun_nilai_umur_matang', '75', 'Penilaian Akun: Nilai umur matang', 'number'),
    ('score_akun_nilai_umur_lama', '90', 'Penilaian Akun: Nilai umur lama', 'number'),
    ('score_akun_penalti_flood_ringan', '10', 'Penilaian Akun: Penalti flood ringan', 'number'),
    ('score_akun_penalti_flood_sedang', '25', 'Penilaian Akun: Penalti flood sedang', 'number'),
    ('score_akun_penalti_flood_berat', '40', 'Penilaian Akun: Penalti flood berat', 'number'),
    ('score_akun_penalti_cooldown', '15', 'Penilaian Akun: Penalti cooldown', 'number'),
    ('score_akun_penalti_gagal_kirim_terbaru', '10', 'Penilaian Akun: Penalti gagal kirim terbaru', 'number'),
    ('score_akun_banned_jadi_nol', '1', 'Penilaian Akun: Akun banned jadi nol', 'boolean'),
    ('score_akun_penalti_banned', '100', 'Penilaian Akun: Penalti banned', 'number'),
    ('score_akun_batas_performa_sangat_baik_persen', '90', 'Penilaian Akun: Batas performa sangat baik', 'number'),
    ('score_akun_batas_performa_baik_persen', '75', 'Penilaian Akun: Batas performa baik', 'number'),
    ('score_akun_batas_performa_cukup_persen', '50', 'Penilaian Akun: Batas performa cukup', 'number'),
    ('score_akun_nilai_awal_tanpa_riwayat', '55', 'Penilaian Akun: Nilai awal tanpa riwayat', 'number'),
    ('score_akun_bonus_online', '5', 'Penilaian Akun: Bonus akun online', 'number'),
    ('score_akun_bonus_stabil', '5', 'Penilaian Akun: Bonus akun stabil', 'number'),
    ('score_akun_bonus_riwayat_bersih', '10', 'Penilaian Akun: Bonus riwayat bersih', 'number'),
)

# Key label lama tetap ada untuk kompatibilitas score akun lama
DEFAULT_SETTINGS += _rows(
    ('score_akun_terpercaya', '80', 'LEGACY: Score Akun batas terpercaya', 'number'),
    ('score_akun_baik', '60', 'LEGACY: Score Akun batas baik', 'number'),
    ('score_akun_perhatian', '40', 'LEGACY: Score Akun batas perhatian', 'number'),
)

# ── Popup Penilaian Grup ────────────────────────────────────────────────────
DEFAULT_SETTINGS += _rows(
    ('score_grup_bobot_ukuran', '35', 'Penilaian Grup: Bobot ukuran', 'number'),
    ('score_grup_bobot_riwayat', '30', 'Penilaian Grup: Bobot riwayat', 'number'),
    ('score_grup_bobot_aktivitas', '20', 'Penilaian Grup: Bobot aktivitas', 'number'),
    ('score_grup_bobot_akses', '15', 'Penilaian Grup: Bobot akses', 'number'),
    ('score_grup_batas_hot', '80', 'Penilaian Grup: Batas Hot', 'number'),
    ('score_grup_batas_normal', '55', 'Penilaian Grup: Batas Normal', 'number'),
    ('score_grup_batas_skip', '35', 'Penilaian Grup: Batas Skip', 'number'),
    ('score_grup_batas_sangat_kecil_member', '100', 'Penilaian Grup: Batas sangat kecil', 'number'),
    ('score_grup_batas_kecil_member', '1000', 'Penilaian Grup: Batas kecil', 'number'),
    ('score_grup_batas_menengah_member', '5000', 'Penilaian Grup: Batas menengah', 'number'),
    ('score_grup_batas_besar_member', '10000', 'Penilaian Grup: Batas besar', 'number'),
    ('score_grup_nilai_sangat_kecil', '20', 'Penilaian Grup: Nilai sangat kecil', 'number'),
    ('score_grup_nilai_kecil', '45', 'Penilaian Grup: Nilai kecil', 'number'),
    ('score_grup_nilai_menengah', '65', 'Penilaian Grup: Nilai menengah', 'number'),
    ('score_grup_nilai_besar', '80', 'Penilaian Grup: Nilai besar', 'number'),
    ('score_grup_nilai_sangat_besar', '90', 'Penilaian Grup: Nilai sangat besar', 'number'),
    ('score_grup_batas_riwayat_sangat_baik_persen', '90', 'Penilaian Grup: Batas riwayat sangat baik', 'number'),
    ('score_grup_batas_riwayat_baik_persen', '75', 'Penilaian Grup: Batas riwayat baik', 'number'),
    ('score_grup_batas_riwayat_cukup_persen', '50', 'Penilaian Grup: Batas riwayat cukup', 'number'),
    ('score_grup_nilai_awal_tanpa_riwayat', '50', 'Penilaian Grup: Nilai awal tanpa riwayat', 'number'),
    ('score_grup_batas_aktif_hari', '7', 'Penilaian Grup: Batas aktif (hari)', 'number'),
    ('score_grup_bonus_aktif', '10', 'Penilaian Grup: Bonus aktif', 'number'),
    ('score_grup_penalti_sepi', '15', 'Penilaian Grup: Penalti sepi', 'number'),
    ('score_grup_bonus_publik', '10', 'Penilaian Grup: Bonus publik', 'number'),
    ('score_grup_penalti_private', '15', 'Penilaian Grup: Penalti private', 'number'),
    ('score_grup_penalti_sulit_dijangkau', '10', 'Penilaian Grup: Penalti sulit dijangkau', 'number'),
    ('score_grup_penalti_gagal_broadcast', '10', 'Penilaian Grup: Penalti gagal broadcast', 'number'),
    ('score_grup_penalti_hold_berulang', '10', 'Penilaian Grup: Penalti hold berulang', 'number'),
    ('score_grup_penalti_sender_tidak_siap', '5', 'Penilaian Grup: Penalti sender tidak siap', 'number'),
)

# Key lama score grup tetap ada untuk kompatibilitas
DEFAULT_SETTINGS += _rows(
    ('score_grup_bobot_member', '50', 'LEGACY: Score Grup bobot member', 'number'),
    ('score_grup_hot', '70', 'LEGACY: Score Grup batas Hot', 'number'),
    ('score_grup_normal', '30', 'LEGACY: Score Grup batas Normal', 'number'),
)

# ── Scraper ─────────────────────────────────────────────────────────────────
DEFAULT_SETTINGS += _rows(
    ('scraper_limit_per_keyword', '30', 'Scraper: hasil per keyword', 'number'),
    ('scraper_min_members', '0', 'Scraper: minimum member', 'number'),
    ('scraper_recommended_score', '30', 'Scraper: ambang rekomendasi', 'number'),
    ('scraper_max_terms', '80', 'Scraper: maksimal query', 'number'),
    ('scraper_delay_keyword_min', '30', 'Scraper: jeda min antar keyword (detik)', 'number'),
    ('scraper_delay_keyword_max', '60', 'Scraper: jeda max antar keyword (detik)', 'number'),
)

# ── Toggle tahap otomasi ───────────────────────────────────────────────────
DEFAULT_SETTINGS += _rows(
    ('auto_import_enabled', '0', 'Automation: auto import aktif', 'boolean'),
    ('auto_permission_enabled', '0', 'Automation: auto permission aktif', 'boolean'),
    ('auto_assign_enabled', '0', 'Automation: auto assign aktif', 'boolean'),
    ('auto_campaign_enabled', '0', 'Automation: auto broadcast aktif', 'boolean'),
    ('auto_recovery_enabled', '1', 'Automation: auto recovery aktif', 'boolean'),
    ('auto_join_enabled', '0', 'Automation: auto join aktif', 'boolean'),
)

# ── Auto Join ───────────────────────────────────────────────────────────────
DEFAULT_SETTINGS += _rows(
    ('auto_join_public_only', '1', 'Auto Join: hanya grup public', 'boolean'),
    ('auto_join_max_per_cycle', '1', 'Auto Join: maks join per siklus', 'number'),
    ('auto_join_reserve_quota', '2', 'Auto Join: sisakan kuota join harian', 'number'),
)

# ── Popup Auto Broadcast (key baru yang dipakai popup) ─────────────────────
DEFAULT_SETTINGS += _rows(
    ('broadcast_enabled', '0', 'Auto Broadcast: aktif', 'boolean'),
    ('broadcast_jeda_kirim_min_detik', '3', 'Auto Broadcast: jeda kirim minimum (detik)', 'number'),
    ('broadcast_jeda_kirim_max_detik', '7', 'Auto Broadcast: jeda kirim maksimum (detik)', 'number'),
    ('broadcast_masa_tunggu_setelah_assign_menit', '0', 'Auto Broadcast: masa tunggu setelah assign (menit)', 'number'),
    ('broadcast_cooldown_grup_jam', '24', 'Auto Broadcast: cooldown grup (jam)', 'number'),
    ('broadcast_cooldown_grup_menit', '0', 'Auto Broadcast: cooldown grup (menit)', 'number'),
    ('broadcast_tahan_grup_sepi', '1', 'Auto Broadcast: tahan grup sepi', 'boolean'),
    ('broadcast_batas_grup_sepi_hari', '14', 'Auto Broadcast: batas grup sepi (hari)', 'number'),
    ('broadcast_tahan_jika_chat_terakhir_milik_sendiri', '1', 'Auto Broadcast: tahan jika chat terakhir milik sendiri', 'boolean'),
    ('broadcast_retry_gagal_enabled', '1', 'Auto Broadcast: retry gagal aktif', 'boolean'),
    ('broadcast_retry_delay_detik', '60', 'Auto Broadcast: jeda retry gagal (detik)', 'number'),
    ('broadcast_requeue_jika_sender_tidak_siap', '1', 'Auto Broadcast: antrekan ulang jika sender tidak siap', 'boolean'),
    ('broadcast_target_per_sesi', '200', 'Auto Broadcast: target grup per sesi', 'number'),
    ('broadcast_target_per_akun_per_sesi', '20', 'Auto Broadcast: target grup per akun per sesi', 'number'),
    ('broadcast_izinkan_grup_baru_masuk_sesi_berjalan', '1', 'Auto Broadcast: izinkan grup baru masuk sesi berjalan', 'boolean'),
    ('broadcast_batch_delivery', '40', 'Auto Broadcast: batch delivery per putaran', 'number'),
    ('broadcast_hanya_pakai_draft_aktif', '1', 'Auto Broadcast: hanya pakai draft aktif', 'boolean'),
    # Pengaturan throttle broadcast — jeda otomatis berbasis kuota
    ('broadcast_jam_mulai', '6', 'Auto Broadcast: jam mulai (0-23)', 'number'),
    ('broadcast_jam_selesai', '22', 'Auto Broadcast: jam selesai (0-23)', 'number'),
    ('broadcast_jeda_min_menit', '1', 'Auto Broadcast: jeda minimum antar kirim (menit)', 'number'),
    ('broadcast_jeda_max_menit', '10', 'Auto Broadcast: jeda maksimum antar kirim (menit)', 'number'),
    ('broadcast_throttle_enabled', '1', 'Auto Broadcast: throttle otomatis aktif', 'boolean'),
)

# Key legacy broadcast & campaign tetap ada untuk engine lama
DEFAULT_SETTINGS += _rows(
    ('broadcast_jeda_min', '3', 'LEGACY: Broadcast jeda minimum (detik)', 'number'),
    ('broadcast_jeda_max', '7', 'LEGACY: Broadcast jeda maksimum (detik)', 'number'),
    ('broadcast_jadwal_ulang', '3', 'LEGACY: Broadcast jadwal ulang (hari)', 'number'),
    ('broadcast_jadwal_aktif', '1', 'LEGACY: Broadcast jadwal ulang aktif', 'number'),
    ('campaign_retry_delay_minutes', '1', 'LEGACY: delay retry gagal (menit)', 'number'),
    ('campaign_skip_inactive_groups_enabled', '1', 'LEGACY: tahan grup tidak aktif', 'boolean'),
    ('campaign_inactive_threshold_days', '14', 'LEGACY: batas grup tidak aktif (hari)', 'number'),
    ('campaign_skip_if_last_chat_is_ours', '1', 'LEGACY: tahan jika chat terakhir milik kita', 'boolean'),
    ('assignment_broadcast_delay_minutes', '15', 'LEGACY: tahan grup baru assigned sebelum broadcast (menit)', 'number'),
    ('campaign_session_target_limit', '200', 'LEGACY: maksimum target per sesi', 'number'),
    ('campaign_session_per_sender_limit', '20', 'LEGACY: maksimum target per akun per siklus', 'number'),
    ('campaign_allow_mid_session_enqueue', '1', 'LEGACY: izinkan grup baru masuk sesi berjalan', 'boolean'),
    ('campaign_group_cooldown_hours', '24', 'LEGACY: cooldown grup setelah kirim berhasil (jam)', 'number'),
    ('campaign_group_cooldown_minutes', '0', 'LEGACY: cooldown grup setelah kirim berhasil (menit)', 'number'),
    ('campaign_requeue_sender_missing', '1', 'LEGACY: antrekan ulang bila sender tidak siap', 'boolean'),
    ('campaign_valid_permission_required', '1', 'LEGACY: valid permission required', 'boolean'),
    ('campaign_managed_required', '0', 'LEGACY: managed status required', 'boolean'),
    ('campaign_default_sender_pool', 'utama', 'LEGACY: default sender pool', 'text'),
    ('campaign_retry_policy', 'retry_once', 'LEGACY: retry policy', 'text'),
)

# ── Popup Mode Otomasi Penuh (key baru yang dipakai popup) ─────────────────
DEFAULT_SETTINGS += _rows(
    ('pipeline_enabled', '1', 'Mode Otomasi Penuh: aktif', 'boolean'),
    ('pipeline_pause_semua', '0', 'Mode Otomasi Penuh: pause semua', 'boolean'),
    ('pipeline_maintenance_mode', '0', 'Mode Otomasi Penuh: maintenance mode', 'boolean'),
    ('pipeline_interval_detik', '10', 'Mode Otomasi Penuh: interval kerja (detik)', 'number'),
    ('pipeline_batch_import', '200', 'Mode Otomasi Penuh: batch import', 'number'),
    ('pipeline_batch_permission', '200', 'Mode Otomasi Penuh: batch permission', 'number'),
    ('pipeline_batch_assign', '150', 'Mode Otomasi Penuh: batch assign', 'number'),
    ('pipeline_batch_campaign', '150', 'Mode Otomasi Penuh: batch campaign', 'number'),
    ('pipeline_batch_delivery', '40', 'Mode Otomasi Penuh: batch delivery', 'number'),
    ('pipeline_batch_recovery', '30', 'Mode Otomasi Penuh: batch recovery', 'number'),
    ('pipeline_wajib_permission_valid', '1', 'Mode Otomasi Penuh: wajib permission valid', 'boolean'),
    ('pipeline_wajib_status_managed_untuk_broadcast', '1', 'Mode Otomasi Penuh: wajib status managed untuk broadcast', 'boolean'),
    ('pipeline_sender_pool_default', 'utama', 'Mode Otomasi Penuh: sender pool default', 'text'),
    ('pipeline_retry_umum_enabled', '1', 'Mode Otomasi Penuh: retry umum aktif', 'boolean'),
    ('pipeline_retry_maks_per_item', '2', 'Mode Otomasi Penuh: retry maksimal per item', 'number'),
    ('pipeline_retry_jeda_detik', '30', 'Mode Otomasi Penuh: jeda retry umum (detik)', 'number'),
    ('pipeline_lanjutkan_proses_setelah_restart', '1', 'Mode Otomasi Penuh: lanjutkan proses setelah restart', 'boolean'),
    ('pipeline_tandai_proses_setengah_jalan', '1', 'Mode Otomasi Penuh: tandai proses setengah jalan', 'boolean'),
)

# Key legacy pipeline / system tetap ada untuk engine lama
DEFAULT_SETTINGS += _rows(
    ('maintenance_mode', '0', 'LEGACY: maintenance mode', 'boolean'),
    ('pause_all_automation', '0', 'LEGACY: pause all automation', 'boolean'),
    ('orchestrator_interval_seconds', '10', 'LEGACY: interval orchestrator (detik)', 'number'),
    ('orchestrator_import_batch', '200', 'LEGACY: batch import orchestrator', 'number'),
    ('orchestrator_permission_batch', '200', 'LEGACY: batch permission orchestrator', 'number'),
    ('orchestrator_assign_batch', '150', 'LEGACY: batch assignment orchestrator', 'number'),
    ('orchestrator_campaign_batch', '150', 'LEGACY: batch campaign orchestrator', 'number'),
    ('orchestrator_delivery_batch', '40', 'LEGACY: batch delivery orchestrator', 'number'),
    ('orchestrator_recovery_batch', '30', 'LEGACY: batch recovery orchestrator', 'number'),
)

# ── Result / Permission / Assignment / Recovery ────────────────────────────
DEFAULT_SETTINGS += _rows(
    ('result_min_quality_score', '20', 'Result Rules: minimum quality score', 'number'),
    ('result_username_required', '0', 'Result Rules: username wajib', 'boolean'),
    ('result_allowed_entity_types', 'group,supergroup', 'Result Rules: allowed entity types', 'text'),
    ('permission_min_score', '0', 'Permission Rules: minimum score grup', 'number'),
    ('permission_require_username', '0', 'Permission Rules: username wajib', 'boolean'),
    ('permission_exclude_channels', '1', 'Permission Rules: abaikan channel', 'boolean'),
    ('assignment_min_health_score', '0', 'Assignment Rules: minimum health score', 'number'),
    ('assignment_min_warming_level', '1', 'Assignment Rules: minimum warming level', 'number'),
    ('assignment_retry_count', '3', 'Assignment Rules: retry count', 'number'),
    ('assignment_reassign_count', '2', 'Assignment Rules: reassign count', 'number'),
    ('recovery_resume_on_restart', '1', 'Recovery Rules: resume on restart', 'boolean'),
    ('recovery_mark_partial_if_worker_missing', '1', 'Recovery Rules: mark partial if worker missing', 'boolean'),
    ('recovery_stuck_scrape_threshold', '30', 'Recovery Rules: stuck scrape threshold (menit)', 'number'),
    ('recovery_stuck_assignment_threshold', '30', 'Recovery Rules: stuck assignment threshold (menit)', 'number'),
    ('recovery_stuck_campaign_threshold', '30', 'Recovery Rules: stuck campaign threshold (menit)', 'number'),
)

# Hindari duplikasi key bila file ini diubah lagi di masa depan
_seen: set[str] = set()
_deduped: list[tuple[str, str, str, str]] = []
for row in DEFAULT_SETTINGS:
    if row[0] in _seen:
        continue
    _seen.add(row[0])
    _deduped.append(row)
DEFAULT_SETTINGS = _deduped

DEFAULT_SETTINGS_MAP = {
    key: {'value': value, 'label': label, 'tipe': tipe}
    for key, value, label, tipe in DEFAULT_SETTINGS
}

SETTINGS_SCOPE_KEYS: dict[str, set[str]] = OrderedDict({
    'automation': {
        'auto_import_enabled', 'auto_permission_enabled', 'auto_assign_enabled', 'auto_join_enabled', 'auto_campaign_enabled', 'auto_recovery_enabled',
    },
    'scraper-defaults': {
        'scraper_limit_per_keyword', 'scraper_min_members', 'scraper_recommended_score', 'scraper_max_terms',
        'scraper_delay_keyword_min', 'scraper_delay_keyword_max',
    },
    'result-rules': {
        'result_min_quality_score', 'result_username_required', 'result_allowed_entity_types',
    },
    'permission-rules': {
        'permission_min_score', 'permission_require_username', 'permission_exclude_channels',
    },
    'assignment-rules': {
        'assignment_min_health_score', 'assignment_min_warming_level', 'assignment_retry_count', 'assignment_reassign_count',
        'w1_hari_min', 'w1_hari_max', 'w1_maks_join', 'w1_maks_kirim', 'w1_jeda_join', 'w1_jeda_kirim',
        'w2_hari_min', 'w2_hari_max', 'w2_maks_join', 'w2_maks_kirim', 'w2_jeda_join', 'w2_jeda_kirim',
        'w3_hari_min', 'w3_hari_max', 'w3_maks_join', 'w3_maks_kirim', 'w3_jeda_join', 'w3_jeda_kirim',
        'w4_hari_min', 'w4_hari_max', 'w4_maks_join', 'w4_maks_kirim', 'w4_jeda_join', 'w4_jeda_kirim',
    },
    'score-akun': {key for key, *_ in DEFAULT_SETTINGS if key.startswith('score_akun_') and not key.startswith('score_akun_terpercaya') and key not in {'score_akun_baik', 'score_akun_perhatian'}},
    'score-grup': {key for key, *_ in DEFAULT_SETTINGS if key.startswith('score_grup_') and key not in {'score_grup_bobot_member', 'score_grup_hot', 'score_grup_normal'}},
    'broadcast-rules': {
        'broadcast_enabled', 'broadcast_jeda_kirim_min_detik', 'broadcast_jeda_kirim_max_detik', 'broadcast_masa_tunggu_setelah_assign_menit',
        'broadcast_cooldown_grup_jam', 'broadcast_cooldown_grup_menit', 'broadcast_tahan_grup_sepi', 'broadcast_batas_grup_sepi_hari',
        'broadcast_tahan_jika_chat_terakhir_milik_sendiri', 'broadcast_retry_gagal_enabled', 'broadcast_retry_delay_detik',
        'broadcast_requeue_jika_sender_tidak_siap', 'broadcast_target_per_sesi', 'broadcast_target_per_akun_per_sesi',
        'broadcast_izinkan_grup_baru_masuk_sesi_berjalan', 'broadcast_batch_delivery', 'broadcast_hanya_pakai_draft_aktif',
        'broadcast_jam_mulai', 'broadcast_jam_selesai', 'broadcast_jeda_min_menit', 'broadcast_jeda_max_menit', 'broadcast_throttle_enabled',
    },
    'pipeline-rules': {
        'pipeline_enabled', 'pipeline_pause_semua', 'pipeline_maintenance_mode', 'pipeline_interval_detik', 'pipeline_batch_import',
        'pipeline_batch_permission', 'pipeline_batch_assign', 'pipeline_batch_campaign', 'pipeline_batch_delivery', 'pipeline_batch_recovery',
        'pipeline_wajib_permission_valid', 'pipeline_wajib_status_managed_untuk_broadcast', 'pipeline_sender_pool_default',
        'pipeline_retry_umum_enabled', 'pipeline_retry_maks_per_item', 'pipeline_retry_jeda_detik',
        'pipeline_lanjutkan_proses_setelah_restart', 'pipeline_tandai_proses_setengah_jalan',
    },
    'recovery-rules': {
        'recovery_resume_on_restart', 'recovery_mark_partial_if_worker_missing', 'recovery_stuck_scrape_threshold',
        'recovery_stuck_assignment_threshold', 'recovery_stuck_campaign_threshold',
    },
})


def defaults_for_scope(scope: str | None = None) -> dict[str, str]:
    if not scope or scope == 'all':
        return {key: value for key, value, _label, _tipe in DEFAULT_SETTINGS}
    allowed = SETTINGS_SCOPE_KEYS.get(scope)
    if not allowed:
        return {}
    return {key: value for key, value, _label, _tipe in DEFAULT_SETTINGS if key in allowed}
