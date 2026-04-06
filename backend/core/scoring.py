from __future__ import annotations

from datetime import datetime, timedelta

from utils.database import get_conn
from utils.settings_manager import get, get_int
from core.warming import hitung_umur_akun


def _to_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _clamp(value: float, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(round(value))))


def _weighted(component_value: int, weight: int) -> int:
    return _clamp((component_value / 100.0) * max(0, weight), 0, max(0, weight))


def _row_value(row, key: str, default=0):
    try:
        value = row[key]
        return default if value is None else value
    except Exception:
        return default


def hitung_score_akun(phone: str) -> int:
    conn = get_conn()
    row = conn.execute('SELECT * FROM akun WHERE phone=%s', (phone,)).fetchone()
    if not row:
        conn.close()
        return 0

    bobot_umur = get_int('score_akun_bobot_umur', 25)
    bobot_kesehatan = get_int('score_akun_bobot_kesehatan', 45)
    bobot_performa = get_int('score_akun_bobot_performa', 30)

    umur = hitung_umur_akun(_row_value(row, 'tanggal_buat', '') or '')
    batas_baru = get_int('score_akun_batas_umur_baru_hari', 7)
    batas_berkembang = get_int('score_akun_batas_umur_berkembang_hari', 30)
    batas_matang = get_int('score_akun_batas_umur_matang_hari', 90)
    if umur <= batas_baru:
        nilai_umur = get_int('score_akun_nilai_umur_baru', 20)
    elif umur <= batas_berkembang:
        nilai_umur = get_int('score_akun_nilai_umur_berkembang', 50)
    elif umur <= batas_matang:
        nilai_umur = get_int('score_akun_nilai_umur_matang', 75)
    else:
        nilai_umur = get_int('score_akun_nilai_umur_lama', 90)

    total_banned = int(_row_value(row, 'total_banned', 0) or 0)
    total_flood = int(_row_value(row, 'total_flood', 0) or 0)
    health_score = int(_row_value(row, 'health_score', 100) or 100)
    kesehatan = health_score
    if total_banned > 0:
        if _to_bool(get('score_akun_banned_jadi_nol', 1), True):
            kesehatan = 0
        else:
            kesehatan = max(0, kesehatan - get_int('score_akun_penalti_banned', 100))
    elif total_flood >= 3:
        kesehatan = max(0, kesehatan - get_int('score_akun_penalti_flood_berat', 40))
    elif total_flood >= 2:
        kesehatan = max(0, kesehatan - get_int('score_akun_penalti_flood_sedang', 25))
    elif total_flood >= 1:
        kesehatan = max(0, kesehatan - get_int('score_akun_penalti_flood_ringan', 10))

    cooldown_until = str(_row_value(row, 'cooldown_until', '') or '').strip()
    if cooldown_until:
        try:
            dt = datetime.fromisoformat(cooldown_until.replace(' ', 'T'))
            if dt > datetime.now():
                kesehatan = max(0, kesehatan - get_int('score_akun_penalti_cooldown', 15))
        except Exception:
            pass

    last_error_code = str(_row_value(row, 'last_error_code', '') or '').lower()
    if last_error_code:
        kesehatan = max(0, kesehatan - get_int('score_akun_penalti_gagal_kirim_terbaru', 10))

    total_kirim = int(_row_value(row, 'total_kirim', 0) or 0)
    total_berhasil = int(_row_value(row, 'total_berhasil', 0) or 0)
    if total_kirim <= 0:
        performa = get_int('score_akun_nilai_awal_tanpa_riwayat', 55)
    else:
        rate = (total_berhasil / max(1, total_kirim)) * 100.0
        if rate >= get_int('score_akun_batas_performa_sangat_baik_persen', 90):
            performa = 95
        elif rate >= get_int('score_akun_batas_performa_baik_persen', 75):
            performa = 75
        elif rate >= get_int('score_akun_batas_performa_cukup_persen', 50):
            performa = 55
        else:
            performa = 30

    # Bonus kecil bila akun stabil / bersih
    if not last_error_code:
        kesehatan = min(100, kesehatan + get_int('score_akun_bonus_stabil', 5))
    if total_flood == 0 and total_banned == 0:
        kesehatan = min(100, kesehatan + get_int('score_akun_bonus_riwayat_bersih', 10))
    if str(_row_value(row, 'status', 'active')).lower() == 'active':
        kesehatan = min(100, kesehatan + get_int('score_akun_bonus_online', 5))

    score = (
        _weighted(_clamp(nilai_umur), bobot_umur) +
        _weighted(_clamp(kesehatan), bobot_kesehatan) +
        _weighted(_clamp(performa), bobot_performa)
    )
    return _clamp(score)


def get_label_akun(score: int) -> str:
    batas_terpercaya = get_int('score_akun_batas_terpercaya', get_int('score_akun_terpercaya', 80))
    batas_baik = get_int('score_akun_batas_baik', get_int('score_akun_baik', 60))
    batas_perhatian = get_int('score_akun_batas_perlu_perhatian', get_int('score_akun_perhatian', 40))
    if score >= batas_terpercaya:
        return '🟢 Terpercaya'
    if score >= batas_baik:
        return '🟡 Baik'
    if score >= batas_perhatian:
        return '🟠 Perlu Perhatian'
    return '🔴 Berisiko'


def update_score_akun(phone: str):
    score = hitung_score_akun(phone)
    conn = get_conn()
    conn.execute('UPDATE akun SET score=%s WHERE phone=%s', (score, phone))
    conn.commit()
    conn.close()
    return score


def hitung_score_grup(grup_id: int) -> int:
    conn = get_conn()
    row = conn.execute('SELECT * FROM grup WHERE id=%s', (grup_id,)).fetchone()
    if not row:
        conn.close()
        return 0

    bobot_ukuran = get_int('score_grup_bobot_ukuran', 35)
    bobot_riwayat = get_int('score_grup_bobot_riwayat', 30)
    bobot_aktivitas = get_int('score_grup_bobot_aktivitas', 20)
    bobot_akses = get_int('score_grup_bobot_akses', 15)

    member = int(_row_value(row, 'jumlah_member', 0) or 0)
    if member < get_int('score_grup_batas_sangat_kecil_member', 100):
        nilai_ukuran = get_int('score_grup_nilai_sangat_kecil', 20)
    elif member < get_int('score_grup_batas_kecil_member', 1000):
        nilai_ukuran = get_int('score_grup_nilai_kecil', 45)
    elif member < get_int('score_grup_batas_menengah_member', 5000):
        nilai_ukuran = get_int('score_grup_nilai_menengah', 65)
    elif member < get_int('score_grup_batas_besar_member', 10000):
        nilai_ukuran = get_int('score_grup_nilai_besar', 80)
    else:
        nilai_ukuran = get_int('score_grup_nilai_sangat_besar', 90)

    total_kirim = int(_row_value(row, 'total_kirim', 0) or 0)
    total_berhasil = int(_row_value(row, 'total_berhasil', 0) or 0)
    if total_kirim <= 0:
        nilai_riwayat = get_int('score_grup_nilai_awal_tanpa_riwayat', 50)
    else:
        rate = (total_berhasil / max(1, total_kirim)) * 100.0
        if rate >= get_int('score_grup_batas_riwayat_sangat_baik_persen', 90):
            nilai_riwayat = 95
        elif rate >= get_int('score_grup_batas_riwayat_baik_persen', 75):
            nilai_riwayat = 75
        elif rate >= get_int('score_grup_batas_riwayat_cukup_persen', 50):
            nilai_riwayat = 55
        else:
            nilai_riwayat = 30

    aktivitas = 70
    idle_days = int(_row_value(row, 'idle_days', 0) or 0)
    batas_aktif = get_int('score_grup_batas_aktif_hari', 7)
    if idle_days <= batas_aktif:
        aktivitas = min(100, aktivitas + get_int('score_grup_bonus_aktif', 10))
    else:
        aktivitas = max(0, aktivitas - get_int('score_grup_penalti_sepi', 15))

    akses = 70
    username = str(_row_value(row, 'username', '') or '').strip()
    tipe = str(_row_value(row, 'tipe', '') or '').lower()
    send_guard_status = str(_row_value(row, 'send_guard_status', '') or '').lower()
    if username:
        akses = min(100, akses + get_int('score_grup_bonus_publik', 10))
    else:
        akses = max(0, akses - get_int('score_grup_penalti_private', 15))
    if tipe == 'channel':
        akses = max(0, akses - get_int('score_grup_penalti_sulit_dijangkau', 10))
    if send_guard_status in {'hold_inactive', 'blocked', 'failed'}:
        akses = max(0, akses - get_int('score_grup_penalti_hold_berulang', 10))
    if str(_row_value(row, 'broadcast_hold_reason', '') or '').strip() == 'sender_missing':
        akses = max(0, akses - get_int('score_grup_penalti_sender_tidak_siap', 5))
    if str(_row_value(row, 'broadcast_status', '') or '').strip() == 'failed':
        nilai_riwayat = max(0, nilai_riwayat - get_int('score_grup_penalti_gagal_broadcast', 10))

    score = (
        _weighted(_clamp(nilai_ukuran), bobot_ukuran) +
        _weighted(_clamp(nilai_riwayat), bobot_riwayat) +
        _weighted(_clamp(aktivitas), bobot_aktivitas) +
        _weighted(_clamp(akses), bobot_akses)
    )
    conn.close()
    return _clamp(score)


def get_label_grup(score: int) -> str:
    batas_hot = get_int('score_grup_batas_hot', get_int('score_grup_hot', 80))
    batas_normal = get_int('score_grup_batas_normal', get_int('score_grup_normal', 55))
    batas_skip = get_int('score_grup_batas_skip', 35)
    if score >= batas_hot:
        return 'Hot'
    if score >= batas_normal:
        return 'Normal'
    if score >= batas_skip:
        return 'Perlu Ditinjau'
    return 'Skip'


def update_score_grup(grup_id: int):
    score = hitung_score_grup(grup_id)
    label = get_label_grup(score)
    conn = get_conn()
    conn.execute('UPDATE grup SET score=%s, label=%s WHERE id=%s', (score, label, grup_id))
    conn.commit()
    conn.close()
    return score, label


def update_semua_score_grup():
    conn = get_conn()
    ids = [r['id'] for r in conn.execute('SELECT id FROM grup').fetchall()]
    conn.close()
    for gid in ids:
        update_score_grup(gid)
