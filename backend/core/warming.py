from datetime import datetime, date
from utils.settings_manager import get_warming_config
from utils.database import get_conn

LABEL_LEVEL = {1:"🌱 Baru", 2:"📈 Berkembang", 3:"✅ Dewasa", 4:"⭐ Terpercaya"}

def hitung_umur_akun(tanggal_buat: str) -> int:
    if not tanggal_buat: return 0
    try:
        tgl = datetime.strptime(tanggal_buat[:10], "%Y-%m-%d").date()
        return (date.today() - tgl).days
    except: return 0

def tentukan_level(tanggal_buat: str) -> int:
    umur = hitung_umur_akun(tanggal_buat)
    for level in [1, 2, 3, 4]:
        cfg = get_warming_config(level)
        if cfg["hari_min"] <= umur <= cfg["hari_max"]:
            return level
    return 4

def get_batas_kirim(phone: str) -> int:
    conn = get_conn()
    row  = conn.execute("SELECT level_warming FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    level = row["level_warming"] if row else 1
    return get_warming_config(level)["maks_kirim"]

def get_batas_join(phone: str) -> int:
    conn = get_conn()
    row  = conn.execute("SELECT level_warming FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    level = row["level_warming"] if row else 1
    return get_warming_config(level)["maks_join"]

def get_jeda_kirim(phone: str) -> int:
    conn = get_conn()
    row  = conn.execute("SELECT level_warming FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    level = row["level_warming"] if row else 1
    return get_warming_config(level)["jeda_kirim"]

def get_jeda_join(phone: str) -> int:
    conn = get_conn()
    row  = conn.execute("SELECT level_warming FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    level = row["level_warming"] if row else 1
    return get_warming_config(level)["jeda_join"]

def update_level_otomatis(phone: str):
    conn = get_conn()
    row  = conn.execute("SELECT tanggal_buat, level_warming FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    if not row or not row["tanggal_buat"]: return
    level_baru = tentukan_level(row["tanggal_buat"])
    if level_baru != row["level_warming"]:
        conn = get_conn()
        conn.execute("UPDATE akun SET level_warming=%s WHERE phone=%s", (level_baru, phone))
        conn.commit()
        conn.close()

def get_info_warming(phone: str) -> dict:
    conn = get_conn()
    row  = conn.execute("SELECT tanggal_buat, level_warming FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    if not row: return {}
    level = row["level_warming"] or 1
    umur  = hitung_umur_akun(row["tanggal_buat"])
    cfg   = get_warming_config(level)
    return {
        "umur_hari"  : umur,
        "level"      : level,
        "label_level": LABEL_LEVEL.get(level, "Unknown"),
        "maks_kirim" : cfg["maks_kirim"],
        "maks_join"  : cfg["maks_join"],
        "jeda_kirim" : cfg["jeda_kirim"],
        "jeda_join"  : cfg["jeda_join"],
    }


def get_daily_capacity(phone: str) -> dict:
    """Single source of truth for daily join/send usage and limits.
    Akun soft_limit memakai kuota waspada yang lebih konservatif.
    """
    from utils.storage_db import hitung_kirim_hari_ini, hitung_join_hari_ini
    from utils.settings_manager import get_int as _gi
    from utils.database import get_conn as _gc

    try:
        conn = _gc()
        row = conn.execute(
            "SELECT last_error_code FROM akun WHERE phone=%s", (phone,)
        ).fetchone()
        conn.close()
        is_soft_limit = bool(row and str(row['last_error_code'] or '') == 'soft_limit')
    except Exception:
        is_soft_limit = False

    if is_soft_limit:
        maks_kirim = max(0, int(_gi('waspada_maks_kirim', 5) or 5))
        maks_join  = max(0, int(_gi('waspada_maks_join',  2) or 2))
    else:
        maks_kirim = max(0, int(get_batas_kirim(phone) or 0))
        maks_join  = max(0, int(get_batas_join(phone) or 0))
    sudah_kirim = max(0, int(hitung_kirim_hari_ini(phone) or 0))
    sudah_join = max(0, int(hitung_join_hari_ini(phone) or 0))
    return {
        "phone": phone,
        "is_soft_limit": is_soft_limit,
        "kirim": {
            "used": sudah_kirim,
            "limit": maks_kirim,
            "remaining": max(0, maks_kirim - sudah_kirim),
        },
        "join": {
            "used": sudah_join,
            "limit": maks_join,
            "remaining": max(0, maks_join - sudah_join),
        },
    }
