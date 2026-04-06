from utils.database import get_conn

_cache: dict = {}


def get(key: str, default=None):
    if key in _cache:
        return _cache[key]
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=%s", (key,)).fetchone()
    conn.close()
    if row:
        val = _convert(row["value"])
        _cache[key] = val
        return val
    return default


def get_int(key: str, default: int = 0) -> int:
    try:
        return int(get(key, default))
    except Exception:
        return default


def set(key: str, value, label: str | None = None, tipe: str = 'text'):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO settings (key, value, label, tipe)
        VALUES (%s, %s, COALESCE(%s, %s), %s)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, str(value), label, key, tipe),
    )
    conn.commit()
    conn.close()
    _cache.pop(key, None)


def get_semua() -> list:
    conn = get_conn()
    rows = conn.execute("SELECT key, value, label, tipe FROM settings ORDER BY key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_banyak(data: dict):
    conn = get_conn()
    for key, value in data.items():
        conn.execute(
            """
            INSERT INTO settings (key, value, label, tipe)
            VALUES (%s, %s, %s, 'text')
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, str(value), key),
        )
    conn.commit()
    conn.close()
    _cache.clear()


def get_warming_config(level: int) -> dict:
    return {
        "hari_min": get_int(f"w{level}_hari_min"),
        "hari_max": get_int(f"w{level}_hari_max"),
        "maks_join": get_int(f"w{level}_maks_join"),
        "maks_kirim": get_int(f"w{level}_maks_kirim"),
        "jeda_join": get_int(f"w{level}_jeda_join"),
        "jeda_kirim": get_int(f"w{level}_jeda_kirim"),
    }


def _convert(value: str):
    try:
        if "." in value:
            return float(value)
        return int(value)
    except Exception:
        if str(value).lower() in {"true", "false"}:
            return str(value).lower() == 'true'
        return value
