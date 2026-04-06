from __future__ import annotations

from datetime import datetime, timedelta
from utils.database import get_conn


# ── AKUN ──────────────────────────────────────────────────

def simpan_akun(phone, nama, username, tanggal_buat=None):
    conn = get_conn()
    # PENTING: jangan timpa status 'banned' saat login ulang
    # Akun banned tetap tidak bisa broadcast, tapi bisa scraping
    conn.execute(
        """
        INSERT INTO akun (phone, nama, username, tanggal_buat, status, last_login_at, last_activity_at)
        VALUES (%s, %s, %s, %s, 'active', TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
        ON CONFLICT(phone) DO UPDATE SET
            nama=excluded.nama,
            username=excluded.username,
            tanggal_buat=COALESCE(akun.tanggal_buat, excluded.tanggal_buat),
            status=CASE WHEN akun.status IN ('banned','suspended') THEN akun.status ELSE 'active' END,
            last_login_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            last_activity_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        """,
        (phone, nama, username or "-", tanggal_buat),
    )
    conn.commit()
    conn.close()


def get_semua_akun():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM akun ORDER BY dibuat DESC, phone ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_status_akun(phone):
    conn = get_conn()
    row = conn.execute("SELECT status FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    return row["status"] if row else "active"


def set_status_akun(phone, status):
    conn = get_conn()
    conn.execute("UPDATE akun SET status=%s WHERE phone=%s", (status, phone))
    conn.commit()
    conn.close()


def tandai_akun_banned(phone: str) -> bool:
    conn = get_conn()
    cur = conn.execute(
        "UPDATE akun SET status='banned', auto_send_enabled=0 WHERE phone=%s AND status != 'banned'",
        (phone,)
    )
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def get_next_join_at(phone):
    conn = get_conn()
    row = conn.execute("SELECT next_join_at FROM akun WHERE phone=%s", (phone,)).fetchone()
    conn.close()
    return row["next_join_at"] if row else None

def set_next_join_at(phone, next_join_at):
    conn = get_conn()
    conn.execute("UPDATE akun SET next_join_at=%s WHERE phone=%s", (next_join_at, phone))
    conn.commit()
    conn.close()


def set_join_throttle(phone: str, nama_grup: str, jeda_detik: float):
    """
    Dipanggil setelah akun berhasil JOIN grup.
    Menulis: waktu join, nama grup, dan kapan boleh join lagi.
    """
    now = datetime.now()
    last_join_at = now.strftime("%Y-%m-%d %H:%M:%S")
    next_join_at = (now + timedelta(seconds=float(jeda_detik))).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute(
        """
        UPDATE akun SET
            last_join_at    = %s,
            last_join_group = %s,
            next_join_at    = %s
        WHERE phone = %s
        """,
        (last_join_at, nama_grup, next_join_at, phone),
    )
    conn.commit()
    conn.close()


def set_broadcast_throttle(phone: str, nama_grup: str, jeda_detik: float):
    """
    Dipanggil setelah akun berhasil KIRIM pesan.
    Menulis: waktu kirim, nama grup, dan kapan boleh kirim lagi.
    """
    now = datetime.now()
    last_broadcast_at = now.strftime("%Y-%m-%d %H:%M:%S")
    next_broadcast_at = (now + timedelta(seconds=float(jeda_detik))).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute(
        """
        UPDATE akun SET
            last_broadcast_at    = %s,
            last_broadcast_group = %s,
            next_broadcast_at    = %s
        WHERE phone = %s
        """,
        (last_broadcast_at, nama_grup, next_broadcast_at, phone),
    )
    conn.commit()
    conn.close()


def delete_akun(phone: str):
    conn = get_conn()
    conn.execute("DELETE FROM akun WHERE phone=%s", (phone,))
    conn.commit()
    conn.close()


def set_level_warming(phone, level):
    conn = get_conn()
    conn.execute("UPDATE akun SET level_warming=%s WHERE phone=%s", (level, phone))
    conn.commit()
    conn.close()


def set_score_akun(phone, score):
    conn = get_conn()
    conn.execute("UPDATE akun SET score=%s WHERE phone=%s", (score, phone))
    conn.commit()
    conn.close()


# ── RELASI AKUN-GRUP ──────────────────────────────────────

def simpan_relasi_akun_grup(phone: str, grup_ids: list[int]):
    """Tambah relasi akun dengan grup tanpa menghapus relasi lama."""
    if not grup_ids:
        return
    conn = get_conn()
    conn.executemany(
        "INSERT INTO akun_grup (phone, grup_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        [(phone, gid) for gid in grup_ids],
    )
    conn.commit()
    conn.close()


def sinkronkan_relasi_akun_grup(phone: str, grup_ids: list[int]):
    """Jadikan relasi akun→grup persis sama dengan hasil fetch terbaru."""
    conn = get_conn()
    if grup_ids:
        placeholders = ",".join("%s" for _ in grup_ids)
        conn.execute(
            f"DELETE FROM akun_grup WHERE phone=%s AND grup_id NOT IN ({placeholders})",
            (phone, *grup_ids),
        )
        conn.executemany(
            "INSERT INTO akun_grup (phone, grup_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            [(phone, gid) for gid in grup_ids],
        )
    else:
        conn.execute("DELETE FROM akun_grup WHERE phone=%s", (phone,))
    conn.commit()
    conn.close()


def get_grup_by_akun(phone: str) -> list:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT g.* FROM grup g
        JOIN akun_grup ag ON g.id = ag.grup_id
        WHERE ag.phone = %s AND g.status = 'active'
        ORDER BY g.score DESC, g.nama ASC
        """,
        (phone,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_akun_by_grup(grup_id: int) -> list:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT a.phone, a.nama, a.score FROM akun a
        JOIN akun_grup ag ON a.phone = ag.phone
        WHERE ag.grup_id = %s
        ORDER BY a.score DESC, a.phone ASC
        """,
        (grup_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_akun_terbaik_per_grup(grup_id: int) -> str | None:
    rows = get_akun_by_grup(grup_id)
    return rows[0]["phone"] if rows else None


def hapus_relasi_akun(phone: str):
    conn = get_conn()
    conn.execute("DELETE FROM akun_grup WHERE phone=%s", (phone,))
    conn.commit()
    conn.close()


# ── GRUP ──────────────────────────────────────────────────

def simpan_banyak_grup(daftar, sumber="fetch"):
    if not daftar:
        return
    conn = get_conn()
    payload = [
        (
            g["id"],
            g["nama"],
            g.get("username"),
            g.get("tipe"),
            g.get("jumlah_member", 0) or 0,
            g.get("link"),
            g.get("sumber", sumber),
        )
        for g in daftar
        if g and g.get("id")
    ]
    conn.executemany(
        """
        INSERT INTO grup (id, nama, username, tipe, jumlah_member, link, sumber)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET
            nama=excluded.nama,
            username=excluded.username,
            tipe=excluded.tipe,
            jumlah_member=excluded.jumlah_member,
            link=excluded.link,
            diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        """,
        payload,
    )
    conn.commit()
    conn.close()


def get_semua_grup():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM grup ORDER BY score DESC, nama ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_grup_aktif():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM grup WHERE status='active' ORDER BY score DESC, nama ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_grup_hot():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM grup WHERE label='Hot' AND status='active' ORDER BY score DESC, nama ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_status_grup(grup_id, status):
    conn = get_conn()
    conn.execute("UPDATE grup SET status=%s WHERE id=%s", (status, grup_id))
    conn.commit()
    conn.close()


def set_status_grup_massal(grup_ids: list, status: str):
    if not grup_ids:
        return
    conn = get_conn()
    conn.executemany("UPDATE grup SET status=%s WHERE id=%s", [(status, gid) for gid in grup_ids])
    conn.commit()
    conn.close()


def set_score_grup(grup_id, score, label):
    conn = get_conn()
    conn.execute("UPDATE grup SET score=%s, label=%s WHERE id=%s", (score, label, grup_id))
    conn.commit()
    conn.close()


def update_last_kirim_grup(grup_id):
    conn = get_conn()
    waktu = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        UPDATE grup SET
            last_kirim     = %s,
            total_kirim    = total_kirim + 1,
            total_berhasil = total_berhasil + 1,
            diupdate       = TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        WHERE id=%s
        """,
        (waktu, grup_id),
    )
    conn.commit()
    conn.close()


def resolve_broadcast_cooldown(cooldown_hours: int | None = None, cooldown_minutes: int | None = None) -> dict:
    from utils.settings_manager import get_int as _get_int

    minutes = cooldown_minutes if cooldown_minutes is not None else _get_int('broadcast_cooldown_grup_menit', _get_int('campaign_group_cooldown_minutes', 0))
    hours = cooldown_hours if cooldown_hours is not None else _get_int('broadcast_cooldown_grup_jam', _get_int('campaign_group_cooldown_hours', 0))
    try:
        minutes = max(0, int(minutes or 0))
    except Exception:
        minutes = 0
    try:
        hours = max(0, int(hours or 0))
    except Exception:
        hours = 0
    if minutes <= 0 and hours <= 0:
        minutes = 1
    if minutes > 0:
        modifier = f'+{minutes} minutes'
        ready_at = datetime.now() + timedelta(minutes=minutes)
    else:
        modifier = f'+{hours} hours'
        ready_at = datetime.now() + timedelta(hours=hours)
    return {
        'hours': hours,
        'minutes': minutes,
        'modifier': modifier,
        'ready_at': ready_at.strftime('%Y-%m-%d %H:%M:%S'),
    }


def tandai_grup_masa_istirahat(grup_id: int, cooldown_hours: int | None = None, cooldown_minutes: int | None = None, reason: str = 'cooldown_after_send'):
    """Samakan status grup setelah kirim sukses agar card Auto Broadcast membaca sumber yang sama."""
    spec = resolve_broadcast_cooldown(cooldown_hours=cooldown_hours, cooldown_minutes=cooldown_minutes)
    conn = get_conn()
    conn.execute(
        """
        UPDATE grup SET
            broadcast_status='cooldown',
            broadcast_hold_reason=%s,
            broadcast_ready_at=%s,
            diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        WHERE id=%s
        """,
        (reason, spec['ready_at'], grup_id),
    )
    # Catat ke riwayat agar dashboard Auto Broadcast bisa hitung "mulai istirahat"
    conn.execute(
        """
        INSERT INTO riwayat (grup_id, status, waktu)
        VALUES (%s, 'cooldown_started', TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
        """,
        (grup_id,),
    )
    conn.commit()
    conn.close()
    return spec


def update_last_chat_grup(grup_id, waktu_chat):
    conn = get_conn()
    conn.execute("UPDATE grup SET last_chat=%s WHERE id=%s", (waktu_chat, grup_id))
    conn.commit()
    conn.close()


def update_indikator_aktif(grup_id, indikator):
    conn = get_conn()
    conn.execute("UPDATE grup SET aktif_indikator=%s WHERE id=%s", (indikator, grup_id))
    conn.commit()
    conn.close()


def grup_sudah_ada(grup_id):
    conn = get_conn()
    row = conn.execute("SELECT id FROM grup WHERE id=%s", (grup_id,)).fetchone()
    conn.close()
    return row is not None


def hapus_grup(grup_id):
    conn = get_conn()
    conn.execute("DELETE FROM grup WHERE id=%s", (grup_id,))
    conn.execute("DELETE FROM akun_grup WHERE grup_id=%s", (grup_id,))
    conn.commit()
    conn.close()


# ── DRAFT ─────────────────────────────────────────────────

def simpan_draft(judul, isi):
    conn = get_conn()
    cur = conn.execute("INSERT INTO draft (judul, isi) VALUES (%s, %s)", (judul, isi))
    did = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM draft WHERE id=%s", (did,)).fetchone()
    conn.close()
    return dict(row)


def get_semua_draft():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM draft ORDER BY aktif DESC, id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_draft_aktif():
    conn = get_conn()
    row = conn.execute("SELECT * FROM draft WHERE aktif=1").fetchone()
    conn.close()
    return dict(row) if row else None


def set_draft_aktif(draft_id):
    conn = get_conn()
    conn.execute("UPDATE draft SET aktif=0")
    conn.execute("UPDATE draft SET aktif=1 WHERE id=%s", (draft_id,))
    conn.commit()
    conn.close()


def hapus_draft(draft_id):
    conn = get_conn()
    conn.execute("DELETE FROM draft WHERE id=%s", (draft_id,))
    conn.commit()
    conn.close()


# ── ANTRIAN ───────────────────────────────────────────────

def tambah_antrian(phone, grup_id, pesan):
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO antrian (phone, grup_id, pesan) VALUES (%s, %s, %s)",
        (phone, grup_id, pesan),
    )
    iid = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM antrian WHERE id=%s", (iid,)).fetchone()
    conn.close()
    return dict(row)


def get_semua_antrian():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM antrian ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_status_antrian(item_id, status):
    conn = get_conn()
    conn.execute(
        "UPDATE antrian SET status=%s, dikirim=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') WHERE id=%s",
        (status, item_id),
    )
    conn.commit()
    conn.close()


def hapus_antrian(item_id):
    conn = get_conn()
    conn.execute("DELETE FROM antrian WHERE id=%s", (item_id,))
    conn.commit()
    conn.close()


# ── RIWAYAT ───────────────────────────────────────────────

def catat_riwayat(phone, grup_id, nama_grup, status, pesan_error=None):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO riwayat (phone, grup_id, nama_grup, status, pesan_error)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (phone, grup_id, nama_grup, status, pesan_error),
    )
    conn.commit()
    conn.close()


def get_riwayat_hari_ini():
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM riwayat WHERE waktu LIKE %s ORDER BY id DESC",
        (hari + "%",),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_ringkasan_hari_ini():
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('berhasil','send_success') THEN 1 ELSE 0 END) as berhasil,
            SUM(CASE WHEN status='cooldown_started' THEN 1 ELSE 0 END) as cooldown_started,
            SUM(CASE WHEN status IN ('gagal','send_failed','join_failed') THEN 1 ELSE 0 END) as gagal,
            SUM(CASE WHEN status='skip'     THEN 1 ELSE 0 END) as skip,
            SUM(CASE WHEN status IN ('join','join_success') THEN 1 ELSE 0 END) as join_grup,
            COUNT(*) as total
        FROM riwayat WHERE waktu LIKE %s
        """,
        (hari + "%",),
    ).fetchone()
    conn.close()
    return {
        "berhasil": row["berhasil"] or 0,
        "gagal": row["gagal"] or 0,
        "cooldown_started": row["cooldown_started"] or 0,
        "skip": row["skip"] or 0,
        "total": row["total"] or 0,
    }


def sudah_dikirim_hari_ini(grup_id):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT COUNT(*) as n FROM riwayat
        WHERE grup_id=%s AND status IN ('berhasil','send_success') AND waktu LIKE %s
        """,
        (grup_id, hari + "%"),
    ).fetchone()
    conn.close()
    return row["n"] > 0


def hitung_kirim_hari_ini(phone):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT COUNT(*) as n FROM riwayat
        WHERE phone=%s AND status IN ('berhasil','send_success') AND waktu LIKE %s
        """,
        (phone, hari + "%"),
    ).fetchone()
    conn.close()
    return row["n"] or 0


def hitung_join_hari_ini(phone):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT COUNT(*) as n FROM riwayat
        WHERE phone=%s AND status IN ('join','join_success') AND waktu LIKE %s
        """,
        (phone, hari + "%"),
    ).fetchone()
    conn.close()
    return row["n"] or 0


def get_auto_join_summary() -> dict:
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('join','join_success') THEN 1 ELSE 0 END) AS joined_today,
            SUM(CASE WHEN status='join_failed' THEN 1 ELSE 0 END) AS failed_today
        FROM riwayat WHERE waktu LIKE %s
        """,
        (hari + "%",),
    ).fetchone()
    waiting = conn.execute(
        """
        SELECT COUNT(*) as n FROM grup g
        WHERE g.assignment_status='assigned' AND g.owner_phone IS NOT NULL AND g.status='active'
          AND NOT EXISTS (
            SELECT 1 FROM akun_grup ag
            WHERE ag.phone=g.owner_phone AND ag.grup_id=g.id
          )
        """
    ).fetchone()
    conn.close()
    return {
        'joined_today': int((row['joined_today'] if row else 0) or 0),
        'failed_today': int((row['failed_today'] if row else 0) or 0),
        'waiting': int((waiting['n'] if waiting else 0) or 0),
    }


def get_history_by_status_today(statuses: list[str], limit: int = 20) -> list[dict]:
    clean = [str(s).strip() for s in (statuses or []) if str(s).strip()]
    if not clean:
        return []
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    placeholders = ','.join('%s' for _ in clean)
    rows = conn.execute(
        f"SELECT * FROM riwayat WHERE waktu LIKE %s AND status IN ({placeholders}) ORDER BY id DESC LIMIT %s",
        (hari + '%', *clean, int(limit or 20)),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── SCRAPER ───────────────────────────────────────────────

def get_existing_group_lookup() -> tuple[set[int], set[str]]:
    conn = get_conn()
    rows = conn.execute("SELECT id, username FROM grup").fetchall()
    conn.close()
    ids = {int(r["id"]) for r in rows}
    usernames = {(r["username"] or "").lower() for r in rows if r["username"]}
    return ids, usernames


def create_scrape_job(phone: str, keywords_text: str, total_keywords: int, options: dict | None = None) -> int:
    conn = get_conn()
    cur = conn.execute(
        """
        INSERT INTO scrape_job (phone, keywords_text, options_json, total_keywords, status)
        VALUES (%s, %s, %s, %s, 'queued')
        """,
        (phone, keywords_text, __import__('json').dumps(options or {}, ensure_ascii=False), int(total_keywords or 0)),
    )
    job_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(job_id)


def update_scrape_job(
    job_id: int,
    *,
    status: str | None = None,
    total_keywords: int | None = None,
    processed_keywords: int | None = None,
    total_found: int | None = None,
    total_saved: int | None = None,
    total_imported: int | None = None,
    error_message: str | None = None,
    selesai: bool = False,
):
    conn = get_conn()
    fields = []
    values = []
    mapping = {
        "status": status,
        "total_keywords": total_keywords,
        "processed_keywords": processed_keywords,
        "total_found": total_found,
        "total_saved": total_saved,
        "total_imported": total_imported,
        "error_message": error_message,
    }
    for key, value in mapping.items():
        if value is not None:
            fields.append(f"{key}=%s")
            values.append(value)
    if selesai:
        fields.append("selesai=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    if fields:
        values.append(job_id)
        conn.execute(f"UPDATE scrape_job SET {', '.join(fields)} WHERE id=%s", values)
        conn.commit()
    conn.close()



def finish_scrape_job(job_id: int, **updates):
    update_scrape_job(job_id, status=updates.pop("status", "done"), selesai=True, **updates)



def get_scrape_job(job_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM scrape_job WHERE id=%s", (job_id,)).fetchone()
    conn.close()
    return dict(row) if row else None



def get_scrape_jobs(limit: int = 20) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scrape_job ORDER BY id DESC LIMIT %s",
        (int(limit or 20),),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]



def save_scrape_results(job_id: int, rows: list[dict]):
    if not rows:
        return
    conn = get_conn()
    payload = [
        (
            job_id,
            r["grup_id"],
            r.get("nama"),
            r.get("username"),
            r.get("tipe"),
            int(r.get("jumlah_member") or 0),
            r.get("link"),
            r.get("deskripsi"),
            r.get("sumber_keyword"),
            int(r.get("relevance_score") or 0),
            int(r.get("recommended") or 0),
            int(r.get("already_in_db") or 0),
            int(r.get("imported") or 0),
            r.get("catatan"),
            r.get("metadata"),
        )
        for r in rows
    ]
    conn.executemany(
        """
        INSERT INTO scrape_result (
            job_id, grup_id, nama, username, tipe, jumlah_member, link, deskripsi,
            sumber_keyword, relevance_score, recommended, already_in_db, imported,
            catatan, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(job_id, grup_id) DO UPDATE SET
            nama=excluded.nama,
            username=excluded.username,
            tipe=excluded.tipe,
            jumlah_member=excluded.jumlah_member,
            link=excluded.link,
            deskripsi=excluded.deskripsi,
            sumber_keyword=excluded.sumber_keyword,
            relevance_score=excluded.relevance_score,
            recommended=excluded.recommended,
            already_in_db=excluded.already_in_db,
            catatan=excluded.catatan,
            metadata=excluded.metadata
        """,
        payload,
    )
    conn.commit()
    conn.close()



def get_scrape_results(
    job_id: int,
    *,
    only_recommended: bool = False,
    only_new: bool = False,
    include_imported: bool = True,
) -> list[dict]:
    conn = get_conn()
    clauses = ["job_id=%s"]
    values = [job_id]
    if only_recommended:
        clauses.append("recommended=1")
    if only_new:
        clauses.append("already_in_db=0")
    if not include_imported:
        clauses.append("imported=0")
    sql = f"SELECT * FROM scrape_result WHERE {' AND '.join(clauses)} ORDER BY relevance_score DESC, jumlah_member DESC, nama ASC"
    rows = conn.execute(sql, values).fetchall()
    conn.close()
    return [dict(r) for r in rows]



def mark_scrape_results_imported(result_ids: list[int]):
    if not result_ids:
        return
    conn = get_conn()
    conn.executemany("UPDATE scrape_result SET imported=1, already_in_db=1 WHERE id=%s", [(int(rid),) for rid in result_ids])
    conn.commit()
    conn.close()


def set_scrape_job_status(job_id: int, status: str):
    conn = get_conn()
    conn.execute("UPDATE scrape_job SET status=%s WHERE id=%s", (status, job_id))
    conn.commit()
    conn.close()


def create_scrape_keyword_runs(job_id: int, keywords: list):
    if not keywords:
        return
    conn = get_conn()
    rows = []
    for item in keywords:
        if isinstance(item, dict):
            rows.append(
                (
                    int(job_id),
                    str(item.get('keyword') or '').strip(),
                    str(item.get('source') or 'base'),
                    int(item.get('priority') or 50),
                    str(item.get('tier') or 'medium'),
                    int(item.get('max_attempts') or 2),
                )
            )
        else:
            rows.append((int(job_id), str(item).strip(), 'base', 50, 'medium', 2))
    rows = [r for r in rows if r[1]]
    conn.executemany(
        """INSERT INTO scrape_keyword_run
        (job_id, keyword, status, source, priority, tier, max_attempts)
        VALUES (%s, %s, 'pending', %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
        rows,
    )
    conn.commit()
    conn.close()


def get_scrape_keyword_runs(job_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scrape_keyword_run WHERE job_id=%s ORDER BY COALESCE(priority, 50) ASC, id ASC",
        (int(job_id),),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_scrape_keyword_run(
    run_id: int,
    *,
    status: str | None = None,
    found_count: int | None = None,
    saved_count: int | None = None,
    error_message: str | None = None,
    source: str | None = None,
    priority: int | None = None,
    tier: str | None = None,
    attempt_count: int | None = None,
    max_attempts: int | None = None,
    quality_score: int | None = None,
    last_error_code: str | None = None,
    started: bool = False,
    finished: bool = False,
):
    conn = get_conn()
    fields = []
    values = []
    mapping = {
        "status": status,
        "found_count": found_count,
        "saved_count": saved_count,
        "error_message": error_message,
        "source": source,
        "priority": priority,
        "tier": tier,
        "attempt_count": attempt_count,
        "max_attempts": max_attempts,
        "quality_score": quality_score,
        "last_error_code": last_error_code,
    }
    for key, value in mapping.items():
        if value is not None:
            fields.append(f"{key}=%s")
            values.append(value)
    if started:
        fields.append("started_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    if finished:
        fields.append("finished_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    if fields:
        values.append(int(run_id))
        conn.execute(f"UPDATE scrape_keyword_run SET {', '.join(fields)} WHERE id=%s", values)
        conn.commit()
    conn.close()


# ── PERMISSIONS ───────────────────────────────────────────

def get_permissions(
    search: str = '',
    status: str | None = None,
    basis: str | None = None,
    expiring_soon: bool = False,
    approved_by: str | None = None,
    *,
    page: int = 1,
    page_size: int = 25,
):
    conn = get_conn()
    clauses = ['1=1']
    values: list = []
    if search:
        clauses.append("LOWER(g.nama) LIKE %s")
        values.append(f"%{search.lower()}%")
    if status:
        clauses.append("gp.status=%s")
        values.append(status)
    if basis:
        clauses.append("gp.permission_basis=%s")
        values.append(basis)
    if approved_by:
        clauses.append("gp.approved_by=%s")
        values.append(approved_by)
    if expiring_soon:
        clauses.append("gp.expires_at IS NOT NULL AND gp.expires_at <= (NOW() + INTERVAL '7 days')")
    total = conn.execute(
        f"SELECT COUNT(*) FROM group_permission gp JOIN grup g ON g.id=gp.group_id WHERE {' AND '.join(clauses)}",
        values,
    ).fetchone()[0]
    rows = conn.execute(
        f"""
        SELECT gp.*, g.nama as group_name, g.username
        FROM group_permission gp
        JOIN grup g ON g.id = gp.group_id
        WHERE {' AND '.join(clauses)}
        ORDER BY gp.id DESC LIMIT %s OFFSET %s
        """,
        (*values, page_size, (page - 1) * page_size),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)


def get_permission_summary() -> dict:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status='valid' THEN 1 ELSE 0 END) AS valid_count,
          SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending_count,
          SUM(CASE WHEN status='expired' THEN 1 ELSE 0 END) AS expired_count,
          SUM(CASE WHEN status='revoked' THEN 1 ELSE 0 END) AS revoked_count,
          (SELECT COUNT(*) FROM grup WHERE COALESCE(permission_status,'unknown')='unknown') AS missing_permission_count
        FROM group_permission
        """
    ).fetchone()
    conn.close()
    return {k: int((row[k] if row else 0) or 0) for k in row.keys()} if row else {
        'valid_count': 0, 'pending_count': 0, 'expired_count': 0, 'revoked_count': 0, 'missing_permission_count': 0
    }


def get_permission(permission_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT gp.*, g.nama as group_name, g.username
        FROM group_permission gp
        JOIN grup g ON g.id = gp.group_id
        WHERE gp.id=%s
        """,
        (permission_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def create_permission(group_id: int, permission_basis: str, approval_source: str | None, approved_by: str | None, approved_at: str | None, expires_at: str | None = None, notes: str | None = None, status: str = 'valid') -> int:
    conn = get_conn()
    cur = conn.execute(
        """
        INSERT INTO group_permission (group_id, permission_basis, approval_source, approved_by, approved_at, expires_at, status, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (group_id, permission_basis, approval_source, approved_by, approved_at, expires_at, status, notes),
    )
    conn.execute(
        "UPDATE grup SET permission_status=%s, permission_basis=%s, approved_by=%s, approved_at=%s, permission_expires_at=%s WHERE id=%s",
        (status, permission_basis, approved_by, approved_at, expires_at, group_id),
    )
    conn.commit()
    pid = cur.lastrowid
    conn.close()
    return int(pid)


def update_permission(permission_id: int, **fields):
    conn = get_conn()
    allowed = {'permission_basis', 'approval_source', 'approved_by', 'approved_at', 'expires_at', 'status', 'notes'}
    parts = []
    values = []
    for key, value in fields.items():
        if key in allowed:
            parts.append(f"{key}=%s")
            values.append(value)
    if not parts:
        conn.close()
        return
    parts.append("updated_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    values.append(permission_id)
    conn.execute(f"UPDATE group_permission SET {', '.join(parts)} WHERE id=%s", values)
    row = conn.execute("SELECT * FROM group_permission WHERE id=%s", (permission_id,)).fetchone()
    if row:
        conn.execute(
            "UPDATE grup SET permission_status=%s, permission_basis=%s, approved_by=%s, approved_at=%s, permission_expires_at=%s WHERE id=%s",
            (row['status'], row['permission_basis'], row['approved_by'], row['approved_at'], row['expires_at'], row['group_id']),
        )
    conn.commit()
    conn.close()


# ── ASSIGNMENTS ───────────────────────────────────────────

def get_assignments(search: str = '', status: str | None = None, pool: str | None = None, assignment_type: str | None = None, retry_due: bool = False, *, page: int = 1, page_size: int = 25):
    conn = get_conn()
    clauses = ['1=1']
    values: list = []
    if search:
        clauses.append("(LOWER(g.nama) LIKE %s OR LOWER(COALESCE(a.nama, ga.assigned_account_id)) LIKE %s)")
        values.extend([f"%{search.lower()}%", f"%{search.lower()}%"])
    if status:
        clauses.append("ga.status=%s")
        values.append(status)
    if pool:
        clauses.append("COALESCE(a.pool,'default')=%s")
        values.append(pool)
    if assignment_type:
        clauses.append("ga.assignment_type=%s")
        values.append(assignment_type)
    if retry_due:
        clauses.append("ga.next_retry_at IS NOT NULL AND ga.next_retry_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    total = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM group_assignment ga
        JOIN grup g ON g.id=ga.group_id
        LEFT JOIN akun a ON a.phone=ga.assigned_account_id
        WHERE {' AND '.join(clauses)}
        """,
        values,
    ).fetchone()[0]
    rows = conn.execute(
        f"""
        SELECT ga.*, g.nama as group_name, g.username, COALESCE(a.nama, ga.assigned_account_id) as owner_name, COALESCE(a.pool,'default') as pool
        FROM group_assignment ga
        JOIN grup g ON g.id=ga.group_id
        LEFT JOIN akun a ON a.phone=ga.assigned_account_id
        WHERE {' AND '.join(clauses)}
        ORDER BY ga.id DESC LIMIT %s OFFSET %s
        """,
        (*values, page_size, (page - 1) * page_size),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)


def get_assignment_summary() -> dict:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN COALESCE(assignment_status,'ready_assign')='ready_assign' THEN 1 ELSE 0 END) AS ready_assign_count,
          SUM(CASE WHEN COALESCE(assignment_status,'')='assigned' THEN 1 ELSE 0 END) AS assigned_count,
          SUM(CASE WHEN COALESCE(assignment_status,'')='managed' THEN 1 ELSE 0 END) AS managed_count,
          SUM(CASE WHEN COALESCE(assignment_status,'')='retry_wait' THEN 1 ELSE 0 END) AS retry_wait_count,
          SUM(CASE WHEN COALESCE(assignment_status,'')='reassign_pending' THEN 1 ELSE 0 END) AS reassign_pending_count,
          SUM(CASE WHEN COALESCE(assignment_status,'')='failed' THEN 1 ELSE 0 END) AS failed_count,
          SUM(CASE WHEN COALESCE(assignment_status,'ready_assign')='ready_assign' AND status='active' THEN 1 ELSE 0 END) AS unassigned_count
        FROM grup
        """
    ).fetchone()
    conn.close()
    return {k: int((row[k] if row else 0) or 0) for k in row.keys()} if row else {}


def get_assignment(assignment_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT ga.*, g.nama as group_name, g.username, COALESCE(a.nama, ga.assigned_account_id) as owner_name, COALESCE(a.pool,'default') as pool
        FROM group_assignment ga
        JOIN grup g ON g.id=ga.group_id
        LEFT JOIN akun a ON a.phone=ga.assigned_account_id
        WHERE ga.id=%s
        """,
        (assignment_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def create_assignment(group_id: int, account_id: str, assignment_type: str = 'sync_owner', status: str = 'assigned', priority_level: int = 100, assign_reason: str | None = None, assign_score_snapshot: str | None = None) -> int:
    conn = get_conn()
    cur = conn.execute(
        """
        INSERT INTO group_assignment (group_id, assigned_account_id, assignment_type, status, priority_level, assign_reason, assign_score_snapshot, assigned_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
        """,
        (group_id, account_id, assignment_type, status, priority_level, assign_reason, assign_score_snapshot),
    )
    conn.execute(
        "UPDATE grup SET owner_phone=%s, assignment_status=%s, diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') WHERE id=%s",
        (account_id, status, group_id),
    )
    conn.commit()
    aid = cur.lastrowid
    conn.close()
    return int(aid)


def update_assignment(assignment_id: int, **fields):
    conn = get_conn()
    allowed = {'assigned_account_id','assignment_type','status','priority_level','assign_reason','assign_score_snapshot','retry_count','max_retry','reassign_count','last_attempt_at','next_retry_at','failure_reason','released_at'}
    parts = []
    values = []
    for key, value in fields.items():
        if key in allowed:
            parts.append(f"{key}=%s")
            values.append(value)
    if not parts:
        conn.close(); return
    parts.append("updated_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    values.append(assignment_id)
    conn.execute(f"UPDATE group_assignment SET {', '.join(parts)} WHERE id=%s", values)
    row = conn.execute("SELECT * FROM group_assignment WHERE id=%s", (assignment_id,)).fetchone()
    if row:
        conn.execute("UPDATE grup SET owner_phone=%s, assignment_status=%s, diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') WHERE id=%s", (row['assigned_account_id'], row['status'], row['group_id']))
    conn.commit()
    conn.close()


def get_assignment_candidates(group_id: int) -> list[dict]:
    from utils.settings_manager import get_int

    min_health_score = int(get_int('assignment_min_health_score', 50) or 0)
    min_warming_level = int(get_int('assignment_min_warming_level', 1) or 0)

    conn = get_conn()
    rows = conn.execute(
        """
        SELECT phone as account_id, COALESCE(nama, phone) as account_name, COALESCE(role,'hybrid') as role,
               COALESCE(pool,'default') as pool, COALESCE(status,'active') as status,
               COALESCE(health_score,100) as health_score, COALESCE(level_warming,1) as warming_level,
               COALESCE(priority_weight,100) as priority_weight, COALESCE(daily_new_group_cap,10) as daily_new_group_cap,
               COALESCE(cooldown_until,'') as cooldown_until,
               COALESCE(last_login_at,'') as last_login_at,
               COALESCE(manual_health_override_enabled,0) as manual_health_override_enabled,
               COALESCE(manual_health_override_score,80) as manual_health_override_score,
               COALESCE(manual_warming_override_enabled,0) as manual_warming_override_enabled,
               COALESCE(manual_warming_override_level,2) as manual_warming_override_level,
               COALESCE(fresh_login_grace_enabled,1) as fresh_login_grace_enabled,
               COALESCE(fresh_login_grace_minutes,180) as fresh_login_grace_minutes,
               COALESCE(fresh_login_health_floor,80) as fresh_login_health_floor,
               COALESCE(fresh_login_warming_floor,2) as fresh_login_warming_floor,
               COALESCE(assignment_notes,'') as assignment_notes,
               (SELECT COUNT(*) FROM group_assignment ga WHERE ga.assigned_account_id=akun.phone AND ga.status IN ('assigned','in_progress','managed','retry_wait')) as active_assignment_count,
               COALESCE((SELECT COUNT(*) FROM grup g WHERE g.owner_phone=akun.phone AND g.assignment_status IN ('assigned','managed')), 0) as total_assigned,
               COALESCE((SELECT COUNT(*) FROM akun_grup ag WHERE ag.phone=akun.phone), 0) as total_joined
        FROM akun
        WHERE COALESCE(status,'active') IN ('active','online') AND COALESCE(auto_assign_enabled,1)=1
        ORDER BY COALESCE(priority_weight,100) DESC, COALESCE(health_score,100) DESC, COALESCE(level_warming,1) DESC
        LIMIT 50
        """
    ).fetchall()
    conn.close()
    now = datetime.now()
    out = []
    for r in rows:
        d = dict(r)
        actual_health = int(d.get('health_score') or 0)
        actual_warming = int(d.get('warming_level') or 0)
        effective_health = actual_health
        effective_warming = actual_warming
        adjustment_reasons = []

        if int(d.get('manual_health_override_enabled') or 0):
            override_score = int(d.get('manual_health_override_score') or 0)
            if override_score > effective_health:
                effective_health = override_score
                adjustment_reasons.append(f'manual_health_override:{override_score}')

        if int(d.get('manual_warming_override_enabled') or 0):
            override_level = int(d.get('manual_warming_override_level') or 0)
            if override_level > effective_warming:
                effective_warming = override_level
                adjustment_reasons.append(f'manual_warming_override:{override_level}')

        last_login_at = str(d.get('last_login_at') or '').strip()
        grace_active = False
        grace_remaining_minutes = 0
        if int(d.get('fresh_login_grace_enabled') or 0) and last_login_at:
            grace_minutes = int(d.get('fresh_login_grace_minutes') or 0)
            if grace_minutes > 0:
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                    try:
                        login_at = datetime.strptime(last_login_at, fmt)
                        age_minutes = max(0, int((now - login_at).total_seconds() // 60))
                        if age_minutes <= grace_minutes:
                            grace_active = True
                            grace_remaining_minutes = max(0, grace_minutes - age_minutes)
                            health_floor = int(d.get('fresh_login_health_floor') or 0)
                            warming_floor = int(d.get('fresh_login_warming_floor') or 0)
                            if health_floor > effective_health:
                                effective_health = health_floor
                            if warming_floor > effective_warming:
                                effective_warming = warming_floor
                            adjustment_reasons.append(f'fresh_login_grace:{grace_remaining_minutes}m')
                        break
                    except ValueError:
                        continue

        d['actual_health_score'] = actual_health
        d['actual_warming_level'] = actual_warming
        d['effective_health_score'] = effective_health
        d['effective_warming_level'] = effective_warming
        d['grace_active'] = grace_active
        d['grace_remaining_minutes'] = grace_remaining_minutes
        d['adjustment_reasons'] = adjustment_reasons

        if effective_health < min_health_score:
            continue
        if effective_warming < min_warming_level:
            continue
        cooldown_until = str(d.get('cooldown_until') or '').strip()
        if cooldown_until:
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                try:
                    if datetime.strptime(cooldown_until, fmt) > now:
                        d['skip_reason'] = 'cooldown_active'
                        break
                except ValueError:
                    continue
            if d.get('skip_reason') == 'cooldown_active':
                continue
        active_assignment_count = int(d.get('active_assignment_count') or 0)
        daily_cap = int(d.get('daily_new_group_cap') or 0)
        if daily_cap > 0 and active_assignment_count >= daily_cap:
            continue
        # Cek selisih assigned vs sudah join — jangan assign kalau terlalu banyak menunggu join
        total_assigned = int(d.get('total_assigned') or 0)
        total_joined = int(d.get('total_joined') or 0)
        selisih_menunggu = max(0, total_assigned - total_joined)
        # Batas selisih = maks_join akun (Level 1=20, Level 2=25, Level 3=30, Level 4=50)
        from core.warming import get_daily_capacity as _get_cap
        _cap_join = _get_cap(str(d.get('account_id', ''))).get('join', {})
        maks_join_akun = max(5, int(_cap_join.get('limit') or 20))
        if selisih_menunggu >= maks_join_akun:
            d['skip_reason'] = 'selisih_join_penuh'
            continue
        d['selisih_menunggu'] = selisih_menunggu
        d['maks_selisih_join'] = maks_join_akun
        load_penalty = active_assignment_count * 5
        d['ranking_score'] = int(d['priority_weight']) + int(effective_health) + (int(effective_warming) * 10) - load_penalty
        d['recommendation'] = 'recommended' if d['ranking_score'] >= 150 else 'consider'
        out.append(d)
    out.sort(key=lambda x: x['ranking_score'], reverse=True)
    return out[:25]


# ── CAMPAIGNS / DELIVERY ──────────────────────────────────

def get_campaigns(search: str = '', status: str | None = None, sender_pool: str | None = None, *, page: int = 1, page_size: int = 25):
    conn = get_conn()
    clauses = ['1=1']
    values: list = []
    if search:
        clauses.append('LOWER(name) LIKE %s')
        values.append(f'%{search.lower()}%')
    if status:
        clauses.append('status=%s')
        values.append(status)
    if sender_pool:
        clauses.append('sender_pool=%s')
        values.append(sender_pool)
    total = conn.execute(f"SELECT COUNT(*) FROM campaign WHERE {' AND '.join(clauses)}", values).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM campaign WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT %s OFFSET %s",
        (*values, page_size, (page - 1) * page_size),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)


def get_campaign_summary() -> dict:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status='draft' THEN 1 ELSE 0 END) AS draft_count,
          SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued_count,
          SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_count,
          SUM(CASE WHEN status='paused' THEN 1 ELSE 0 END) AS paused_count,
          SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_count,
          SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) AS partial_count,
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
          SUM(CASE WHEN status='stopped' THEN 1 ELSE 0 END) AS stopped_count
        FROM campaign
        """
    ).fetchone()
    conn.close()
    return {k: int((row[k] if row else 0) or 0) for k in row.keys()} if row else {}


def get_campaign(campaign_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute('SELECT * FROM campaign WHERE id=%s', (campaign_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_campaign(name: str, template_id: int | None = None, sender_pool: str = 'default', target_mode: str = 'rule_based', auto_start_enabled: int = 0, required_permission_status: str = 'valid', required_group_status: str = 'managed') -> int:
    conn = get_conn()
    cur = conn.execute(
        """
        INSERT INTO campaign (name, template_id, sender_pool, target_mode, auto_start_enabled, required_permission_status, required_group_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (name, template_id, sender_pool, target_mode, auto_start_enabled, required_permission_status, required_group_status),
    )
    conn.commit()
    cid = cur.lastrowid
    conn.close()
    return int(cid)


def update_campaign(campaign_id: int, **fields):
    conn = get_conn()
    allowed = {'name','type','status','target_mode','template_id','sender_pool','auto_start_enabled','required_permission_status','required_group_status','total_targets','eligible_targets','sent_count','failed_count','blocked_count','started_at','finished_at','session_key','session_status','session_started_at','session_finished_at','session_target_limit','session_note'}
    parts = []
    values = []
    for k, v in fields.items():
        if k in allowed:
            parts.append(f"{k}=%s")
            values.append(v)
    if not parts:
        conn.close(); return
    values.append(campaign_id)
    conn.execute(f"UPDATE campaign SET {', '.join(parts)} WHERE id=%s", values)
    conn.commit()
    conn.close()


def get_broadcast_queue(campaign_id: int | None = None, sender_account_id: str | None = None, status: str | None = None, blocked_only: bool = False, *, page: int = 1, page_size: int = 25):
    conn = get_conn()
    clauses = ['1=1']
    values: list = []
    if campaign_id:
        clauses.append('ct.campaign_id=%s')
        values.append(campaign_id)
    if sender_account_id:
        clauses.append('ct.sender_account_id=%s')
        values.append(sender_account_id)
    if status:
        clauses.append('ct.status=%s')
        values.append(status)
    if blocked_only:
        clauses.append("ct.status='blocked'")
    total = conn.execute(
        f"SELECT COUNT(*) FROM campaign_target ct WHERE {' AND '.join(clauses)}",
        values,
    ).fetchone()[0]
    rows = conn.execute(
        f"""
        SELECT ct.*, c.name as campaign_name, g.nama as group_name, COALESCE(a.nama, ct.sender_account_id) as sender_name
        FROM campaign_target ct
        JOIN campaign c ON c.id=ct.campaign_id
        JOIN grup g ON g.id=ct.group_id
        LEFT JOIN akun a ON a.phone=ct.sender_account_id
        WHERE {' AND '.join(clauses)}
        ORDER BY COALESCE(ct.queue_position, ct.id) ASC LIMIT %s OFFSET %s
        """,
        (*values, page_size, (page - 1) * page_size),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)


def get_broadcast_queue_summary() -> dict:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status='eligible' THEN 1 ELSE 0 END) AS eligible_count,
          SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued_count,
          SUM(CASE WHEN status='sending' THEN 1 ELSE 0 END) AS sending_count,
          SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent_count,
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
          SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped_count,
          SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END) AS blocked_count,
          SUM(CASE WHEN next_attempt_at IS NOT NULL AND next_attempt_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') THEN 1 ELSE 0 END) AS retry_due_count
        FROM campaign_target
        """
    ).fetchone()
    conn.close()
    return {k: int((row[k] if row else 0) or 0) for k in row.keys()} if row else {}


def get_queue_target(target_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT ct.*, c.name as campaign_name, g.nama as group_name, COALESCE(a.nama, ct.sender_account_id) as sender_name
        FROM campaign_target ct
        JOIN campaign c ON c.id=ct.campaign_id
        JOIN grup g ON g.id=ct.group_id
        LEFT JOIN akun a ON a.phone=ct.sender_account_id
        WHERE ct.id=%s
        """,
        (target_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def create_campaign_targets(campaign_id: int, group_ids: list[int], sender_account_id: str | None = None, eligibility_reason: str = 'eligible_by_rule'):
    if not group_ids:
        return 0
    conn = get_conn()
    conn.executemany(
        "INSERT INTO campaign_target (campaign_id, group_id, sender_account_id, status, eligibility_reason, queue_position) VALUES (%s, %s, %s, 'eligible', %s, %s)",
        [(campaign_id, gid, sender_account_id, eligibility_reason, i + 1) for i, gid in enumerate(group_ids)],
    )
    conn.execute('UPDATE campaign SET total_targets=%s, eligible_targets=%s WHERE id=%s', (len(group_ids), len(group_ids), campaign_id))
    conn.commit()
    conn.close()
    return len(group_ids)


def update_queue_target(target_id: int, **fields):
    conn = get_conn()
    allowed = {'sender_account_id','status','eligibility_reason','queue_position','attempt_count','last_attempt_at','next_attempt_at','delivery_result','failure_reason','blocked_reason','hold_reason','staged_at','session_key','dispatch_slot','reserved_at','finalized_at','last_outcome_code'}
    parts = []
    values = []
    for k, v in fields.items():
        if k in allowed:
            parts.append(f"{k}=%s")
            values.append(v)
    if not parts:
        conn.close(); return
    parts.append("updated_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    values.append(target_id)
    conn.execute(f"UPDATE campaign_target SET {', '.join(parts)} WHERE id=%s", values)
    conn.commit()
    conn.close()


# ── AUTOMATION RULES ──────────────────────────────────────

def get_automation_rules(rule_type: str | None = None, enabled: bool | None = None, *, page: int = 1, page_size: int = 25):
    conn = get_conn()
    clauses = ['1=1']
    values: list = []
    if rule_type:
        clauses.append('rule_type=%s')
        values.append(rule_type)
    if enabled is not None:
        clauses.append('enabled=%s')
        values.append(1 if enabled else 0)
    total = conn.execute(f"SELECT COUNT(*) FROM automation_rule WHERE {' AND '.join(clauses)}", values).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM automation_rule WHERE {' AND '.join(clauses)} ORDER BY priority ASC, id DESC LIMIT %s OFFSET %s",
        (*values, page_size, (page - 1) * page_size),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)


def get_automation_rule_summary() -> dict:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN enabled=1 THEN 1 ELSE 0 END) AS enabled,
          SUM(CASE WHEN enabled=0 THEN 1 ELSE 0 END) AS disabled,
          SUM(success_count) AS success_count,
          SUM(fail_count) AS fail_count
        FROM automation_rule
        """
    ).fetchone()
    stage_rows = conn.execute(
        """
        SELECT rule_type,
               COUNT(*) AS total,
               SUM(CASE WHEN enabled=1 THEN 1 ELSE 0 END) AS enabled,
               SUM(CASE WHEN enabled=0 THEN 1 ELSE 0 END) AS disabled
        FROM automation_rule
        GROUP BY rule_type
        ORDER BY rule_type ASC
        """
    ).fetchall()
    conn.close()
    summary = {k: int((row[k] if row else 0) or 0) for k in row.keys()} if row else {}
    summary['by_rule_type'] = {
        str(r['rule_type']): {
            'total': int(r['total'] or 0),
            'enabled': int(r['enabled'] or 0),
            'disabled': int(r['disabled'] or 0),
        }
        for r in stage_rows
    }
    return summary


def get_automation_rule(rule_id: int) -> dict | None:
    conn = get_conn(); row = conn.execute('SELECT * FROM automation_rule WHERE id=%s', (rule_id,)).fetchone(); conn.close(); return dict(row) if row else None


def create_automation_rule(name: str, rule_type: str, enabled: int = 1, priority: int = 100, condition_json: str | None = None, action_json: str | None = None, cooldown_seconds: int = 0, scope_json: str | None = None) -> int:
    conn = get_conn()
    cur = conn.execute(
        'INSERT INTO automation_rule (name, rule_type, enabled, priority, condition_json, action_json, cooldown_seconds, scope_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        (name, rule_type, enabled, priority, condition_json, action_json, cooldown_seconds, scope_json),
    )
    conn.commit(); rid = cur.lastrowid; conn.close(); return int(rid)


def update_automation_rule(rule_id: int, **fields):
    conn = get_conn()
    allowed = {'name','rule_type','enabled','priority','condition_json','action_json','cooldown_seconds','scope_json','last_triggered_at','success_count','fail_count'}
    parts = []
    values = []
    for k, v in fields.items():
        if k in allowed:
            parts.append(f"{k}=%s")
            values.append(v)
    if not parts:
        conn.close(); return
    parts.append("updated_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
    values.append(rule_id)
    conn.execute(f"UPDATE automation_rule SET {', '.join(parts)} WHERE id=%s", values)
    conn.commit(); conn.close()


def delete_automation_rule(rule_id: int):
    conn = get_conn(); conn.execute('DELETE FROM automation_rule WHERE id=%s', (rule_id,)); conn.commit(); conn.close()


# ── RECOVERY ──────────────────────────────────────────────

def get_recovery_items(entity_type: str | None = None, status: str | None = None, severity: str | None = None, recoverable_only: bool = False, *, page: int = 1, page_size: int = 25):
    conn = get_conn()
    clauses = ['1=1']
    values: list = []
    if entity_type:
        clauses.append('entity_type=%s'); values.append(entity_type)
    if status:
        clauses.append('recovery_status=%s'); values.append(status)
    if severity:
        clauses.append('severity=%s'); values.append(severity)
    if recoverable_only:
        clauses.append("recovery_status IN ('recovery_needed','recoverable')")
    total = conn.execute(f"SELECT COUNT(*) FROM recovery_item WHERE {' AND '.join(clauses)}", values).fetchone()[0]
    rows = conn.execute(f"SELECT * FROM recovery_item WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT %s OFFSET %s", (*values, page_size, (page - 1) * page_size)).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)


def get_recovery_summary() -> dict:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN entity_type='scrape_job' AND recovery_status IN ('recovery_needed','recoverable') THEN 1 ELSE 0 END) AS stuck_jobs_count,
          SUM(CASE WHEN entity_type='assignment' AND recovery_status IN ('recovery_needed','recoverable') THEN 1 ELSE 0 END) AS stuck_assignments_count,
          SUM(CASE WHEN entity_type='campaign' AND recovery_status IN ('recovery_needed','recoverable') THEN 1 ELSE 0 END) AS stuck_campaigns_count,
          SUM(CASE WHEN worker_status='missing' THEN 1 ELSE 0 END) AS missing_workers_count,
          SUM(CASE WHEN recovery_status IN ('recovery_needed','recoverable') THEN 1 ELSE 0 END) AS recovery_needed_count,
          SUM(CASE WHEN recovery_status='recovered' THEN 1 ELSE 0 END) AS recovered_today_count
        FROM recovery_item
        """
    ).fetchone()
    conn.close()
    return {k: int((row[k] if row else 0) or 0) for k in row.keys()} if row else {}


def get_recovery_item(item_id: int) -> dict | None:
    conn = get_conn(); row = conn.execute('SELECT * FROM recovery_item WHERE id=%s', (item_id,)).fetchone(); conn.close(); return dict(row) if row else None


def create_or_update_recovery_item(entity_type: str, entity_id: str, **fields) -> int:
    conn = get_conn()
    row = conn.execute('SELECT id FROM recovery_item WHERE entity_type=%s AND entity_id=%s', (entity_type, str(entity_id))).fetchone()
    if row:
        item_id = int(row['id'])
        allowed = {'entity_name','current_status','worker_status','problem_type','severity','recovery_status','recovery_attempt_count','last_activity_at','heartbeat_at','last_recovery_at','last_recovery_result','note'}
        parts=[]; values=[]
        for k,v in fields.items():
            if k in allowed:
                parts.append(f"{k}=%s"); values.append(v)
        if parts:
            parts.append("updated_at=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')")
            values.append(item_id)
            conn.execute(f"UPDATE recovery_item SET {', '.join(parts)} WHERE id=%s", values)
    else:
        cur = conn.execute(
            '''INSERT INTO recovery_item (entity_type, entity_id, entity_name, current_status, worker_status, problem_type, severity, recovery_status, recovery_attempt_count, last_activity_at, heartbeat_at, last_recovery_at, last_recovery_result, note)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            (entity_type, str(entity_id), fields.get('entity_name'), fields.get('current_status'), fields.get('worker_status'), fields.get('problem_type'), fields.get('severity','medium'), fields.get('recovery_status','recovery_needed'), fields.get('recovery_attempt_count',0), fields.get('last_activity_at'), fields.get('heartbeat_at'), fields.get('last_recovery_at'), fields.get('last_recovery_result'), fields.get('note')),
        )
        item_id = int(cur.lastrowid)
    conn.commit(); conn.close(); return item_id


def update_recovery_item(item_id: int, **fields):
    create_or_update_recovery_item(get_recovery_item(item_id)['entity_type'], get_recovery_item(item_id)['entity_id'], **fields)


# ── AUDIT LOG ─────────────────────────────────────────────

def add_audit_log(level: str, module: str, action: str, message: str, *, entity_type: str | None = None, entity_id: str | None = None, result: str | None = None, payload: str | None = None):
    conn = get_conn()
    conn.execute(
        'INSERT INTO audit_log (level, module, entity_type, entity_id, action, result, message, payload) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        (level, module, entity_type, entity_id, action, result, message, payload),
    )
    conn.commit(); conn.close()


def get_audit_logs(level: str | None = None, module: str | None = None, entity_type: str | None = None, action: str | None = None, *, page: int = 1, page_size: int = 25):
    conn = get_conn()
    clauses=['1=1']; values=[]
    if level: clauses.append('level=%s'); values.append(level)
    if module: clauses.append('module=%s'); values.append(module)
    if entity_type: clauses.append('entity_type=%s'); values.append(entity_type)
    if action: clauses.append('action=%s'); values.append(action)
    total = conn.execute(f"SELECT COUNT(*) FROM audit_log WHERE {' AND '.join(clauses)}", values).fetchone()[0]
    rows = conn.execute(f"SELECT * FROM audit_log WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT %s OFFSET %s", (*values, page_size, (page - 1) * page_size)).fetchall()
    conn.close()
    return [dict(r) for r in rows], int(total or 0)