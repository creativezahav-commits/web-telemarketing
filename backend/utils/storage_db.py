from datetime import datetime
from utils.database import get_conn


# ── AKUN ──────────────────────────────────────────────────

def simpan_akun(phone, nama, username, tanggal_buat=None):
    conn = get_conn()
    conn.execute("""
        INSERT INTO akun (phone, nama, username, tanggal_buat)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(phone) DO UPDATE SET
            nama=excluded.nama,
            username=excluded.username,
            tanggal_buat=COALESCE(excluded.tanggal_buat, akun.tanggal_buat)
    """, (phone, nama, username or "-", tanggal_buat))
    conn.commit()
    conn.close()

def get_semua_akun():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM akun").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_status_akun(phone):
    conn = get_conn()
    row  = conn.execute("SELECT status FROM akun WHERE phone=?", (phone,)).fetchone()
    conn.close()
    return row["status"] if row else "active"

def set_status_akun(phone, status):
    conn = get_conn()
    conn.execute("UPDATE akun SET status=? WHERE phone=?", (status, phone))
    conn.commit()
    conn.close()

def set_level_warming(phone, level):
    conn = get_conn()
    conn.execute("UPDATE akun SET level_warming=? WHERE phone=?", (level, phone))
    conn.commit()
    conn.close()

def set_score_akun(phone, score):
    conn = get_conn()
    conn.execute("UPDATE akun SET score=? WHERE phone=?", (score, phone))
    conn.commit()
    conn.close()

def update_stats_akun(phone, berhasil: bool, flood: bool = False, banned: bool = False):
    conn = get_conn()
    conn.execute("""
        UPDATE akun SET
            total_kirim    = total_kirim + 1,
            total_berhasil = total_berhasil + ?,
            total_flood    = total_flood + ?,
            total_banned   = total_banned + ?
        WHERE phone=?
    """, (1 if berhasil else 0, 1 if flood else 0, 1 if banned else 0, phone))
    conn.commit()
    conn.close()


# ── GRUP ──────────────────────────────────────────────────

def simpan_banyak_grup(daftar, sumber="fetch"):
    conn = get_conn()
    for g in daftar:
        conn.execute("""
            INSERT INTO grup (id, nama, username, tipe, jumlah_member, link, sumber)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                nama=excluded.nama,
                username=excluded.username,
                tipe=excluded.tipe,
                jumlah_member=excluded.jumlah_member,
                link=excluded.link,
                diupdate=datetime('now','localtime')
        """, (g["id"], g["nama"], g.get("username"), g.get("tipe"),
              g.get("jumlah_member", 0), g.get("link"), sumber))
    conn.commit()
    conn.close()

def get_semua_grup():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM grup ORDER BY score DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_grup_aktif():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM grup WHERE status='active' ORDER BY score DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_grup_hot():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM grup WHERE label='Hot' AND status='active' ORDER BY score DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_status_grup(grup_id):
    conn = get_conn()
    row  = conn.execute("SELECT status FROM grup WHERE id=?", (grup_id,)).fetchone()
    conn.close()
    return row["status"] if row else "active"

def set_status_grup(grup_id, status):
    conn = get_conn()
    conn.execute("UPDATE grup SET status=? WHERE id=?", (status, grup_id))
    conn.commit()
    conn.close()

def set_score_grup(grup_id, score, label):
    conn = get_conn()
    conn.execute(
        "UPDATE grup SET score=?, label=? WHERE id=?",
        (score, label, grup_id)
    )
    conn.commit()
    conn.close()

def update_last_kirim_grup(grup_id):
    conn = get_conn()
    waktu = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        UPDATE grup SET
            last_kirim     = ?,
            total_kirim    = total_kirim + 1,
            total_berhasil = total_berhasil + 1
        WHERE id=?
    """, (waktu, grup_id))
    conn.commit()
    conn.close()

def update_last_chat_grup(grup_id, waktu_chat):
    conn = get_conn()
    conn.execute(
        "UPDATE grup SET last_chat=? WHERE id=?",
        (waktu_chat, grup_id)
    )
    conn.commit()
    conn.close()

def grup_sudah_ada(grup_id):
    conn = get_conn()
    row  = conn.execute("SELECT id FROM grup WHERE id=?", (grup_id,)).fetchone()
    conn.close()
    return row is not None


# ── DRAFT ─────────────────────────────────────────────────

def simpan_draft(judul, isi):
    conn = get_conn()
    cur  = conn.execute(
        "INSERT INTO draft (judul, isi) VALUES (?, ?)", (judul, isi)
    )
    did = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM draft WHERE id=?", (did,)).fetchone()
    conn.close()
    return dict(row)

def get_semua_draft():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM draft ORDER BY aktif DESC, id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_draft_aktif():
    conn = get_conn()
    row  = conn.execute("SELECT * FROM draft WHERE aktif=1").fetchone()
    conn.close()
    return dict(row) if row else None

def set_draft_aktif(draft_id):
    conn = get_conn()
    conn.execute("UPDATE draft SET aktif=0")
    conn.execute("UPDATE draft SET aktif=1 WHERE id=?", (draft_id,))
    conn.commit()
    conn.close()

def hapus_draft(draft_id):
    conn = get_conn()
    conn.execute("DELETE FROM draft WHERE id=?", (draft_id,))
    conn.commit()
    conn.close()


# ── ANTRIAN ───────────────────────────────────────────────

def tambah_antrian(phone, grup_id, pesan):
    conn = get_conn()
    cur  = conn.execute(
        "INSERT INTO antrian (phone, grup_id, pesan) VALUES (?, ?, ?)",
        (phone, grup_id, pesan)
    )
    iid = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM antrian WHERE id=?", (iid,)).fetchone()
    conn.close()
    return dict(row)

def get_semua_antrian():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM antrian ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_antrian_menunggu():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM antrian WHERE status='menunggu' ORDER BY id ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def update_status_antrian(item_id, status):
    conn = get_conn()
    conn.execute("""
        UPDATE antrian SET status=?, dikirim=datetime('now','localtime')
        WHERE id=?
    """, (status, item_id))
    conn.commit()
    conn.close()

def hapus_antrian(item_id):
    conn = get_conn()
    conn.execute("DELETE FROM antrian WHERE id=?", (item_id,))
    conn.commit()
    conn.close()


# ── RIWAYAT ───────────────────────────────────────────────

def catat_riwayat(phone, grup_id, nama_grup, status, pesan_error=None):
    conn = get_conn()
    conn.execute("""
        INSERT INTO riwayat (phone, grup_id, nama_grup, status, pesan_error)
        VALUES (?, ?, ?, ?, ?)
    """, (phone, grup_id, nama_grup, status, pesan_error))
    conn.commit()
    conn.close()

def get_riwayat_hari_ini():
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT * FROM riwayat WHERE waktu LIKE ? ORDER BY id DESC
    """, (hari + "%",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_ringkasan_hari_ini():
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row  = conn.execute("""
        SELECT
            SUM(CASE WHEN status='berhasil' THEN 1 ELSE 0 END) as berhasil,
            SUM(CASE WHEN status='gagal'    THEN 1 ELSE 0 END) as gagal,
            SUM(CASE WHEN status='skip'     THEN 1 ELSE 0 END) as skip,
            COUNT(*) as total
        FROM riwayat WHERE waktu LIKE ?
    """, (hari + "%",)).fetchone()
    conn.close()
    return {
        "berhasil": row["berhasil"] or 0,
        "gagal"   : row["gagal"]    or 0,
        "skip"    : row["skip"]     or 0,
        "total"   : row["total"]    or 0
    }

def sudah_dikirim_hari_ini(grup_id):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row  = conn.execute("""
        SELECT COUNT(*) as n FROM riwayat
        WHERE grup_id=? AND status='berhasil' AND waktu LIKE ?
    """, (grup_id, hari + "%")).fetchone()
    conn.close()
    return row["n"] > 0

def hitung_kirim_hari_ini(phone):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row  = conn.execute("""
        SELECT COUNT(*) as n FROM riwayat
        WHERE phone=? AND status='berhasil' AND waktu LIKE ?
    """, (phone, hari + "%")).fetchone()
    conn.close()
    return row["n"] or 0

def hitung_join_hari_ini(phone):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row  = conn.execute("""
        SELECT COUNT(*) as n FROM riwayat
        WHERE phone=? AND status='join' AND waktu LIKE ?
    """, (phone, hari + "%")).fetchone()
    conn.close()
    return row["n"] or 0
