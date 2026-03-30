from datetime import datetime
from utils.database import get_conn


def simpan_akun(phone, nama, username):
    conn = get_conn()
    conn.execute(
        "INSERT INTO akun (phone, nama, username) VALUES (?, ?, ?) "
        "ON CONFLICT(phone) DO UPDATE SET nama=excluded.nama, username=excluded.username",
        (phone, nama, username or "-")
    )
    conn.commit()
    conn.close()


def get_semua_akun():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM akun").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_status_akun(phone):
    conn = get_conn()
    row = conn.execute("SELECT status FROM akun WHERE phone=?", (phone,)).fetchone()
    conn.close()
    return row["status"] if row else "active"


def set_status_akun(phone, status):
    conn = get_conn()
    conn.execute("UPDATE akun SET status=? WHERE phone=?", (status, phone))
    conn.commit()
    conn.close()


def simpan_banyak_grup(daftar):
    conn = get_conn()
    for g in daftar:
        conn.execute(
            "INSERT INTO grup (id, nama, username, tipe, jumlah_member, link, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active') "
            "ON CONFLICT(id) DO UPDATE SET nama=excluded.nama, username=excluded.username, "
            "tipe=excluded.tipe, jumlah_member=excluded.jumlah_member, link=excluded.link, "
            "diupdate=datetime('now','localtime')",
            (g["id"], g["nama"], g.get("username"), g.get("tipe"),
             g.get("jumlah_member"), g.get("link"))
        )
    conn.commit()
    conn.close()


def get_semua_grup():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM grup").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_grup_aktif():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM grup WHERE status='active'").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_status_grup(grup_id):
    conn = get_conn()
    row = conn.execute("SELECT status FROM grup WHERE id=?", (grup_id,)).fetchone()
    conn.close()
    return row["status"] if row else "active"


def set_status_grup(grup_id, status):
    conn = get_conn()
    conn.execute("UPDATE grup SET status=? WHERE id=?", (status, grup_id))
    conn.commit()
    conn.close()


def simpan_draft(judul, isi):
    conn = get_conn()
    cur = conn.execute("INSERT INTO draft (judul, isi) VALUES (?, ?)", (judul, isi))
    did = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM draft WHERE id=?", (did,)).fetchone()
    conn.close()
    return dict(row)


def get_semua_draft():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM draft ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def hapus_draft(draft_id):
    conn = get_conn()
    conn.execute("DELETE FROM draft WHERE id=?", (draft_id,))
    conn.commit()
    conn.close()


def tambah_antrian(phone, grup_id, pesan):
    conn = get_conn()
    cur = conn.execute(
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
    conn.execute(
        "UPDATE antrian SET status=?, dikirim=datetime('now','localtime') WHERE id=?",
        (status, item_id)
    )
    conn.commit()
    conn.close()


def hapus_antrian(item_id):
    conn = get_conn()
    conn.execute("DELETE FROM antrian WHERE id=?", (item_id,))
    conn.commit()
    conn.close()


def catat_riwayat(phone, grup_id, nama_grup, status, pesan_error=None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO riwayat (phone, grup_id, nama_grup, status, pesan_error) "
        "VALUES (?, ?, ?, ?, ?)",
        (phone, grup_id, nama_grup, status, pesan_error)
    )
    conn.commit()
    conn.close()


def get_riwayat_hari_ini():
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM riwayat WHERE waktu LIKE ? ORDER BY id DESC",
        (hari + "%",)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_ringkasan_hari_ini():
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT "
        "SUM(CASE WHEN status='berhasil' THEN 1 ELSE 0 END) as berhasil, "
        "SUM(CASE WHEN status='gagal' THEN 1 ELSE 0 END) as gagal, "
        "SUM(CASE WHEN status='skip' THEN 1 ELSE 0 END) as skip, "
        "COUNT(*) as total "
        "FROM riwayat WHERE waktu LIKE ?",
        (hari + "%",)
    ).fetchone()
    conn.close()
    return {
        "berhasil": row["berhasil"] or 0,
        "gagal": row["gagal"] or 0,
        "skip": row["skip"] or 0,
        "total": row["total"] or 0
    }


def sudah_dikirim_hari_ini(grup_id):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) as n FROM riwayat "
        "WHERE grup_id=? AND status='berhasil' AND waktu LIKE ?",
        (grup_id, hari + "%")
    ).fetchone()
    conn.close()
    return row["n"] > 0


def hitung_kirim_hari_ini(phone):
    conn = get_conn()
    hari = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) as n FROM riwayat "
        "WHERE phone=? AND status='berhasil' AND waktu LIKE ?",
        (phone, hari + "%")
    ).fetchone()
    conn.close()
    return row["n"] or 0
