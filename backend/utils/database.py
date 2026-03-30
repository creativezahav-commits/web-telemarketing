import sqlite3
from pathlib import Path

Path("data").mkdir(exist_ok=True)
Path("session").mkdir(exist_ok=True)

DB_FILE = "data/dashboard.db"


def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "CREATE TABLE IF NOT EXISTS akun ("
        "phone TEXT PRIMARY KEY, "
        "nama TEXT, "
        "username TEXT, "
        "status TEXT DEFAULT 'active', "
        "dibuat TEXT DEFAULT (datetime('now','localtime')))"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS grup ("
        "id INTEGER PRIMARY KEY, "
        "nama TEXT, "
        "username TEXT, "
        "tipe TEXT, "
        "jumlah_member INTEGER, "
        "link TEXT, "
        "status TEXT DEFAULT 'active', "
        "diupdate TEXT DEFAULT (datetime('now','localtime')))"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS draft ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "judul TEXT NOT NULL, "
        "isi TEXT NOT NULL, "
        "dibuat TEXT DEFAULT (datetime('now','localtime')))"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS antrian ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "phone TEXT NOT NULL, "
        "grup_id INTEGER NOT NULL, "
        "pesan TEXT NOT NULL, "
        "status TEXT DEFAULT 'menunggu', "
        "dibuat TEXT DEFAULT (datetime('now','localtime')), "
        "dikirim TEXT)"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS riwayat ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "phone TEXT NOT NULL, "
        "grup_id INTEGER NOT NULL, "
        "nama_grup TEXT, "
        "status TEXT NOT NULL, "
        "pesan_error TEXT, "
        "waktu TEXT DEFAULT (datetime('now','localtime')))"
    )

    conn.commit()
    conn.close()
    print("Database siap: data/dashboard.db")
