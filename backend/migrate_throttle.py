"""
migrate_throttle.py
Jalankan SEKALI untuk tambah kolom throttle baru ke tabel akun.
Letakkan file ini di folder: backend/
Jalankan: python migrate_throttle.py
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "dashboard.db")

COLUMNS = [
    ("next_broadcast_at",  "TEXT DEFAULT NULL"),
    ("last_broadcast_at",  "TEXT DEFAULT NULL"),
    ("last_broadcast_group", "TEXT DEFAULT NULL"),
    ("last_join_at",       "TEXT DEFAULT NULL"),
    ("last_join_group",    "TEXT DEFAULT NULL"),
]

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("PRAGMA table_info(akun)")
    existing = {row[1] for row in cur.fetchall()}

    added = []
    for col_name, col_def in COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE akun ADD COLUMN {col_name} {col_def}")
            added.append(col_name)
            print(f"  ✅ Kolom ditambahkan: {col_name}")
        else:
            print(f"  ⏭️  Sudah ada, skip: {col_name}")

    conn.commit()
    conn.close()

    if added:
        print(f"\n✅ Migrasi selesai. {len(added)} kolom baru ditambahkan.")
    else:
        print("\n✅ Semua kolom sudah ada. Tidak ada perubahan.")

if __name__ == "__main__":
    main()
