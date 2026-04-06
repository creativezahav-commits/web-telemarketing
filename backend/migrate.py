import sqlite3, os

DB_FILE = "data/dashboard.db"

def migrate():
    print("Migrasi database...")
    conn = sqlite3.connect(DB_FILE)

    # Tabel baru akun_grup
    conn.execute("""
        CREATE TABLE IF NOT EXISTS akun_grup (
            phone    TEXT NOT NULL,
            grup_id  INTEGER NOT NULL,
            dibuat   TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (phone, grup_id)
        )
    """)
    print("  ✅ Tabel akun_grup siap")

    # Kolom baru di grup
    for nama, tipe in [
        ("aktif_indikator", "TEXT DEFAULT 'unknown'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE grup ADD COLUMN {nama} {tipe}")
            print(f"  ✅ grup.{nama} ditambahkan")
        except sqlite3.OperationalError:
            print(f"  ⏭️  grup.{nama} sudah ada")

    # Settings baru
    new_settings = [
        ("broadcast_jeda_min","20","Broadcast: Jeda minimum (detik)","number"),
        ("broadcast_jeda_max","60","Broadcast: Jeda maksimum (detik)","number"),
    ]
    for key, value, label, tipe in new_settings:
        conn.execute(
            "INSERT OR IGNORE INTO settings (key,value,label,tipe) VALUES (?,?,?,?)",
            (key, value, label, tipe)
        )
    print("  ✅ Settings baru ditambahkan")

    conn.commit()
    conn.close()
    print("\n✅ Migrasi selesai! Jalankan: python app.py")

if __name__ == "__main__":
    migrate()
