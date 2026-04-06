"""
migrate_throttle_perakun.py
Jalankan SEKALI dari folder backend sebelum restart server:
    python migrate_throttle_perakun.py

Yang dilakukan:
1. Buat tabel broadcast_throttle_akun (throttle per akun)
2. Ubah broadcast_jam_selesai → 0 (24 jam aktif)
3. Reset throttle lama
"""
import sqlite3
import os

DB = os.path.join(os.path.dirname(__file__), "data", "dashboard.db")
conn = sqlite3.connect(DB)

print("=" * 55)
print("  Migrasi Throttle Per Akun")
print("=" * 55)

# 1. Buat tabel throttle per akun
print("\n[1] Buat tabel broadcast_throttle_akun...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS broadcast_throttle_akun (
        phone             TEXT PRIMARY KEY,
        last_broadcast_at TEXT,
        next_allowed_at   TEXT
    )
""")
print("    ✅ Tabel siap")

# 2. Ubah jam_selesai ke 0 untuk 24 jam aktif
print("\n[2] Set broadcast 24 jam (jam_mulai=0, jam_selesai=0)...")
conn.execute("""
    UPDATE settings SET value='0' WHERE key='broadcast_jam_mulai'
""")
conn.execute("""
    UPDATE settings SET value='0' WHERE key='broadcast_jam_selesai'
""")
# Cek apakah setting ada
r = conn.execute("SELECT key, value FROM settings WHERE key IN ('broadcast_jam_mulai','broadcast_jam_selesai')").fetchall()
for row in r:
    print(f"    {row[0]}: {row[1]}")
print("    ✅ Setting 24 jam aktif")

# 3. Reset throttle lama
print("\n[3] Reset throttle lama...")
conn.execute("UPDATE broadcast_throttle SET next_allowed_at=NULL, last_broadcast_at=NULL WHERE id=1")
print("    ✅ Throttle global direset")

conn.commit()
conn.close()

print()
print("=" * 55)
print("  ✅ Migrasi selesai!")
print("  → Sekarang restart server: python app.py")
print("=" * 55)
