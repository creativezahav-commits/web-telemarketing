import sqlite3
conn = sqlite3.connect("data/dashboard.db")
rows = conn.execute("SELECT nama_grup, pesan_error FROM riwayat WHERE status='gagal' ORDER BY id DESC LIMIT 5").fetchall()
for r in rows:
    print(r)
conn.close()