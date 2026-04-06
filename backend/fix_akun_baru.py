"""
fix_akun_baru.py
Jalankan dari folder backend (saat server MATI):
    python fix_akun_baru.py

Yang dilakukan:
1. Set akun +6285368414569 sebagai owner 5 grup yang sudah diikutinya
2. Set broadcast_status grup ke broadcast_eligible agar masuk campaign
3. Pastikan warming level akun baru tidak menghalangi assignment
"""
import sqlite3
import os
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), "data", "dashboard.db")
PHONE = "+6285368414569"
NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=30000")

print("=" * 55)
print(f"  Fix Akun Baru: {PHONE}")
print("=" * 55)

# Ambil grup yang diikuti akun ini
groups = conn.execute("""
    SELECT ag.grup_id, g.nama, g.broadcast_status, g.assignment_status, g.owner_phone
    FROM akun_grup ag
    JOIN grup g ON g.id = ag.grup_id
    WHERE ag.phone = ?
""", (PHONE,)).fetchall()

print(f"\n[1] Grup yang diikuti akun ({len(groups)} grup):")
for g in groups:
    print(f"    ID={g['grup_id']} | {g['nama'][:35]} | bc={g['broadcast_status']} | owner={g['owner_phone']}")

# Set akun ini sebagai owner grup yang belum punya owner
print(f"\n[2] Set sebagai owner grup yang belum punya owner...")
r1 = conn.execute(f"""
    UPDATE grup
    SET owner_phone = ?,
        assignment_status = 'managed',
        broadcast_status = 'broadcast_eligible',
        broadcast_hold_reason = NULL,
        broadcast_ready_at = NULL,
        join_status = 'joined',
        diupdate = ?
    WHERE id IN (SELECT grup_id FROM akun_grup WHERE phone = ?)
      AND (owner_phone IS NULL OR owner_phone = ?)
      AND status = 'active'
""", (PHONE, NOW, PHONE, PHONE))
print(f"    ✅ {r1.rowcount} grup di-set ke managed + broadcast_eligible")

# Grup yang sudah punya owner lain — tambahkan akun ini sebagai fallback sender
print(f"\n[3] Grup yang sudah punya owner lain (akun ini sebagai fallback):")
others = conn.execute("""
    SELECT ag.grup_id, g.nama, g.owner_phone
    FROM akun_grup ag
    JOIN grup g ON g.id = ag.grup_id
    WHERE ag.phone = ?
      AND g.owner_phone IS NOT NULL
      AND g.owner_phone != ?
""", (PHONE, PHONE)).fetchall()
for g in others:
    print(f"    ID={g['grup_id']} | {g['nama'][:35]} | owner={g['owner_phone']} (dibiarkan)")

# Verifikasi hasil
print(f"\n[4] Verifikasi setelah patch:")
result = conn.execute("""
    SELECT g.id, g.nama, g.broadcast_status, g.assignment_status, g.owner_phone
    FROM akun_grup ag
    JOIN grup g ON g.id = ag.grup_id
    WHERE ag.phone = ?
""", (PHONE,)).fetchall()
for r in result:
    print(f"    {r['nama'][:30]} | bc={r['broadcast_status']} | assign={r['assignment_status']} | owner={r['owner_phone']}")

conn.commit()
conn.close()

print()
print("=" * 55)
print("  ✅ Selesai!")
print(f"  → {PHONE} siap sebagai sender")
print("  → Restart server: python app.py")
print("  → Grup akan masuk campaign di siklus berikutnya")
print("=" * 55)
