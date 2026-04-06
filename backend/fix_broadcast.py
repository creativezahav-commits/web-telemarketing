"""
fix_broadcast.py
Jalankan dari folder backend:
    python fix_broadcast.py

Perbaikan:
1. Reset target 'sending' yang macet di campaign #24
2. Skip permanen 101 grup dengan owner banned tanpa kandidat pengganti
"""
import sqlite3
import os
from datetime import datetime

DB = os.path.join(os.path.dirname(__file__), "data", "dashboard.db")

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("=" * 55)
print("  TG Dashboard — Fix Broadcast")
print("=" * 55)

# ── 1. RESET TARGET SENDING YANG MACET ────────────────────
print("\n[1] Reset target 'sending' yang macet...")

sending = conn.execute("""
    SELECT ct.id, g.nama, ct.sender_account_id
    FROM campaign_target ct
    JOIN grup g ON g.id = ct.group_id
    WHERE ct.status = 'sending'
""").fetchall()

print(f"    Ditemukan: {len(sending)} target sending")
for s in sending:
    print(f"    → CT_ID={s['id']} | {s['nama'][:30]} | sender={s['sender_account_id']}")

r1 = conn.execute("""
    UPDATE campaign_target
    SET status = 'queued',
        next_attempt_at = NULL,
        hold_reason = NULL,
        updated_at = ?
    WHERE status = 'sending'
""", (now,))
print(f"    ✅ {r1.rowcount} target direset ke 'queued'")

# ── 2. SKIP GRUP BOTTLENECK (OWNER BANNED TANPA KANDIDAT) ─
print("\n[2] Skip permanen grup dengan owner banned tanpa kandidat...")

banned_phones = ['+6283161394209', '+6287788741275', '+6287884147284']

# Hanya skip grup yang owner-nya banned DAN tidak ada akun lain yang join
r2 = conn.execute(f"""
    UPDATE grup
    SET broadcast_status = 'blocked',
        broadcast_hold_reason = 'owner_banned_no_candidate',
        diupdate = ?
    WHERE owner_phone IN ({','.join('?' for _ in banned_phones)})
      AND assignment_status IN ('assigned', 'managed', 'ready_assign')
      AND status = 'active'
      AND id NOT IN (
          SELECT ag.grup_id FROM akun_grup ag
          WHERE ag.phone NOT IN ({','.join('?' for _ in banned_phones)})
      )
""", [now] + banned_phones + banned_phones)
print(f"    ✅ {r2.rowcount} grup di-skip permanen")

# ── 3. RESET GRUP BROADCAST_STATUS YANG MASIH BISA JALAN ──
print("\n[3] Reset grup valid yang stuck di broadcast_status lama...")

r3 = conn.execute("""
    UPDATE grup
    SET broadcast_status = 'broadcast_eligible',
        broadcast_hold_reason = NULL,
        broadcast_ready_at = NULL,
        diupdate = ?
    WHERE assignment_status = 'managed'
      AND status = 'active'
      AND COALESCE(broadcast_status, 'hold') IN ('stabilization_wait', 'hold')
      AND broadcast_hold_reason IS NULL
      AND owner_phone NOT IN ('+6283161394209', '+6287788741275', '+6287884147284')
      AND id IN (
          SELECT ag.grup_id FROM akun_grup ag
          WHERE ag.phone NOT IN ('+6283161394209', '+6287788741275', '+6287884147284')
      )
""", (now,))
print(f"    ✅ {r3.rowcount} grup direset ke 'broadcast_eligible'")

# ── 4. RESET THROTTLE BROADCAST ───────────────────────────
print("\n[4] Reset throttle broadcast...")

r4 = conn.execute("""
    UPDATE broadcast_throttle
    SET next_allowed_at = NULL,
        last_broadcast_at = NULL
    WHERE id = 1
""")
print(f"    ✅ Throttle direset ({r4.rowcount} baris)")

# ── 5. RINGKASAN KONDISI SETELAH PATCH ────────────────────
print("\n[5] Kondisi setelah patch:")

total_queued = conn.execute("""
    SELECT COUNT(*) as n FROM campaign_target
    WHERE status IN ('queued', 'eligible')
    AND campaign_id IN (SELECT id FROM campaign WHERE status IN ('running', 'queued'))
""").fetchone()['n']

total_eligible = conn.execute("""
    SELECT COUNT(*) as n FROM grup
    WHERE broadcast_status = 'broadcast_eligible'
    AND assignment_status = 'managed'
    AND status = 'active'
""").fetchone()['n']

total_blocked_skip = conn.execute("""
    SELECT COUNT(*) as n FROM grup
    WHERE broadcast_hold_reason = 'owner_banned_no_candidate'
""").fetchone()['n']

campaigns = conn.execute("""
    SELECT id, status, session_status, sent_count, failed_count, total_targets
    FROM campaign
    WHERE status IN ('running', 'queued')
    ORDER BY id
""").fetchall()

print(f"    Target queued/eligible : {total_queued}")
print(f"    Grup broadcast_eligible: {total_eligible}")
print(f"    Grup di-skip (banned)  : {total_blocked_skip}")
print()
print("    Campaign aktif:")
for c in campaigns:
    print(f"      Campaign #{c['id']} | {c['status']}/{c['session_status']} | sent={c['sent_count']} failed={c['failed_count']} total={c['total_targets']}")

conn.commit()
conn.close()

print()
print("=" * 55)
print("  ✅ Patch selesai!")
print("  → Restart server: python app.py")
print("  → Tunggu 1-2 siklus (10-20 detik) untuk delivery mulai")
print("=" * 55)
