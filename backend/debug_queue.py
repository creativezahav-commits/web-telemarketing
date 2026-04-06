import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.database import get_conn

conn = get_conn()

print("=== STATUS CAMPAIGN TARGET ===")
rows = conn.execute("""
    SELECT status, 
           COUNT(*) as jumlah,
           SUM(CASE WHEN next_attempt_at IS NULL THEN 1 ELSE 0 END) as tanpa_next,
           SUM(CASE WHEN next_attempt_at IS NOT NULL AND next_attempt_at <= datetime('now','localtime') THEN 1 ELSE 0 END) as next_sudah_lewat,
           SUM(CASE WHEN next_attempt_at > datetime('now','localtime') THEN 1 ELSE 0 END) as next_belum_tiba
    FROM campaign_target
    GROUP BY status
""").fetchall()
for r in rows:
    print(f"  {r['status']}: total={r['jumlah']}, tanpa_next={r['tanpa_next']}, sudah_lewat={r['next_sudah_lewat']}, belum_tiba={r['next_belum_tiba']}")

print()
print("=== 5 SAMPLE TARGET QUEUED ===")
rows2 = conn.execute("""
    SELECT ct.id, ct.status, ct.next_attempt_at, ct.hold_reason, ct.last_outcome_code,
           g.nama, g.broadcast_status
    FROM campaign_target ct
    JOIN grup g ON g.id = ct.group_id
    WHERE ct.status = 'queued'
    LIMIT 5
""").fetchall()
for r in rows2:
    print(f"  ID={r['id']} | {r['nama'][:30]} | next_attempt={r['next_attempt_at']} | hold={r['hold_reason']} | grup_status={r['broadcast_status']}")

print()
print("=== STATUS BROADCAST THROTTLE ===")
t = conn.execute("SELECT * FROM broadcast_throttle WHERE id=1").fetchone()
if t:
    print(f"  last_broadcast_at: {t['last_broadcast_at']}")
    print(f"  next_allowed_at: {t['next_allowed_at']}")
else:
    print("  Tidak ada data throttle")

print()
print("=== CAMPAIGN AKTIF ===")
c = conn.execute("SELECT id, status, name FROM campaign WHERE status IN ('running','queued') LIMIT 5").fetchall()
for r in c:
    print(f"  ID={r['id']} | {r['status']} | {r['name']}")

conn.close()
