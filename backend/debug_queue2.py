import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.database import get_conn

conn = get_conn()

print("=== CEK QUERY DELIVERY PERSIS ===")
# Ini query yang dipakai stage_delivery
rows = conn.execute("""
    SELECT ct.id, ct.status, ct.campaign_id, ct.group_id,
           ct.next_attempt_at, ct.hold_reason,
           c.status as campaign_status,
           g.nama, g.broadcast_status, g.owner_phone
    FROM campaign_target ct
    JOIN campaign c ON c.id=ct.campaign_id
    JOIN grup g ON g.id=ct.group_id
    WHERE ct.campaign_id=23
      AND c.status IN ('queued','running')
      AND ct.status IN ('queued','eligible')
      AND (ct.next_attempt_at IS NULL OR ct.next_attempt_at <= datetime('now','localtime'))
    ORDER BY COALESCE(ct.queue_position, ct.id) ASC
    LIMIT 10
""").fetchall()

print(f"Hasil query delivery untuk campaign #23: {len(rows)} kandidat")
for r in rows:
    print(f"  ID={r['id']} | {r['nama'][:30]} | ct_status={r['status']} | camp_status={r['campaign_status']} | grup_status={r['broadcast_status']}")

print()
print("=== CEK SEMUA TARGET DI CAMPAIGN 23 ===")
rows2 = conn.execute("""
    SELECT ct.status, COUNT(*) as jumlah
    FROM campaign_target ct
    WHERE ct.campaign_id=23
    GROUP BY ct.status
""").fetchall()
for r in rows2:
    print(f"  {r['status']}: {r['jumlah']}")

print()
print("=== CEK APAKAH CAMPAIGN 23 ADA GRUP QUEUED ===")
rows3 = conn.execute("""
    SELECT ct.id, ct.status, ct.next_attempt_at, g.broadcast_status, g.assignment_status
    FROM campaign_target ct
    JOIN grup g ON g.id=ct.group_id
    WHERE ct.campaign_id=23 AND ct.status='queued'
    LIMIT 5
""").fetchall()
print(f"Target queued di campaign 23: {len(rows3)}")
for r in rows3:
    print(f"  ID={r['id']} | next={r['next_attempt_at']} | grup_bc={r['broadcast_status']} | assign={r['assignment_status']}")

print()
print("=== CEK TARGET QUEUED ADA DI CAMPAIGN MANA ===")
rows4 = conn.execute("""
    SELECT ct.campaign_id, COUNT(*) as jumlah
    FROM campaign_target ct
    WHERE ct.status='queued'
    GROUP BY ct.campaign_id
""").fetchall()
for r in rows4:
    print(f"  campaign_id={r['campaign_id']}: {r['jumlah']} target queued")

conn.close()
