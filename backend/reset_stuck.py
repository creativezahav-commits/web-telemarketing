import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.database import get_conn

conn = get_conn()
r1 = conn.execute("UPDATE campaign_target SET next_attempt_at=NULL, hold_reason=NULL WHERE status='queued' AND next_attempt_at IS NOT NULL")
r2 = conn.execute("UPDATE campaign_target SET status='queued', next_attempt_at=NULL, failure_reason=NULL, finalized_at=NULL WHERE status='failed' AND finalized_at IS NULL")
r3 = conn.execute("UPDATE grup SET broadcast_status='broadcast_eligible', broadcast_hold_reason=NULL, broadcast_ready_at=NULL WHERE broadcast_status IN ('queued','hold') AND assignment_status='managed'")
r4 = conn.execute("UPDATE broadcast_throttle SET next_allowed_at=NULL, last_broadcast_at=NULL WHERE id=1")
conn.commit()
conn.close()
print(f"Reset selesai:")
print(f"  {r1.rowcount} target queued direset")
print(f"  {r2.rowcount} target failed dikembalikan ke antrian")
print(f"  {r3.rowcount} grup direset ke eligible")
print(f"  Throttle broadcast direset")
print("Selesai! Sekarang restart server.")
