import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.database import get_conn

conn = get_conn()

# Tandai campaign #23 sebagai completed karena semua targetnya sudah selesai
r1 = conn.execute("""
    UPDATE campaign 
    SET status='completed', finished_at=datetime('now','localtime')
    WHERE id=23 AND status='running'
""")
print(f"Campaign #23 → completed: {r1.rowcount} baris")

# Aktifkan campaign #24 (yang paling banyak target queued)
r2 = conn.execute("""
    UPDATE campaign 
    SET status='running', started_at=datetime('now','localtime')
    WHERE id=24 AND status='queued'
""")
print(f"Campaign #24 → running: {r2.rowcount} baris")

# Reset throttle agar langsung bisa kirim
r3 = conn.execute("""
    UPDATE broadcast_throttle 
    SET next_allowed_at=NULL, last_broadcast_at=NULL 
    WHERE id=1
""")
print(f"Throttle direset: {r3.rowcount} baris")

conn.commit()
conn.close()

print()
print("Selesai! Sekarang jalankan: python app.py")
print("Broadcast akan langsung berjalan ke 55 grup di campaign #24")
