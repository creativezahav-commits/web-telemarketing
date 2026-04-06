import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.database import get_conn
from datetime import datetime

conn = get_conn()
hari = datetime.now().strftime("%Y-%m-%d")

print("=== RIWAYAT HARI INI PER STATUS ===")
rows = conn.execute("""
    SELECT status, COUNT(*) as jumlah
    FROM riwayat WHERE waktu LIKE ?
    GROUP BY status ORDER BY jumlah DESC
""", (hari + "%",)).fetchall()
for r in rows:
    print(f"  {r['status']}: {r['jumlah']}")

print()
print("=== TOTAL RIWAYAT HARI INI ===")
total = conn.execute("SELECT COUNT(*) as n FROM riwayat WHERE waktu LIKE ?", (hari+"%",)).fetchone()
print(f"  Total: {total['n']}")

conn.close()
