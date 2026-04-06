import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.database import get_conn

conn = get_conn()

# Set status akun banned ke 'banned' agar tidak dipilih sender broadcast
r1 = conn.execute("""
    UPDATE akun SET status='banned'
    WHERE phone='+6287884147284'
""")
print(f"Akun +6287884147284 → banned: {r1.rowcount} baris")

# Pastikan auto_send_enabled = 0 untuk akun banned
r2 = conn.execute("""
    UPDATE akun SET auto_send_enabled=0
    WHERE phone='+6287884147284'
""")
print(f"auto_send_enabled=0: {r2.rowcount} baris")

# Reassign grup yang dipegang akun banned ke akun lain
# Cukup hapus owner_phone agar sistem assign ulang otomatis
r3 = conn.execute("""
    UPDATE grup 
    SET assignment_status='ready_assign', owner_phone=NULL,
        broadcast_status='hold', broadcast_hold_reason='owner_banned'
    WHERE owner_phone='+6287884147284'
      AND assignment_status IN ('assigned','managed')
""")
print(f"Grup di-reassign dari akun banned: {r3.rowcount} grup")

conn.commit()
conn.close()

print()
print("Selesai! Jalankan python app.py")
print("Sistem akan auto-assign grup ke akun yang sehat")
