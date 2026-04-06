import sqlite3

DB = r'C:\Users\user\Downloads\telegrammm\web-telemarketing\backend\data\dashboard.db'
PHONE = '+6283186603470'

conn = sqlite3.connect(DB)

# Block semua campaign_target yang grupnya tidak ada di akun_grup akun aktif
r = conn.execute("""
    UPDATE campaign_target
    SET status = 'blocked',
        hold_reason = 'sender_not_joined',
        failure_reason = 'Akun belum join grup ini',
        next_attempt_at = NULL,
        finalized_at = datetime('now','localtime')
    WHERE status IN ('queued','sending','eligible')
    AND sender_account_id = ?
    AND group_id NOT IN (
        SELECT grup_id FROM akun_grup WHERE phone = ?
    )
""", (PHONE, PHONE))

print(f'Target bermasalah diblocked: {r.rowcount}')
conn.commit()

# Verifikasi sisa target yang masih bisa dikirim
sisa = conn.execute("""
    SELECT COUNT(*) as n FROM campaign_target ct
    WHERE ct.status IN ('queued','eligible')
    AND ct.sender_account_id = ?
    AND ct.group_id IN (
        SELECT grup_id FROM akun_grup WHERE phone = ?
    )
""", (PHONE, PHONE)).fetchone()[0]

print(f'Target valid yang masih bisa broadcast: {sisa}')
conn.close()
print('Selesai.')
