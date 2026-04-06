# -*- coding: utf-8 -*-
import asyncio, sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from services.account_manager import _clients, _loop, run_sync
import services.account_manager as am
from utils.database import get_conn

print("Menghubungkan akun...")
run_sync(am.auto_reconnect_semua(), timeout=60)
time.sleep(2)

print("=" * 60)
print("CEK STATUS AKUN DARI TELEGRAM")
print("=" * 60)

if not _clients:
    print("Tidak ada akun online.")
    sys.exit(0)

async def cek_semua():
    for phone, client in list(_clients.items()):
        print("\n--- Akun: " + phone + " ---")
        try:
            me = await client.get_me()
            if me is None:
                print("  ERROR: get_me() None")
                continue

            nama = str(getattr(me, 'first_name', '') or '') + ' ' + str(getattr(me, 'last_name', '') or '')
            print("  Nama       : " + nama.strip())
            print("  deleted    : " + str(getattr(me, 'deleted', False)))
            print("  restricted : " + str(getattr(me, 'restricted', False)))
            print("  scam       : " + str(getattr(me, 'scam', False)))
            print("  fake       : " + str(getattr(me, 'fake', False)))

            alasan = getattr(me, 'restriction_reason', None)
            print("  restriction_reason : " + str(alasan) if alasan else "  restriction_reason : (kosong)")

            conn = get_conn()
            row = conn.execute(
                "SELECT status, auto_send_enabled, cooldown_until, last_error_message FROM akun WHERE phone=%s",
                (phone,)
            ).fetchone()
            conn.close()
            if row:
                print("  DB status    : " + str(row['status']))
                print("  DB auto_send : " + str(row['auto_send_enabled']))
                print("  DB cooldown  : " + str(row['cooldown_until'] or '-'))
                print("  DB error     : " + str(row['last_error_message'] or '-'))

            if getattr(me, 'deleted', False):
                print("  VERDICT: AKUN DIHAPUS")
            elif getattr(me, 'restricted', False):
                print("  VERDICT: AKUN DIBATASI (restricted=True)")
            elif getattr(me, 'scam', False):
                print("  VERDICT: DITANDAI SCAM")
            else:
                print("  VERDICT: get_me() normal, tidak ada tanda dari Telegram")
                print("  SARAN  : Cek @SpamBot di Telegram dengan akun ini")

        except Exception as e:
            print("  ERROR  : " + str(e))

print("\n" + "=" * 60)
print("Selesai.")

run_sync(cek_semua(), timeout=60)