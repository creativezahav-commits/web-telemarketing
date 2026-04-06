# Analisis grup — fetch last chat, hitung score, auto update background
from __future__ import annotations

import threading
import time
import random
from datetime import datetime
from utils.storage_db import update_last_chat_grup
from utils.database import get_conn
from core.scoring import update_score_grup

# ── Fetch last chat satu grup ──────────────────────────────────────────────

async def fetch_last_chat(client, grup_id: int) -> str:
    try:
        msgs = await client.get_messages(int(grup_id), limit=1)
        if msgs and msgs[0].date:
            waktu = msgs[0].date.strftime("%Y-%m-%d %H:%M")
            update_last_chat_grup(grup_id, waktu)
            return waktu
    except:
        pass
    return None


# ── Auto update last_chat background worker ────────────────────────────────

_LAST_CHAT_WORKER: threading.Thread | None = None
_LAST_CHAT_RUNNING = False

def _pilih_klien_terbaik(clients: dict) -> tuple:
    """Pilih akun yang paling sedikit dipakai untuk update last_chat."""
    if not clients:
        return None, None
    # Pilih acak dari akun yang online agar tidak memberatkan satu akun
    phone = random.choice(list(clients.keys()))
    return phone, clients[phone]


def _update_last_chat_batch(clients: dict, batch_size: int = 5) -> int:
    """
    Update last_chat untuk sekelompok grup yang paling lama tidak diupdate.
    Hanya memproses grup yang sudah managed dan aktif.
    Kembalikan jumlah grup yang berhasil diupdate.
    """
    if not clients:
        return 0

    from services.account_manager import run_sync

    conn = get_conn()
    # Ambil grup yang paling lama tidak diupdate last_chat-nya
    # Prioritaskan grup managed yang sudah kirim pesan (butuh deteksi respons)
    rows = conn.execute("""
        SELECT id, nama, last_chat, last_kirim, owner_phone
        FROM grup
        WHERE status = 'active'
          AND assignment_status = 'managed'
        ORDER BY
            CASE WHEN last_chat IS NULL THEN 0 ELSE 1 END ASC,
            last_chat ASC
        LIMIT %s
    """, (batch_size,)).fetchall()
    conn.close()

    if not rows:
        return 0

    updated = 0
    for row in rows:
        grup_id = int(row['id'])
        grup_nama = row['nama'] or str(grup_id)

        # Pilih klien: utamakan owner grup, fallback ke klien lain yang sehat
        from utils.database import get_conn as _gc2
        owner = str(row['owner_phone'] or '')
        client = None

        # Cek status owner di DB sebelum pakai
        if owner and owner in clients:
            try:
                _c2 = _gc2()
                _st2 = _c2.execute("SELECT status FROM akun WHERE phone=%s", (owner,)).fetchone()
                _c2.close()
                if _st2 and str(_st2['status'] or '').lower() not in ('banned', 'restricted', 'suspended'):
                    client = clients[owner]
            except Exception:
                pass

        # Fallback: cari akun lain yang sehat
        if not client:
            for _ph, _cl in list(clients.items()):
                try:
                    _c3 = _gc2()
                    _st3 = _c3.execute("SELECT status FROM akun WHERE phone=%s", (_ph,)).fetchone()
                    _c3.close()
                    if _st3 and str(_st3['status'] or '').lower() in ('active', 'online'):
                        client = _cl
                        break
                except Exception:
                    continue

        if not client:
            continue

        try:
            async def _fetch(c, gid):
                try:
                    msgs = await c.get_messages(int(gid), limit=1)
                    if msgs and msgs[0].date:
                        waktu = msgs[0].date.strftime("%Y-%m-%d %H:%M")
                        update_last_chat_grup(gid, waktu)
                        return waktu
                except Exception:
                    pass
                return None

            waktu = run_sync(_fetch(client, grup_id), timeout=15)
            if waktu:
                updated += 1
                print(f"[LastChat] ✅ {grup_nama}: {waktu}")
        except Exception as e:
            print(f"[LastChat] ⚠️ Gagal update {grup_nama}: {str(e)[:60]}")

        # Jeda antar grup agar tidak spam API Telegram
        time.sleep(3)

    return updated


def start_last_chat_worker(interval_menit: int = 10) -> bool:
    """
    Jalankan background worker yang memperbarui last_chat secara berkala.
    Interval default: 10 menit sekali, update 5 grup per putaran.
    Tidak akan berjalan jika sudah ada worker aktif.
    """
    global _LAST_CHAT_WORKER, _LAST_CHAT_RUNNING

    if _LAST_CHAT_WORKER and _LAST_CHAT_WORKER.is_alive():
        return False  # Sudah berjalan

    _LAST_CHAT_RUNNING = True

    def _worker():
        print(f"[LastChat] Worker dimulai — update tiap {interval_menit} menit, 5 grup per putaran")
        # Tunda awal agar tidak bentrok saat startup
        time.sleep(60)

        while _LAST_CHAT_RUNNING:
            try:
                from services.account_manager import _clients
                if _clients:
                    updated = _update_last_chat_batch(_clients, batch_size=5)
                    if updated > 0:
                        print(f"[LastChat] Selesai: {updated} grup diperbarui")
                else:
                    print("[LastChat] Tidak ada akun online, skip")
            except Exception as exc:
                print(f"[LastChat] Worker error: {exc}")

            # Tunggu sebelum putaran berikutnya
            time.sleep(interval_menit * 60)

    _LAST_CHAT_WORKER = threading.Thread(
        target=_worker,
        daemon=True,
        name='last_chat_updater'
    )
    _LAST_CHAT_WORKER.start()
    return True


def stop_last_chat_worker():
    global _LAST_CHAT_RUNNING
    _LAST_CHAT_RUNNING = False


# ── Analisis dan scoring ───────────────────────────────────────────────────

def get_semua_analisis() -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM grup ORDER BY score DESC, jumlah_member DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_semua_score():
    conn = get_conn()
    ids = [r["id"] for r in conn.execute("SELECT id FROM grup").fetchall()]
    conn.close()
    for gid in ids:
        update_score_grup(gid)


# ─── Daily Reset Worker ───────────────────────────────────────────────────────

_DAILY_RESET_WORKER: threading.Thread | None = None
_DAILY_RESET_RUNNING = False

def _reset_stuck_targets_otomatis():
    """
    Reset otomatis target broadcast yang stuck setiap hari saat tengah malam.
    Membersihkan next_attempt_at yang expired dan target failed yang belum final.
    """
    from utils.database import get_conn
    conn = get_conn()
    try:
        # 1. Reset queued yang next_attempt_at sudah lewat
        r1 = conn.execute(
            """UPDATE campaign_target
               SET next_attempt_at=NULL, hold_reason=NULL
               WHERE status='queued'
                 AND next_attempt_at IS NOT NULL
                 AND next_attempt_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')"""
        )
        # 2. Reset queued dengan hold_reason sender_daily_limit → kuota sudah reset
        r2 = conn.execute(
            """UPDATE campaign_target
               SET next_attempt_at=NULL, hold_reason=NULL
               WHERE status='queued'
                 AND hold_reason IN ('sender_daily_limit','daily_limit_exhausted')"""
        )
        # 3. Reset grup yang stuck di broadcast_status queued/hold karena sender_daily_limit
        r3 = conn.execute(
            """UPDATE grup
               SET broadcast_status='broadcast_eligible',
                   broadcast_hold_reason=NULL,
                   broadcast_ready_at=NULL
               WHERE broadcast_status IN ('queued','hold')
                 AND broadcast_hold_reason IN ('sender_daily_limit','daily_limit_exhausted')
                 AND assignment_status='managed'"""
        )
        # 4. Reset throttle broadcast global agar mulai segar hari ini
        r4 = conn.execute(
            """UPDATE broadcast_throttle
               SET next_allowed_at=NULL, last_broadcast_at=NULL
               WHERE id=1"""
        )
        # 5. Reset throttle per akun agar semua akun mulai segar hari ini
        r5 = conn.execute(
            """UPDATE broadcast_throttle_akun
               SET next_allowed_at=NULL, last_broadcast_at=NULL"""
        )
        # 6. Reset next_join_at tiap akun agar auto join bisa mulai lagi hari baru
        conn.execute("UPDATE akun SET next_join_at = NULL")
        conn.commit()
        total = r1.rowcount + r2.rowcount
        print(f"[DailyReset] ✅ Reset otomatis: {r1.rowcount} target queued expired, "
              f"{r2.rowcount} target daily_limit, {r3.rowcount} grup, throttle direset")
        return total
    except Exception as e:
        print(f"[DailyReset] ❌ Error: {e}")
        return 0
    finally:
        conn.close()


def start_daily_reset_worker() -> bool:
    """
    Background worker yang menjalankan reset otomatis setiap tengah malam.
    Membersihkan target stuck agar broadcast bisa jalan normal setiap hari.
    """
    global _DAILY_RESET_WORKER, _DAILY_RESET_RUNNING

    if _DAILY_RESET_WORKER and _DAILY_RESET_WORKER.is_alive():
        return False

    _DAILY_RESET_RUNNING = True

    def _worker():
        from datetime import datetime, timedelta
        print("[DailyReset] Worker dimulai — reset otomatis tiap tengah malam")

        while _DAILY_RESET_RUNNING:
            try:
                sekarang = datetime.now()
                # Hitung waktu reset berikutnya: tengah malam hari ini atau besok
                reset_berikutnya = sekarang.replace(hour=0, minute=1, second=0, microsecond=0)
                if reset_berikutnya <= sekarang:
                    reset_berikutnya += timedelta(days=1)

                tunggu_detik = (reset_berikutnya - sekarang).total_seconds()
                print(f"[DailyReset] Reset berikutnya: {reset_berikutnya.strftime('%Y-%m-%d %H:%M')} "
                      f"(dalam {int(tunggu_detik/3600)} jam {int((tunggu_detik%3600)/60)} menit)")

                # Tidur sampai tengah malam
                time.sleep(max(60, tunggu_detik))

                # Jalankan reset
                if _DAILY_RESET_RUNNING:
                    print("[DailyReset] ⏰ Tengah malam — menjalankan reset otomatis...")
                    _reset_stuck_targets_otomatis()

            except Exception as e:
                print(f"[DailyReset] Worker error: {e}")
                time.sleep(300)  # tunggu 5 menit kalau ada error

    _DAILY_RESET_WORKER = threading.Thread(
        target=_worker,
        daemon=True,
        name='daily_reset_worker'
    )
    _DAILY_RESET_WORKER.start()
    return True
