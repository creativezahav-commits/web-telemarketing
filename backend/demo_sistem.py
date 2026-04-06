# -*- coding: utf-8 -*-
"""
DEMO SISTEM TG DASHBOARD — Semua Kondisi Automation Rules
==========================================================
Jalankan dari folder backend/:
  python demo_sistem.py

Mensimulasikan SEMUA kondisi yang mungkin terjadi di sistem
TANPA mengirim pesan atau join grup ke Telegram.
Output dapat dipaste ke Claude untuk analisa dan debug.
"""

import os, sys, json, traceback
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Warna terminal ────────────────────────────────────────────
G    = '\033[92m'
R    = '\033[91m'
Y    = '\033[93m'
B    = '\033[94m'
C    = '\033[96m'
W    = '\033[97m'
DIM  = '\033[2m'
BOLD = '\033[1m'
RESET = '\033[0m'

def header(no, teks):
    print(f"\n{B}{'='*70}{RESET}")
    print(f"{BOLD}{W}[{no}] {teks}{RESET}")
    print(f"{B}{'='*70}{RESET}")

def sub(teks):    print(f"\n{C}  > {teks}{RESET}")
def ok(teks):     print(f"    {G}OK{RESET}   {teks}")
def warn(teks):   print(f"    {Y}WARN{RESET} {teks}")
def err(teks):    print(f"    {R}ERR{RESET}  {teks}")
def info(teks):   print(f"    {DIM}->{RESET}   {teks}")
def nilai(k, v):  print(f"    {DIM}{k:<42}{RESET} {W}{v}{RESET}")

MASALAH  = []
RINGKASAN = {}

def catat(kat, teks):
    MASALAH.append(f"[{kat}] {teks}")


# ════════════════════════════════════════════════════════════════
# INISIALISASI
# ════════════════════════════════════════════════════════════════
header("INIT", "INISIALISASI")

try:
    from utils.database import get_conn
    from utils.settings_manager import get as _gs, get_int as _gi
    from utils.storage_db import get_semua_akun, get_semua_grup
    from core.warming import get_daily_capacity, get_info_warming
    ok("Modul dasar OK")
except Exception as e:
    print(f"FATAL: {e}"); sys.exit(1)

try:
    conn = get_conn(); conn.execute("SELECT 1"); conn.close()
    ok("Koneksi database OK")
except Exception as e:
    print(f"FATAL DB: {e}"); sys.exit(1)

try:
    from services.orchestrator_service import (
        _join_boleh_sekarang, _hitung_jeda_join, _join_quota_snapshot,
        _broadcast_boleh_kirim_sekarang, _hitung_jeda_broadcast,
        _send_quota_snapshot, _available_online_senders,
        _choose_candidate, _sender_available_for_delivery,
    )
    from services.automation_rule_engine import resolve_stage_rules
    from services.group_send_guard import evaluate_group_send_guard
    ok("Modul orchestrator OK")
except Exception as e:
    print(f"FATAL orchestrator: {e}"); sys.exit(1)

akun_list = get_semua_akun()
grup_list = get_semua_grup()
RINGKASAN['total_akun'] = len(akun_list)
RINGKASAN['total_grup'] = len(grup_list)
print(f"\n  Akun: {len(akun_list)} | Grup: {len(grup_list)}")


# ════════════════════════════════════════════════════════════════
# DEMO 1: STATUS SEMUA AKUN
# ════════════════════════════════════════════════════════════════
header(1, "STATUS SEMUA AKUN")

status_count = {}
for akun in akun_list:
    st = akun.get('status') or 'active'
    status_count[st] = status_count.get(st, 0) + 1

print()
for st, count in sorted(status_count.items()):
    warna = G if st in ('active','online') else R if st in ('banned','restricted','suspended') else Y
    print(f"  {warna}{st:<22}{RESET} {count} akun")

RINGKASAN['status_akun'] = status_count

sub("Detail per akun — join, broadcast, throttle, risiko")
for akun in akun_list:
    phone       = akun['phone']
    status      = akun.get('status') or 'active'
    level       = akun.get('level_warming') or 1
    auto_send   = bool(akun.get('auto_send_enabled', 1))
    auto_assign = bool(akun.get('auto_assign_enabled', 1))
    auto_join   = bool(akun.get('auto_join_enabled', 1))

    try:
        cap     = get_daily_capacity(phone)
        join_u  = cap['join']['used'];  join_l  = cap['join']['limit'];  join_s  = cap['join']['remaining']
        kirim_u = cap['kirim']['used']; kirim_l = cap['kirim']['limit']; kirim_s = cap['kirim']['remaining']
    except:
        join_u = join_l = join_s = kirim_u = kirim_l = kirim_s = 0

    try:
        info_w = get_info_warming(phone)
        umur   = info_w.get('umur_hari', 0)
        label  = info_w.get('label_level', '')
    except:
        umur = 0; label = ''

    wst = G if status in ('active','online') else R
    print(f"\n  {wst}[{status.upper()}]{RESET} {W}{phone}{RESET} Level {level} {label} | Umur {umur} hari")

    flags = []
    if not auto_send:   flags.append(f"{R}auto_send=OFF{RESET}")
    if not auto_assign: flags.append(f"{Y}auto_assign=OFF{RESET}")
    if not auto_join:   flags.append(f"{Y}auto_join=OFF{RESET}")
    if flags: info("Flags: " + " | ".join(flags))

    info(f"Join : {join_u}/{join_l} terpakai, sisa {join_s}")
    info(f"Kirim: {kirim_u}/{kirim_l} terpakai, sisa {kirim_s}")

    try:
        boleh_j, alasan_j = _join_boleh_sekarang(phone)
        if boleh_j:
            jeda_j = _hitung_jeda_join(phone)
            ok(f"Join: BOLEH — jeda berikutnya {jeda_j//60}m {jeda_j%60}s")
        else:
            warn(f"Join: DITAHAN — {alasan_j}")
            catat("JOIN", f"{phone}: {alasan_j}")
    except Exception as e:
        warn(f"Join throttle error: {e}")

    try:
        boleh_b, alasan_b = _broadcast_boleh_kirim_sekarang(phone)
        if boleh_b:
            jeda_b = _hitung_jeda_broadcast(phone)
            ok(f"Broadcast: BOLEH — jeda setelah kirim {jeda_b:.1f} menit")
        else:
            warn(f"Broadcast: DITAHAN — {alasan_b}")
    except Exception as e:
        warn(f"Broadcast throttle error: {e}")

    if status in ('banned','restricted','suspended','session_expired'):
        err("AKUN TIDAK AKTIF — tidak ikut automasi")
        catat("AKUN", f"{phone} status={status}")
    elif join_s == 0 and join_l > 0:
        warn("Kuota join HABIS hari ini")
    elif kirim_s == 0 and kirim_l > 0:
        warn("Kuota kirim HABIS hari ini")


# ════════════════════════════════════════════════════════════════
# DEMO 2: AUTOMATION RULE ENGINE — SEMUA STAGE
# ════════════════════════════════════════════════════════════════
header(2, "AUTOMATION RULE ENGINE — STATUS SEMUA STAGE")

STAGES = [
    ('import',           'Auto Import'),
    ('permission',       'Auto Permission'),
    ('assignment',       'Auto Assignment'),
    ('campaign_prepare', 'Auto Campaign Prepare'),
    ('delivery',         'Auto Broadcast/Delivery'),
    ('recovery_scan',    'Recovery Scan'),
    ('recovery_execute', 'Recovery Execute'),
]

print()
for stage_key, stage_label in STAGES:
    try:
        plan       = resolve_stage_rules(stage_key)
        enabled    = plan['enabled']
        n_matched  = len(plan['matched_rules'])
        n_all      = len(plan['all_rules'])
        action     = plan.get('effective_action', {})
        scope      = plan.get('effective_scope', {})

        warna  = G if enabled else R
        status = "AKTIF" if enabled else "TIDAK AKTIF"
        print(f"\n  {warna}[{status}]{RESET} {W}{stage_label}{RESET} — rules match: {n_matched}/{n_all}")

        if not enabled:
            for rule in plan['all_rules']:
                alasan = rule.get('match_reason', [])
                info(f"Rule #{rule.get('id','?')}: {', '.join(alasan)}")
            catat("RULE", f"{stage_label} tidak aktif")

        if stage_key == 'delivery':
            info(f"limit={action.get('limit')} | retry_delay={action.get('retry_delay_minutes')}m | require_draft={action.get('require_active_draft')} | require_online={action.get('require_online_sender')}")
            info(f"target_status scope: {scope.get('target_status_in')}")
        elif stage_key == 'assignment':
            info(f"limit={action.get('limit')} | prefer_joined={action.get('prefer_joined_owner')} | scope: {scope.get('assignment_status_in')}")
        elif stage_key in ('recovery_scan','recovery_execute'):
            info(f"watch: {scope.get('watch_entities', scope.get('entity_types'))}")

    except Exception as e:
        err(f"{stage_label}: {e}")
        catat("RULE", f"{stage_label} error: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 3: KONDISI SEMUA GRUP
# ════════════════════════════════════════════════════════════════
header(3, "KONDISI SEMUA GRUP")

conn = get_conn()
try:
    assignment_count = {}
    broadcast_count  = {}
    for g in grup_list:
        ast = g.get('assignment_status') or 'ready_assign'
        bst = g.get('broadcast_status')  or 'hold'
        assignment_count[ast] = assignment_count.get(ast, 0) + 1
        broadcast_count[bst]  = broadcast_count.get(bst, 0) + 1

    sub("Assignment Status")
    for st, cnt in sorted(assignment_count.items(), key=lambda x: -x[1]):
        warna = G if st == 'managed' else Y if st == 'assigned' else R if st == 'failed' else W
        print(f"    {warna}{st:<25}{RESET} {cnt} grup")

    sub("Broadcast Status")
    for st, cnt in sorted(broadcast_count.items(), key=lambda x: -x[1]):
        warna = G if st == 'broadcast_eligible' else R if st == 'blocked' else Y
        print(f"    {warna}{st:<25}{RESET} {cnt} grup")

    RINGKASAN['assignment_count'] = assignment_count
    RINGKASAN['broadcast_count']  = broadcast_count

    sub("Deteksi Masalah Grup")

    q = lambda sql, *p: conn.execute(sql, p).fetchone()['n']

    n1 = q("""SELECT COUNT(*) as n FROM grup g WHERE g.status='active' AND g.assignment_status='ready_assign'
               AND g.owner_phone IS NOT NULL AND g.owner_phone IN
               (SELECT phone FROM akun WHERE COALESCE(status,'active') IN ('banned','restricted','suspended','session_expired'))""")
    if n1 > 0: warn(f"{n1} grup TERBENGKALAI — ready_assign + owner tidak aktif"); catat("GRUP", f"{n1} grup terbengkalai")
    else: ok("Tidak ada grup terbengkalai (kondisi 1)")

    n2 = q("""SELECT COUNT(*) as n FROM grup g WHERE g.status='active' AND g.assignment_status='assigned'
               AND g.owner_phone IN
               (SELECT phone FROM akun WHERE COALESCE(status,'active') IN ('banned','restricted','suspended','session_expired'))""")
    if n2 > 0: warn(f"{n2} grup ASSIGNED ke akun tidak aktif"); catat("GRUP", f"{n2} grup assigned ke akun tidak aktif")
    else: ok("Tidak ada grup assigned ke akun tidak aktif (kondisi 2)")

    n3 = q("""SELECT COUNT(*) as n FROM grup g WHERE g.status='active' AND g.assignment_status='managed'
               AND g.broadcast_status IN ('queued','hold')
               AND g.broadcast_hold_reason NOT IN ('sender_daily_limit','daily_limit_exhausted','flood_wait',
                   'approval_pending','new_assignment_wait','stabilization_wait',
                   'recovered_assignment_wait','sender_diblokir_spambot')
               AND (g.broadcast_ready_at IS NULL OR g.broadcast_ready_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
               AND g.diupdate <= TO_CHAR(NOW() - INTERVAL '60 minutes','YYYY-MM-DD HH24:MI:SS')""")
    if n3 > 0: warn(f"{n3} grup STUCK di broadcast > 60 menit"); catat("GRUP", f"{n3} grup stuck broadcast")
    else: ok("Tidak ada grup stuck broadcast > 60 menit (kondisi 3)")

    n4 = q("SELECT COUNT(*) as n FROM grup WHERE status='active' AND broadcast_status='blocked'")
    if n4 > 0: warn(f"{n4} grup BLOCKED permanen")
    else: ok("Tidak ada grup blocked permanen")

    n5 = q("SELECT COUNT(*) as n FROM grup WHERE status='active' AND COALESCE(permission_status,'unknown')='unknown'")
    if n5 > 0: warn(f"{n5} grup belum permission (unknown)")
    else: ok("Semua grup sudah permission")

    n6 = q("""SELECT COUNT(*) as n FROM grup WHERE status='active' AND assignment_status='managed'
               AND broadcast_status != 'broadcast_eligible'""")
    if n6 > 0: info(f"{n6} grup managed belum broadcast_eligible (wajar jika baru join/cooldown)")

    RINGKASAN.update({'grup_terbengkalai':int(n1),'grup_assigned_banned':int(n2),
                      'grup_stuck_broadcast':int(n3),'grup_blocked':int(n4)})
except Exception as e:
    err(f"Demo 3: {e}")
finally:
    conn.close()


# ════════════════════════════════════════════════════════════════
# DEMO 4: SIMULASI AUTO JOIN — SEMUA KONDISI
# ════════════════════════════════════════════════════════════════
header(4, "SIMULASI AUTO JOIN — SEMUA KONDISI")

try:
    conn = get_conn()
    perlu_join = conn.execute("""
        SELECT g.id, g.nama, g.username, g.owner_phone,
               g.join_hold_reason, COALESCE(g.join_attempt_count,0) as attempts
        FROM grup g
        WHERE g.assignment_status='assigned' AND g.owner_phone IS NOT NULL AND g.status='active'
          AND COALESCE(g.broadcast_status,'hold') != 'blocked'
          AND (g.join_ready_at IS NULL OR g.join_ready_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'))
          AND NOT EXISTS (SELECT 1 FROM akun_grup ag WHERE ag.phone=g.owner_phone AND ag.grup_id=g.id)
        ORDER BY g.score DESC LIMIT 30
    """).fetchall()
    conn.close()

    RINGKASAN['grup_perlu_join'] = len(perlu_join)
    print(f"\n  Grup perlu di-join: {W}{len(perlu_join)}{RESET}")

    hasil = {'siap':0,'throttle':0,'kuota_habis':0,'owner_tidak_aktif':0,'hold_retry':0,'private':0}

    for row in perlu_join:
        phone       = row['owner_phone']
        nama        = row['nama'] or str(row['id'])
        username    = row['username'] or '-'
        hold_reason = row['join_hold_reason'] or ''
        attempts    = int(row['attempts'])

        akun_info   = next((a for a in akun_list if a['phone'] == phone), None)
        if not akun_info:
            err(f"'{nama}' — owner {phone} tidak ada di DB"); catat("JOIN", f"'{nama}' owner hilang"); continue

        status_akun = akun_info.get('status') or 'active'

        if status_akun in ('banned','restricted','suspended','session_expired'):
            warn(f"'{nama}' — owner {phone} [{status_akun}] → self-healing akan tangani")
            hasil['owner_tidak_aktif'] += 1; continue

        if hold_reason in ('join_failed_private_or_banned','invalid_target_final'):
            err(f"'{nama}' — PRIVATE/BANNED (@{username})"); hasil['private'] += 1; continue

        if hold_reason == 'join_retry_wait':
            warn(f"'{nama}' — menunggu retry ({attempts}/2)"); hasil['hold_retry'] += 1; continue

        boleh, alasan = _join_boleh_sekarang(phone)
        if not boleh:
            info(f"'{nama}' — throttle: {alasan}"); hasil['throttle'] += 1; continue

        quota = _join_quota_snapshot(phone)
        if quota['limit'] > 0 and quota['remaining'] <= 0:
            warn(f"'{nama}' — kuota join habis ({quota['used']}/{quota['limit']})"); hasil['kuota_habis'] += 1; continue

        ok(f"'{nama}' — SIAP JOIN (@{username}) oleh {phone}"); hasil['siap'] += 1

    sub("Rangkuman")
    for k, v in hasil.items():
        warna = G if k=='siap' else R if k in ('owner_tidak_aktif','private') else Y
        print(f"    {warna}{k:<22}{RESET} {v}")

except Exception as e:
    err(f"Demo 4: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 5: SIMULASI AUTO ASSIGNMENT
# ════════════════════════════════════════════════════════════════
header(5, "SIMULASI AUTO ASSIGNMENT — SEMUA KONDISI")

try:
    conn = get_conn()
    siap_assign = conn.execute("""
        SELECT g.id, g.nama, g.assignment_status, g.permission_status, g.owner_phone
        FROM grup g WHERE g.status='active'
        AND COALESCE(g.assignment_status,'ready_assign') IN
            ('ready_assign','retry_wait','reassign_pending','failed','assigned')
        AND COALESCE(g.permission_status,'unknown') IN
            ('valid','owned','admin','partner_approved','opt_in')
        ORDER BY g.score DESC LIMIT 20
    """).fetchall()
    conn.close()

    RINGKASAN['grup_siap_assign'] = len(siap_assign)
    print(f"\n  Grup bisa di-assign: {W}{len(siap_assign)}{RESET}")

    hasil = {'ada_kandidat':0,'tidak_ada_kandidat':0}
    for row in siap_assign[:10]:
        group_id = int(row['id'])
        nama     = row['nama'] or str(group_id)
        try:
            best, candidates = _choose_candidate(group_id)
            if best:
                ok(f"'{nama}' — kandidat: {best.get('account_id')} score={best.get('ranking_score',0)} beban={best.get('active_assignment_count',0)}")
                hasil['ada_kandidat'] += 1
            else:
                warn(f"'{nama}' — TIDAK ADA kandidat"); catat("ASSIGN", f"'{nama}' tidak ada kandidat"); hasil['tidak_ada_kandidat'] += 1
        except Exception as e:
            err(f"'{nama}': {e}")

    sub("Rangkuman")
    for k, v in hasil.items():
        warna = G if k=='ada_kandidat' else R
        print(f"    {warna}{k:<25}{RESET} {v}")

except Exception as e:
    err(f"Demo 5: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 6: SEND GUARD — SEMUA KONDISI GRUP MANAGED
# ════════════════════════════════════════════════════════════════
header(6, "SEND GUARD — EVALUASI SEMUA GRUP MANAGED")

try:
    conn = get_conn()
    grup_managed = conn.execute("""
        SELECT g.id, g.nama, g.owner_phone, g.broadcast_status,
               g.last_chat, g.last_kirim, g.broadcast_hold_reason, g.idle_days
        FROM grup g WHERE g.status='active' AND g.assignment_status='managed'
        ORDER BY g.score DESC LIMIT 50
    """).fetchall()
    conn.close()

    guard_count = {}
    print(f"\n  Grup managed: {W}{len(grup_managed)}{RESET}")

    for row in grup_managed:
        row_dict = dict(row)
        nama   = row_dict.get('nama') or str(row_dict['id'])
        bst    = row_dict.get('broadcast_status') or 'hold'

        if bst == 'blocked':
            guard_count['broadcast_blocked'] = guard_count.get('broadcast_blocked', 0) + 1; continue
        if bst == 'cooldown':
            guard_count['cooldown'] = guard_count.get('cooldown', 0) + 1; continue

        try:
            guard  = evaluate_group_send_guard(row_dict)
            reason = guard.get('send_guard_reason_code','unknown')
            idle   = guard.get('idle_days')
            guard_count[reason] = guard_count.get(reason, 0) + 1

            if not guard['send_eligible']:
                if reason == 'inactive_group':
                    info(f"'{nama}' sepi {idle} hari")
                elif reason == 'last_chat_is_our_message':
                    info(f"'{nama}' chat terakhir milik sendiri")
        except Exception as e:
            err(f"'{nama}': {e}")

    sub("Rangkuman send guard")
    for k, v in sorted(guard_count.items(), key=lambda x: -x[1]):
        warna = G if k == 'eligible' else R if k == 'broadcast_blocked' else Y
        print(f"    {warna}{k:<35}{RESET} {v} grup")

    RINGKASAN['guard_eligible'] = guard_count.get('eligible', 0)
    if guard_count.get('eligible', 0) == 0 and len(grup_managed) > 0:
        catat("BROADCAST", "Tidak ada grup lolos send guard — broadcast tidak akan jalan")

except Exception as e:
    err(f"Demo 6: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 7: STATUS CAMPAIGN DAN TARGET
# ════════════════════════════════════════════════════════════════
header(7, "STATUS CAMPAIGN DAN TARGET BROADCAST")

try:
    conn = get_conn()

    campaigns = conn.execute("""
        SELECT c.id, c.name, c.status,
               SUM(CASE WHEN ct.status='queued'   THEN 1 ELSE 0 END) as queued,
               SUM(CASE WHEN ct.status='eligible' THEN 1 ELSE 0 END) as eligible,
               SUM(CASE WHEN ct.status='sending'  THEN 1 ELSE 0 END) as sending,
               SUM(CASE WHEN ct.status='sent'     THEN 1 ELSE 0 END) as sent,
               SUM(CASE WHEN ct.status='failed'   THEN 1 ELSE 0 END) as failed,
               SUM(CASE WHEN ct.status='blocked'  THEN 1 ELSE 0 END) as blocked,
               SUM(CASE WHEN ct.status='skipped'  THEN 1 ELSE 0 END) as skipped,
               COUNT(ct.id) as total
        FROM campaign c LEFT JOIN campaign_target ct ON ct.campaign_id=c.id
        WHERE c.status IN ('queued','running','paused')
        GROUP BY c.id ORDER BY c.id DESC LIMIT 5
    """).fetchall()

    for c in campaigns:
        warna = G if c['status']=='running' else Y
        print(f"\n  {warna}[{c['status'].upper()}]{RESET} #{c['id']}: {c['name']}")
        info(f"queued={c['queued']} sent={c['sent']} failed={c['failed']} blocked={c['blocked']} skipped={c['skipped']} sending={c['sending']} total={c['total']}")
        if int(c['sending'] or 0) > 0:
            warn(f"Ada {c['sending']} target masih 'sending'")

    stuck_sending = conn.execute("""
        SELECT COUNT(*) as n FROM campaign_target WHERE status='sending'
        AND last_attempt_at <= TO_CHAR(NOW() - INTERVAL '10 minutes','YYYY-MM-DD HH24:MI:SS')
    """).fetchone()['n']

    n_spambot = conn.execute("""
        SELECT COUNT(*) as n FROM campaign_target WHERE hold_reason='sender_diblokir_spambot'
    """).fetchone()['n']

    hold_reasons = conn.execute("""
        SELECT hold_reason, COUNT(*) as n FROM campaign_target
        WHERE status='queued' AND hold_reason IS NOT NULL AND hold_reason!=''
        GROUP BY hold_reason ORDER BY n DESC LIMIT 15
    """).fetchall()
    conn.close()

    sub("Deteksi masalah target")
    if stuck_sending > 0: warn(f"{stuck_sending} target STUCK di 'sending' > 10 menit"); catat("TARGET", f"{stuck_sending} stuck sending")
    else: ok("Tidak ada target stuck sending")

    if n_spambot > 0: warn(f"{n_spambot} target hold karena SpamBot"); catat("SPAMBOT", f"{n_spambot} target ditahan")
    else: ok("Tidak ada target hold karena SpamBot")

    if hold_reasons:
        sub("Hold reason")
        for hr in hold_reasons:
            warna = R if hr['hold_reason'] in ('sender_diblokir_spambot','broadcast_blacklisted','broadcast_blacklisted_max_attempt') else Y
            print(f"    {warna}{hr['hold_reason']:<42}{RESET} {hr['n']}")

    RINGKASAN.update({'campaign_aktif':len(campaigns),'stuck_sending':int(stuck_sending),'target_spambot':int(n_spambot)})

except Exception as e:
    err(f"Demo 7: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 8: SENDER AVAILABILITY
# ════════════════════════════════════════════════════════════════
header(8, "SENDER AVAILABILITY UNTUK BROADCAST")

try:
    from services.account_manager import _clients
    senders_online = list(_clients.keys()) if _clients else []
    senders_siap   = _available_online_senders()

    print(f"\n  Akun online di memori : {W}{len(senders_online)}{RESET}")
    print(f"  Sender siap broadcast : {W}{len(senders_siap)}{RESET}")

    if not senders_online:
        warn("Tidak ada akun online — jalankan app.py dulu atau semua akun terputus")
        catat("SENDER", "Tidak ada akun online di memori")
    else:
        for phone in senders_online:
            boleh, alasan = _broadcast_boleh_kirim_sekarang(phone)
            tersedia = _sender_available_for_delivery(phone, require_online_sender=True)
            quota    = _send_quota_snapshot(phone)
            print(f"\n  {phone}")
            info(f"tersedia={tersedia} | boleh={'ya' if boleh else 'tidak — '+alasan}")
            info(f"kuota: {quota.get('used',0)}/{quota.get('limit',0)} (sisa {quota.get('remaining',0)})")

    RINGKASAN.update({'senders_online':len(senders_online),'senders_siap':len(senders_siap)})

except Exception as e:
    err(f"Demo 8: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 9: RECOVERY — SEMUA KONDISI
# ════════════════════════════════════════════════════════════════
header(9, "KONDISI RECOVERY — SEMUA ITEM")

try:
    conn = get_conn()
    items = conn.execute("""
        SELECT entity_type, problem_type, recovery_status, COUNT(*) as n
        FROM recovery_item GROUP BY entity_type, problem_type, recovery_status
        ORDER BY recovery_status, n DESC
    """).fetchall()

    assign_macet = conn.execute("""
        SELECT COUNT(*) as n FROM group_assignment
        WHERE status IN ('assigned','retry_wait','failed')
        AND updated_at <= TO_CHAR(NOW() - INTERVAL '30 minutes','YYYY-MM-DD HH24:MI:SS')
    """).fetchone()['n']

    scrape_macet = conn.execute("""
        SELECT COUNT(*) as n FROM scrape_job
        WHERE status IN ('queued','running','paused')
        AND dibuat <= TO_CHAR(NOW() - INTERVAL '30 minutes','YYYY-MM-DD HH24:MI:SS')
    """).fetchone()['n']
    conn.close()

    if items:
        sub("Recovery items")
        for item in items:
            warna = G if item['recovery_status']=='recovered' else R if item['recovery_status'] in ('recovery_needed','recoverable') else Y
            print(f"    {warna}[{item['recovery_status']}]{RESET} {item['entity_type']}/{item['problem_type']}: {item['n']}")
    else:
        ok("Tidak ada recovery item")

    if assign_macet > 0: warn(f"{assign_macet} assignment macet > 30 menit"); catat("RECOVERY", f"{assign_macet} assignment macet")
    else: ok("Tidak ada assignment macet")

    if scrape_macet > 0: warn(f"{scrape_macet} scrape job macet > 30 menit"); catat("RECOVERY", f"{scrape_macet} scrape macet")
    else: ok("Tidak ada scrape job macet")

    RINGKASAN['recovery_items'] = len(items)

except Exception as e:
    err(f"Demo 9: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 10: SELF-HEALING CHECK
# ════════════════════════════════════════════════════════════════
header(10, "SELF-HEALING — KONDISI YANG AKAN DIPERBAIKI OTOMATIS")

try:
    conn = get_conn()
    cek1 = conn.execute("""
        SELECT g.id, g.nama, g.owner_phone, a.status as ast
        FROM grup g JOIN akun a ON a.phone=g.owner_phone
        WHERE g.assignment_status='ready_assign' AND g.status='active'
        AND COALESCE(a.status,'active') IN ('banned','restricted','suspended','session_expired')
        LIMIT 10
    """).fetchall()

    cek2 = conn.execute("""
        SELECT g.id, g.nama, g.owner_phone, a.status as ast, g.diupdate
        FROM grup g JOIN akun a ON a.phone=g.owner_phone
        WHERE g.assignment_status='assigned' AND g.status='active'
        AND COALESCE(a.status,'active') IN ('banned','restricted','suspended','session_expired')
        AND (g.diupdate IS NULL OR g.diupdate <= TO_CHAR(NOW() - INTERVAL '60 minutes','YYYY-MM-DD HH24:MI:SS'))
        LIMIT 10
    """).fetchall()
    conn.close()

    sub("Kondisi 1: ready_assign + owner tidak aktif")
    if cek1:
        for r in cek1:
            warn(f"'{r['nama']}' — owner {r['owner_phone']} [{r['ast']}]")
            info("-> self-healing bersihkan owner_phone -> stage_assignment assign ulang")
    else: ok("Bersih")

    sub("Kondisi 2: assigned + owner tidak aktif > 60 menit")
    if cek2:
        for r in cek2:
            warn(f"'{r['nama']}' — owner {r['owner_phone']} [{r['ast']}] update:{r['diupdate']}")
            info("-> self-healing reset ke ready_assign")
    else: ok("Bersih")

    RINGKASAN.update({'self_heal_1':len(cek1),'self_heal_2':len(cek2)})

except Exception as e:
    err(f"Demo 10: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 11: POTENSI RISIKO AKUN DIBLOKIR
# ════════════════════════════════════════════════════════════════
header(11, "POTENSI RISIKO AKUN DIBLOKIR TELEGRAM")

try:
    conn = get_conn()
    print()
    for akun in akun_list:
        phone  = akun['phone']
        status = akun.get('status') or 'active'
        level  = akun.get('level_warming') or 1
        risiko = []

        if status in ('banned','restricted'):
            risiko.append(f"{R}SUDAH DIBLOKIR/RESTRICTED{RESET}")

        try:
            cap   = get_daily_capacity(phone)
            join_u = cap['join']['used']; join_l = cap['join']['limit']
            if join_l > 0 and join_u / max(1,join_l) > 0.9:
                risiko.append(f"{Y}Join mendekati batas ({join_u}/{join_l}){RESET}")
        except: pass

        if level == 1:
            try:
                batas_j = int(_gs('w1_maks_join', 5) or 5)
                if batas_j > 10: risiko.append(f"{Y}Level 1 kuota join {batas_j} — risiko tinggi{RESET}")
            except: pass

        try:
            gagal = conn.execute("""
                SELECT COUNT(*) as n FROM riwayat_aktivitas
                WHERE phone=%s AND jenis='kirim' AND status='gagal'
                AND dibuat >= TO_CHAR(NOW() - INTERVAL '24 hours','YYYY-MM-DD HH24:MI:SS')
            """, (phone,)).fetchone()['n']
            if gagal >= 5: risiko.append(f"{R}Gagal kirim {gagal}x dalam 24 jam{RESET}"); catat("RISIKO", f"{phone}: gagal kirim {gagal}x")
            elif gagal >= 2: risiko.append(f"{Y}Gagal kirim {gagal}x dalam 24 jam{RESET}")
        except: pass

        try:
            flood = conn.execute("""
                SELECT COUNT(*) as n FROM riwayat_aktivitas
                WHERE phone=%s AND keterangan LIKE '%flood%'
                AND dibuat >= TO_CHAR(NOW() - INTERVAL '24 hours','YYYY-MM-DD HH24:MI:SS')
            """, (phone,)).fetchone()['n']
            if flood >= 3: risiko.append(f"{R}FloodWait {flood}x dalam 24 jam{RESET}"); catat("RISIKO", f"{phone}: FloodWait {flood}x")
            elif flood >= 1: risiko.append(f"{Y}FloodWait {flood}x dalam 24 jam{RESET}")
        except: pass

        warna_ph = R if any('SUDAH' in r for r in risiko) else Y if risiko else G
        print(f"  {warna_ph}{phone}{RESET}")
        if risiko:
            for r in risiko: print(f"    ! {r}")
        else: ok("Tidak ada risiko terdeteksi")

    conn.close()

except Exception as e:
    err(f"Demo 11: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 12: SIMULASI JEDA JOIN DAN BROADCAST
# ════════════════════════════════════════════════════════════════
header(12, "SIMULASI KALKULASI JEDA JOIN DAN BROADCAST")

import random
random.seed(42)

def sim_jeda_join(level, sisa, jam):
    BATAS = {1:{'min':1800,'max':14400},2:{'min':900,'max':7200},
             3:{'min':300,'max':5400},  4:{'min':120,'max':3600}}
    b = BATAS.get(level, BATAS[4])
    now = datetime.now().replace(hour=jam, minute=0, second=0)
    tmalam = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    sisa_d = max(300, int((tmalam - now).total_seconds()))
    jeda = int(sisa_d / max(1, sisa) * random.uniform(0.8, 1.2))
    return max(b['min'], min(b['max'], jeda)) // 60

sub("Jeda join berbagai kondisi")
print(f"\n    {'Kondisi':<38} {'Jeda'}")
print(f"    {'-'*50}")
kasus = [
    (1,20,8,'L1 pagi, kuota penuh'),    (1,10,12,'L1 siang, setengah kuota'),
    (1,3,20,'L1 malam, hampir habis'),  (2,10,8,'L2 pagi'),
    (2,3,18,'L2 sore, hampir habis'),   (3,20,8,'L3 pagi'),
    (3,5,16,'L3 sore'),                 (4,30,8,'L4 pagi, penuh'),
    (4,10,14,'L4 siang'),               (4,3,20,'L4 malam, hampir habis'),
]
for level, kuota, jam, ket in kasus:
    jeda = sim_jeda_join(level, kuota, jam)
    warna = G if jeda < 60 else Y if jeda < 180 else R
    print(f"    L{level} sisa={kuota:>2} jam={jam:02d}:00 {ket:<22} {warna}{jeda:>4} menit{RESET}")

sub("Jeda broadcast akun aktif saat ini")
jam_mulai   = int(_gs('broadcast_jam_mulai', 6) or 6)
jam_selesai = int(_gs('broadcast_jam_selesai', 22) or 22)
jeda_min    = int(_gs('broadcast_jeda_min_menit', 1) or 1)
jeda_max    = int(_gs('broadcast_jeda_max_menit', 10) or 10)
print(f"\n    Jam aktif: {jam_mulai}:00-{jam_selesai}:00 | Jeda: {jeda_min}-{jeda_max} menit\n")

for akun in akun_list:
    if akun.get('status') not in ('active','online'): continue
    phone = akun['phone']
    try:
        jeda  = _hitung_jeda_broadcast(phone)
        quota = _send_quota_snapshot(phone)
        warna = G if jeda < 5 else Y if jeda < 30 else R
        print(f"    {phone}")
        info(f"jeda={warna}{jeda:.1f}m{RESET} | kuota={quota.get('used',0)}/{quota.get('limit',0)} sisa={quota.get('remaining',0)}")
    except Exception as e:
        warn(f"  {phone}: {e}")


# ════════════════════════════════════════════════════════════════
# DEMO 13: SETTINGS KRITIS
# ════════════════════════════════════════════════════════════════
header(13, "VERIFIKASI SETTINGS KRITIS")

SETTINGS_CEK = [
    ('auto_join_enabled',            'Auto Join aktif',               '1',  True),
    ('auto_import_enabled',          'Auto Import aktif',             '1',  False),
    ('auto_permission_enabled',      'Auto Permission aktif',         '1',  False),
    ('auto_assign_enabled',          'Auto Assign aktif',             '1',  True),
    ('auto_campaign_enabled',        'Auto Broadcast aktif',          '1',  True),
    ('auto_recovery_enabled',        'Auto Recovery aktif',           '1',  True),
    ('broadcast_throttle_enabled',   'Throttle broadcast',            '1',  True),
    ('pause_all_automation',         'PAUSE semua (harus 0)',         '0',  True),
    ('maintenance_mode',             'Maintenance mode (harus 0)',    '0',  True),
    ('w1_maks_join',                 'Kuota join Level 1',            '20', False),
    ('w2_maks_join',                 'Kuota join Level 2',            '10', False),
    ('w3_maks_join',                 'Kuota join Level 3',            '20', False),
    ('w4_maks_join',                 'Kuota join Level 4',            '30', False),
    ('broadcast_jeda_min_menit',     'Jeda broadcast min (menit)',    '1',  False),
    ('broadcast_jeda_max_menit',     'Jeda broadcast max (menit)',    '10', False),
    ('broadcast_jam_mulai',          'Jam mulai broadcast',           '6',  False),
    ('broadcast_jam_selesai',        'Jam selesai broadcast',         '22', False),
    ('orchestrator_interval_seconds','Interval siklus (detik)',       '10', True),
    ('auto_join_max_per_cycle',      'Max join per siklus',           '1',  False),
]

print()
for key, label, expected, kritis in SETTINGS_CEK:
    val = _gs(key, None)
    if val is None:
        warn(f"{label:<45} belum diset")
    elif str(val) != str(expected) and kritis:
        err(f"{label:<45} = {R}{val}{RESET} (expected: {expected})")
        catat("SETTINGS", f"{label} = {val}")
    elif str(val) != str(expected):
        warn(f"{label:<45} = {Y}{val}{RESET} (default: {expected})")
    else:
        ok(f"{label:<45} = {G}{val}{RESET}")


# ════════════════════════════════════════════════════════════════
# DEMO 14: AKTIVITAS HARI INI
# ════════════════════════════════════════════════════════════════
header(14, "AKTIVITAS HARI INI")

try:
    conn = get_conn()
    hari_ini = datetime.now().strftime('%Y-%m-%d')
    aktivitas = conn.execute("""
        SELECT jenis, status, COUNT(*) as n FROM riwayat_aktivitas
        WHERE dibuat >= %s GROUP BY jenis, status ORDER BY jenis, n DESC
    """, (hari_ini,)).fetchall()
    conn.close()

    if aktivitas:
        jenis_now = None
        for row in aktivitas:
            if row['jenis'] != jenis_now:
                jenis_now = row['jenis']
                print(f"\n  {C}{jenis_now.upper()}{RESET}")
            warna = G if row['status'] in ('berhasil','sukses','joined') else R if row['status'] in ('gagal','error') else Y
            print(f"    {warna}{row['status']:<22}{RESET} {row['n']}x")
    else:
        info("Belum ada aktivitas hari ini")
except Exception as e:
    err(f"Demo 14: {e}")


# ════════════════════════════════════════════════════════════════
# RANGKUMAN AKHIR
# ════════════════════════════════════════════════════════════════
header("END", "RANGKUMAN AKHIR")

print(f"""
  {W}STATISTIK SISTEM:{RESET}
    Total akun             : {RINGKASAN.get('total_akun',0)}
    Status akun            : {json.dumps(RINGKASAN.get('status_akun',{}), ensure_ascii=False)}
    Total grup             : {RINGKASAN.get('total_grup',0)}
    Grup perlu join        : {RINGKASAN.get('grup_perlu_join',0)}
    Grup siap assign       : {RINGKASAN.get('grup_siap_assign',0)}
    Grup terbengkalai      : {R if RINGKASAN.get('grup_terbengkalai',0)>0 else G}{RINGKASAN.get('grup_terbengkalai',0)}{RESET}
    Grup stuck broadcast   : {R if RINGKASAN.get('grup_stuck_broadcast',0)>0 else G}{RINGKASAN.get('grup_stuck_broadcast',0)}{RESET}
    Grup blocked permanen  : {R if RINGKASAN.get('grup_blocked',0)>0 else G}{RINGKASAN.get('grup_blocked',0)}{RESET}
    Grup lolos send guard  : {G if RINGKASAN.get('guard_eligible',0)>0 else R}{RINGKASAN.get('guard_eligible',0)}{RESET}
    Sender online          : {RINGKASAN.get('senders_online',0)}
    Sender siap kirim      : {RINGKASAN.get('senders_siap',0)}
    Campaign aktif         : {RINGKASAN.get('campaign_aktif',0)}
    Target stuck sending   : {R if RINGKASAN.get('stuck_sending',0)>0 else G}{RINGKASAN.get('stuck_sending',0)}{RESET}
    Target hold SpamBot    : {R if RINGKASAN.get('target_spambot',0)>0 else G}{RINGKASAN.get('target_spambot',0)}{RESET}
    Self-heal kondisi 1    : {R if RINGKASAN.get('self_heal_1',0)>0 else G}{RINGKASAN.get('self_heal_1',0)}{RESET}
    Self-heal kondisi 2    : {R if RINGKASAN.get('self_heal_2',0)>0 else G}{RINGKASAN.get('self_heal_2',0)}{RESET}
    Recovery items         : {RINGKASAN.get('recovery_items',0)}
""")

if MASALAH:
    print(f"  {R}{BOLD}MASALAH TERDETEKSI ({len(MASALAH)}):{RESET}")
    for i, m in enumerate(MASALAH, 1):
        print(f"    {R}{i}.{RESET} {m}")
else:
    print(f"  {G}{BOLD}Tidak ada masalah kritis terdeteksi{RESET}")

print(f"\n{B}{'='*70}{RESET}")
print(f"{W}Demo selesai — paste seluruh output ini ke Claude untuk analisa.{RESET}")
print(f"{B}{'='*70}{RESET}\n")
