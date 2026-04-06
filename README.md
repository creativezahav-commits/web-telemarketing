# Telegram Dashboard — Final Serious Baseline

Baseline kode ini adalah versi yang lebih matang untuk **uji operasional serius**:
- struktur backend lama tetap dipertahankan agar tidak memutus flow yang sudah jalan
- ditambahkan **API v2 modular** untuk dashboard-driven control
- database tetap menjadi sumber data utama
- route lama tetap ada untuk kompatibilitas frontend lama

## Yang dipertahankan
- `backend/app.py` sebagai entry point Flask
- `services/account_manager.py` untuk login/session akun Telegram
- `services/group_manager.py` untuk fetch grup dari akun
- `services/message_service.py` untuk pengiriman satu target
- `utils/database.py` dan `utils/storage_db.py` sebagai fondasi data

## Yang ditambahkan
- `backend/routes/` untuk endpoint modular v2
- `backend/services/overview_service.py` untuk ringkasan dashboard
- `backend/utils/api.py` untuk response standar
- kolom database tambahan untuk akun, grup, dan hasil scrape agar siap menuju dashboard-driven control

## Namespace API
### Legacy API
Masih aktif agar frontend lama tetap berjalan.

### API v2
Namespace baru ada di:
- `/api/v2/overview/*`
- `/api/v2/settings/*`
- `/api/v2/accounts/*`
- `/api/v2/scraper/*`
- `/api/v2/groups/*`
- `/api/v2/logs/*`

Tujuannya adalah migrasi bertahap ke backend yang lebih terstruktur.

## Setup

```bash
pip install -r requirements.txt
cd backend
python app.py
```

## Catatan penting
Versi ini saya anggap sebagai **baseline final yang matang untuk diuji serius**, tetapi belum boleh dianggap 100% production-hardened untuk skala besar tanpa pengujian runtime nyata pada akun, session, dan server yang akan kamu pakai.


## Tambahan pada versi ini
- route v2 baru untuk permissions, assignments, campaigns, automation rules, dan recovery
- tabel database baru: group_permission, group_assignment, campaign, campaign_target, automation_rule, recovery_item, audit_log
- dokumen implementasi: `IMPLEMENTATION_TASKS_BY_ENGINE.md`
- catatan status paket: `FINAL_STATUS_AND_LIMITS.md`


## Debug cepat
Setelah perubahan kode, jalankan smoke test berikut dari folder project:

```bash
python backend/smoke_test.py
```

Smoke test ini mengecek endpoint inti backend, status pipeline, settings, permissions, assignments, campaigns, recovery, dan memastikan endpoint yang tidak ada mengembalikan **404 JSON** yang benar.

## Endpoint untuk membaca alur otomasi
- `GET /api/flow`
- `GET /api/v2/overview/flow`

Keduanya membantu melihat backlog per tahap: hasil scrape, grup tanpa permission, grup siap assign, grup managed, grup siap broadcast, dan item recovery.
