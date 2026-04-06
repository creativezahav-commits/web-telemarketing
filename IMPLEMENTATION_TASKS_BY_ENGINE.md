# Implementation Tasks by Engine and Project File

Dokumen ini memetakan engine utama ke file project yang harus disentuh agar implementasi tetap rapi dan tidak memutus flow lama.

## 1. Result Pipeline

### File backend utama
- `backend/services/scraper_service.py`
- `backend/utils/storage_db.py`
- `backend/utils/database.py`
- `backend/routes/scraper_routes.py`
- `backend/routes/groups_routes.py`
- `backend/routes/logs_routes.py`

### File frontend utama
- `frontend/scraper.js`
- `frontend/app.js`
- `frontend/style.css`

### Task implementasi
- tambahkan normalized key dan quality fields pada `scrape_result`
- perkuat dedupe di `scraper_service.py`
- expose summary/list/detail/import di `scraper_routes.py`
- import ke `grup` hanya lewat action/import rule
- log event result ke `audit_log`

## 2. Assignment Engine

### File backend utama
- `backend/utils/database.py`
- `backend/utils/storage_db.py`
- `backend/routes/assignments_routes.py`
- `backend/routes/groups_routes.py`
- `backend/routes/accounts_routes.py`

### File frontend utama
- `frontend/app.js`
- `frontend/style.css`
- halaman Assignments baru jika dipisah

### Task implementasi
- buat owner tunggal per group
- buat ranking kandidat akun
- buat status assignment lengkap
- tambah retry/reassign/release
- tampilkan candidate ranking di drawer UI
- catat semua keputusan assign ke `audit_log`

## 3. Recovery Engine

### File backend utama
- `backend/utils/database.py`
- `backend/utils/storage_db.py`
- `backend/routes/recovery_routes.py`
- `backend/services/overview_service.py`
- `backend/app.py`

### File frontend utama
- halaman Recovery baru
- `frontend/app.js`
- `frontend/style.css`

### Task implementasi
- tambah tabel `recovery_item`
- tambah stuck/recovery summary ke overview
- tambah route scan/recover/requeue/partial/ignore
- catat event recovery ke `audit_log`
- siapkan startup recovery bertahap di `app.py`

## 4. Campaign / Delivery Engine

### File backend utama
- `backend/utils/database.py`
- `backend/utils/storage_db.py`
- `backend/routes/campaigns_routes.py`
- `backend/services/message_service.py`
- `backend/services/overview_service.py`

### File frontend utama
- `frontend/broadcast.js`
- halaman Campaigns/Queue baru jika dipisah
- `frontend/style.css`

### Task implementasi
- jadikan campaign pusat operasi delivery
- buat `campaign` dan `campaign_target`
- expose queue list/detail/retry/move-sender/block
- batasi auto-create campaign hanya untuk group dengan permission valid + managed
- tampilkan queue summary di overview

## 5. Settings / Rule Engine

### File backend utama
- `backend/routes/settings_routes.py`
- `backend/routes/automation_routes.py`
- `backend/utils/settings_manager.py`
- `backend/utils/database.py`
- `backend/utils/storage_db.py`

### File frontend utama
- `frontend/settings.js`
- halaman Automation Rules baru jika dipisah
- `frontend/style.css`

### Task implementasi
- pindahkan seluruh setting penting ke database
- kelompokkan settings per scope
- buat CRUD automation rules
- tambah rule testing endpoint
- tambah emergency pause dan pause all rules
- catat perubahan settings/rules ke `audit_log`

## 6. Files legacy yang tetap dipertahankan sementara
- `backend/app.py` (route lama tetap hidup)
- `backend/utils/storage.py`
- `backend/core/message_queue.py`
- `backend/core/send_history.py`

## 7. Files yang sebaiknya dianggap baseline final saat ini
- `backend/routes/*.py`
- `backend/utils/api.py`
- `backend/utils/database.py`
- `backend/utils/storage_db.py`
- `backend/services/overview_service.py`
- `backend/services/scraper_service.py`
- `backend/routes/permissions_routes.py`
- `backend/routes/assignments_routes.py`
- `backend/routes/campaigns_routes.py`
- `backend/routes/automation_routes.py`
- `backend/routes/recovery_routes.py`
