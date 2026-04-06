# Alur Otomasi Sistem

## Peta besar
Sistem ini bekerja dalam pipa berikut:

1. **Scraper** mengambil kandidat grup dari keyword.
2. **Import** memasukkan hasil scraper ke tabel `grup`.
3. **Permission** memberi status izin agar grup legal dipakai untuk campaign.
4. **Assignment** memilih akun owner untuk grup.
5. **Broadcast/Campaign** mengirim pesan dengan akun yang sudah cocok.
6. **Recovery** memantau entitas macet lalu memulihkannya.

## Status utama yang perlu dibaca
- `permission_status`
  - `unknown`/`pending`: belum siap ke tahap berikutnya.
  - `valid`: boleh lanjut ke assignment.
- `assignment_status`
  - `ready_assign`: menunggu owner akun.
  - `assigned`/`managed`: sudah punya owner.
  - `released`/`failed`: perlu ditinjau atau diassign ulang.
- `broadcast_status`
  - `hold`: belum siap kirim.
  - `broadcast_eligible`: siap masuk queue/campaign.
  - `broadcast_blocked`: diblok dari pengiriman.

## Toggle yang benar-benar mempengaruhi engine
- `maintenance_mode=1` → semua worker otomatis berhenti.
- `pause_all_automation=1` → semua worker otomatis berhenti.
- `auto_import_enabled=1` → hasil scraper dapat diimpor otomatis.
- `auto_assign_enabled=1` → grup valid bisa diassign otomatis.
- `auto_campaign_enabled=1` → broadcast/campaign background bisa berjalan.
- `auto_recovery_enabled=1` → recovery background diizinkan berjalan.

## Endpoint penting untuk debugging
- `GET /api/health` → status backend dasar.
- `GET /api/flow` → ringkasan alur pipeline legacy JSON.
- `GET /api/v2/overview/flow` → pipeline v2 yang lebih rapi.
- `GET /api/v2/overview/health` → status kesehatan backend dan queue.
- `GET /api/v2/logs/summary` → error, warning, dan recovery harian.

## Cara baca bottleneck
- `scrape_results_ready_import` tinggi → hasil scrape menumpuk sebelum import.
- `groups_missing_permission` tinggi → permission worker tertinggal.
- `groups_ready_assign` tinggi → grup valid belum mendapat owner akun.
- `groups_broadcast_eligible` tinggi → grup siap kirim, tetapi belum diproses queue/campaign.
- `recovery_needed` tinggi → banyak entitas macet atau tidak konsisten.

## Pola debugging yang disarankan
1. Cek `GET /api/health`.
2. Cek `GET /api/flow` atau `GET /api/v2/overview/flow`.
3. Jika semua toggle aktif tetapi stage tidak bergerak, cek `maintenance_mode` dan `pause_all_automation`.
4. Jika campaign tidak berjalan, cek draft aktif, akun online, dan group ownership.
5. Jika 404 pada endpoint API, backend kini akan mengembalikan 404 JSON yang benar, bukan 500 palsu.
