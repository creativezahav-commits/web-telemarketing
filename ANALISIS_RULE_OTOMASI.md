# Analisis dan Penjelasan Automation Rules

## Gambaran besar

Mesin otomasi sekarang dibagi menjadi 7 stage yang berurutan:

1. `import`
2. `permission`
3. `assignment`
4. `campaign_prepare`
5. `delivery`
6. `recovery_scan`
7. `recovery_execute`

Setiap stage **tidak lagi murni hardcoded**. Stage sekarang membaca rule aktif dari tabel `automation_rule`, lalu:

- menormalkan `rule_type` ke stage kanonik
- memeriksa apakah rule aktif
- memeriksa cooldown rule
- mengevaluasi `condition_json`
- menggabungkan `action_json` dan `scope_json`
- menjalankan stage memakai konfigurasi hasil rule
- mencatat `success_count`, `fail_count`, dan `last_triggered_at`

Jika satu stage gagal, orchestrator **tidak mematikan seluruh siklus**. Error stage dicatat ke `stage_errors`, lalu stage berikutnya tetap lanjut.

---

## Struktur rule

Setiap rule memakai 3 blok utama:

### 1. `condition_json`
Menentukan kapan stage boleh berjalan.

Contoh:

```json
{
  "pending_count_gte": 1,
  "online_accounts_gte": 1,
  "active_draft_required": true
}
```

Makna:
- hanya jalan kalau ada item yang layak diproses
- hanya jalan kalau akun online cukup
- hanya jalan kalau draft aktif tersedia

### 2. `action_json`
Menentukan apa yang harus dilakukan dan batas operasionalnya.

Contoh:

```json
{
  "limit": 100,
  "retry_delay_minutes": 10,
  "prefer_joined_owner": true
}
```

Makna:
- berapa item maksimum per siklus
- kapan target gagal boleh dicoba ulang
- apakah assignment mendahulukan akun yang memang sudah join grup

### 3. `scope_json`
Menentukan ruang lingkup entity yang boleh diproses.

Contoh:

```json
{
  "permission_status_in": ["valid", "opt_in"],
  "assignment_status_in": ["managed"],
  "exclude_channels": true
}
```

Makna:
- hanya grup dengan permission tertentu
- hanya grup dengan status assignment tertentu
- channel tidak ikut diproses

---

## Rule default yang sekarang dibuat otomatis

Sistem akan memastikan minimal ada rule default berikut:

### 1. `[SYSTEM] Import done scrape jobs`
**Stage:** `import`

**Logika:**
- cari `scrape_job` dengan status `done`
- cek apakah masih ada `scrape_result.imported=0`
- import ke tabel `grup`

**Tujuan:**
hasil scraper tidak berhenti di tabel hasil scrape, tetapi masuk ke pipeline utama.

**Proteksi crash:**
- hanya memproses job yang match rule
- cooldown 15 detik
- bila gagal, fail_count rule naik, tetapi siklus orchestrator tetap lanjut

### 2. `[SYSTEM] Grant permission to new groups`
**Stage:** `permission`

**Logika:**
- cari grup `active`
- `permission_status='unknown'`
- bukan `channel`
- buat record `group_permission`
- sinkronkan kolom di tabel `grup`

**Tujuan:**
gr up baru langsung punya basis izin operasional sehingga bisa lanjut ke assignment.

**Proteksi crash:**
- batch dibatasi
- approval metadata diisi konsisten
- tidak memproses channel

### 3. `[SYSTEM] Assign best owner to permitted groups`
**Stage:** `assignment`

**Logika:**
- cari grup aktif yang permission-nya valid
- status assignment harus termasuk status yang memang perlu tindakan
- pilih kandidat terbaik
- utamakan akun yang memang sudah join grup
- bila owner ternyata sudah join grup, langsung naik ke `managed`
- bila tidak ada kandidat, buat `recovery_item`

**Tujuan:**
setiap grup punya akun owner operasional yang paling relevan.

**Proteksi crash:**
- no-candidate tidak membuat stage crash
- no-candidate diubah menjadi recovery case
- status grup dan assignment disinkronkan

### 4. `[SYSTEM] Queue managed groups into campaign`
**Stage:** `campaign_prepare`

**Logika:**
- cari grup `managed`
- permission valid
- broadcast status masih eligible
- belum menjadi target campaign aktif lain
- buat campaign baru jika belum ada campaign aktif
- masukkan ke `campaign_target`

**Tujuan:**
gr up yang siap siar masuk antrean broadcast secara otomatis.

**Proteksi crash:**
- menghindari duplikasi target campaign aktif
- bisa reuse campaign aktif
- bila create campaign dimatikan, stage akan skip secara aman

### 5. `[SYSTEM] Deliver queued broadcast targets`
**Stage:** `delivery`

**Logika:**
- hanya berjalan jika ada draft aktif
- hanya berjalan jika ada akun online
- ambil target `queued/eligible/failed` yang sudah jatuh tempo
- pilih sender dari `sender_account_id` atau `owner_phone`
- bila kirim sukses → `sent`
- bila pesan error mengandung kata blokir → `blocked`
- selain itu → `failed` dan dijadwalkan ulang

**Tujuan:**
antrian broadcast benar-benar terkirim otomatis, bukan cuma antre.

**Proteksi crash:**
- tidak memaksa kirim jika draft tidak ada
- tidak memaksa kirim jika sender offline
- target gagal dijadwalkan ulang, bukan mematikan batch
- blocked dibedakan dari failed biasa

### 6. `[SYSTEM] Detect stuck automation items`
**Stage:** `recovery_scan`

**Logika:**
- baca scrape job yang macet
- baca assignment yang terlalu lama pending/gagal
- baca campaign yang tidak mengalami progres
- buat atau update `recovery_item`

**Tujuan:**
sistem punya radar untuk mendeteksi bottleneck otomatis.

**Proteksi crash:**
- threshold per entity bisa diatur rule
- tiap entity dibuat menjadi recovery record, bukan langsung diubah agresif

### 7. `[SYSTEM] Recover recoverable items safely`
**Stage:** `recovery_execute`

**Logika:**
- ambil `recovery_item` yang statusnya `recoverable`
- `scrape_job` → resume
- `assignment` → cari kandidat ulang lalu reassign
- `campaign` → requeue target failed yang sudah due
- hentikan percobaan bila sudah melewati `max_recovery_attempts`

**Tujuan:**
item macet bisa dipulihkan tanpa perlu intervensi manual terus-menerus.

**Proteksi crash:**
- ada batas maksimum recovery attempt
- entity yang hilang ditandai `ignored`, bukan meledakkan proses
- setiap item ditangani granular, bukan satu batch sekaligus gagal

---

## Alur logika end-to-end

### A. Dari scraper ke grup
`import` membaca hasil scrape final dan memindahkan yang masih baru ke tabel `grup`.

### B. Dari grup baru ke grup yang sah diproses
`permission` memberi basis izin otomatis sehingga grup tidak mandek di status `unknown`.

### C. Dari grup sah ke grup ber-owner
`assignment` memilih akun terbaik. Bila akun itu memang sudah ada di grup, status dinaikkan menjadi `managed`.

### D. Dari grup managed ke queue campaign
`campaign_prepare` mendorong grup managed ke `campaign_target` sambil mencegah duplikasi target aktif.

### E. Dari queue ke pengiriman nyata
`delivery` memakai draft aktif dan akun online untuk mengirim broadcast. Outcome dibedakan menjadi `sent`, `failed`, atau `blocked`.

### F. Dari macet ke pulih
`recovery_scan` mendeteksi item abnormal, lalu `recovery_execute` mencoba memulihkannya secara aman.

---

## Kenapa sekarang lebih tahan crash

1. **Rule parsing aman**
   - JSON rusak tidak langsung meledakkan engine
   - fallback ke object kosong/default

2. **Cooldown per rule**
   - rule tidak menembak terus setiap loop
   - mengurangi spam proses berulang

3. **Stage isolation**
   - satu stage gagal tidak membunuh semua stage lain
   - hasil error dikumpulkan di `stage_errors`

4. **Rule result tracking**
   - setiap rule punya `success_count`, `fail_count`, dan `last_triggered_at`
   - memudahkan audit apakah rule benar-benar bekerja

5. **Scope-aware processing**
   - stage hanya memproses entity yang memang layak
   - mengurangi noise dan side effect

6. **Recovery with attempt cap**
   - item yang gagal terus tidak di-loop tanpa henti

---

## Endpoint penting untuk debug rule

### `GET /api/v2/automation-rules`
Menampilkan rule lengkap, sudah termasuk:
- `canonical_stage`
- `condition`
- `action`
- `scope`
- `explanation`

### `GET /api/v2/automation-rules/overview`
Menampilkan:
- jumlah rule per stage
- konfigurasi efektif per stage
- rule mana yang benar-benar match pada kondisi saat ini

### `POST /api/v2/automation-rules/{id}/test`
Mengetes satu rule terhadap context stage saat ini:
- match atau tidak
- alasan tidak match
- action/scope efektif

### `GET /api/v2/orchestrator/status`
Menampilkan status worker, flow stage, settings, dan `rule_overview`.

### `POST /api/v2/orchestrator/run`
Menjalankan satu siklus penuh dan mengembalikan:
- hasil per stage
- stage error bila ada

---

## Catatan penting operasional

1. Rule yang dimatikan tidak ikut dihitung.
2. Rule yang sedang cooldown tidak akan match.
3. Bila semua rule untuk suatu stage nonaktif atau tidak match, stage akan `skipped` dengan alasan yang jelas.
4. Toggle global (`maintenance_mode` dan `pause_all_automation`) tetap menjadi pagar paling luar.
5. Rule default sistem akan di-seed otomatis jika belum ada.

---

## Rekomendasi tuning berikutnya

1. Tambahkan editor JSON rule di UI agar `condition_json`, `action_json`, dan `scope_json` bisa diubah tanpa API manual.
2. Tambahkan histori eksekusi rule per stage di tabel terpisah bila ingin audit yang lebih detail.
3. Pisahkan `campaign_prepare` dan `delivery` ke dashboard observability agar bottleneck campaign mudah dibaca.
4. Tambahkan health score sender untuk balancing pengirim, bukan hanya owner_phone/default sender.
