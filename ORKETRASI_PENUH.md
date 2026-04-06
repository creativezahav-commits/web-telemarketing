# Orkestrasi Penuh: Scraper → Recovery

## Mesin yang sekarang benar-benar tersambung

1. **Scraper Import**
   - job scrape yang sudah `done` dan masih punya hasil baru akan diimpor otomatis ke tabel `grup`
   - hanya hasil baru yang belum `imported`

2. **Permission Engine**
   - grup aktif dengan `permission_status=unknown` akan diberi permission `opt_in`
   - kolom `permission_status`, `permission_basis`, `approved_by`, dan `approved_at` ikut diperbarui

3. **Assignment Engine**
   - grup dengan permission valid akan dipilihkan owner akun terbaik
   - jika owner ternyata memang sudah join grup, status langsung naik menjadi `managed`
   - jika owner belum join, status tetap `assigned`

4. **Campaign Preparation Engine**
   - grup `managed` + permission valid + aktif dimasukkan ke `campaign_target`
   - queue sender mengikuti `owner_phone`
   - orchestrator memakai campaign aktif yang ada, atau membuat campaign otomatis baru

5. **Delivery Engine**
   - target queue dikirim memakai `draft` aktif
   - target yang berhasil menjadi `sent`
   - target yang gagal menjadi `failed` atau `blocked`
   - counter campaign diperbarui ulang setelah batch delivery

6. **Recovery Scan Engine**
   - mendeteksi `scrape_job`, `assignment`, dan `campaign` yang macet berdasarkan threshold menit di settings
   - item dibuat ke tabel `recovery_item`

7. **Recovery Execute Engine**
   - `scrape_job` yang recoverable akan di-resume
   - `assignment` yang recoverable akan dicari owner terbaik lagi lalu di-reassign
   - `campaign` yang recoverable akan me-requeue target gagal yang sudah due

## Endpoint baru

- `GET /api/v2/orchestrator/status`
- `POST /api/v2/orchestrator/run`
- `POST /api/v2/orchestrator/scan-recovery`
- `POST /api/v2/orchestrator/execute-recovery`

## Setting penting

- `orchestrator_interval_seconds`
- `orchestrator_import_batch`
- `orchestrator_permission_batch`
- `orchestrator_assign_batch`
- `orchestrator_campaign_batch`
- `orchestrator_delivery_batch`
- `orchestrator_recovery_batch`
- `campaign_retry_delay_minutes`

## Catatan desain

- worker lama tidak lagi dijalankan otomatis supaya tidak bentrok dengan orchestrator baru
- fungsi worker lama tetap disimpan sebagai fallback utilitas/debug
- delivery tetap aman: bila tidak ada `draft` aktif atau akun online, queue tidak dipaksa dikirim
