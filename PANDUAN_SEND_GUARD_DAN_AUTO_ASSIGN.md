# Panduan Send Guard, Auto Assign, dan Hapus Akun

## 1) Guard hemat kuota untuk grup
Sistem sekarang menahan pengiriman ke grup pada dua kondisi utama:

1. **Grup tidak aktif**
   - Diukur dari `last_chat` grup.
   - Jika umur `last_chat` melebihi ambang hari yang ditentukan, grup diberi status guard `hold_inactive`.
   - Grup seperti ini **tidak akan dimasukkan ke queue campaign** dan **tidak akan dikirim** bila kebetulan sudah ada di queue.

2. **Chat terakhir masih pesan kita sendiri**
   - Diukur dari perbandingan `last_chat` dan `last_kirim`.
   - Jika `last_chat <= last_kirim`, sistem menganggap belum ada respons baru dari grup.
   - Grup diberi status guard `hold_waiting_response`.
   - Tujuannya menghindari spam dan menghemat kuota.

### Status guard yang dipakai
- `sendable` → aman untuk dikirim
- `hold_inactive` → grup terlalu sepi
- `hold_waiting_response` → chat terakhir masih pesan kita
- `unknown` → data aktivitas belum cukup, sistem belum bisa menilai penuh

### Di mana melihatnya
- **Tab Analisis Grup**
  - kolom aktivitas sekarang menampilkan:
    - `Last chat`
    - `Last kirim`
    - status guard
    - alasan guard

### Di mana mengaturnya
Masuk ke **Settings → Broadcast**:
- `campaign_skip_inactive_groups_enabled`
- `campaign_inactive_threshold_days`
- `campaign_skip_if_last_chat_is_ours`

### Pengaruh ke automation rules
Rule `campaign_prepare` dan `delivery` sekarang mengenal field baru:
- `skip_inactive_groups`
- `inactive_threshold_days`
- `skip_if_last_chat_is_ours`

Kalau field ini tidak diisi di rule, sistem akan memakai nilai dari Settings.

---

## 2) Kriteria auto assign
Mesin kandidat auto assign sekarang benar-benar memakai filter operasional, bukan hanya urutan kasar.

### Filter kandidat
Akun hanya dianggap kandidat bila:
- status akun `active` atau `online`
- `auto_assign_enabled = 1`
- `health_score >= assignment_min_health_score`
- `level_warming >= assignment_min_warming_level`
- `cooldown_until` sudah lewat / kosong
- `active_assignment_count < daily_new_group_cap`

### Ranking kandidat
Setelah lolos filter, skor ranking dihitung dengan formula:

`priority_weight + health_score + (warming_level * 10) - (active_assignment_count * 5)`

### Preferensi owner yang sudah join
Bila ada akun kandidat yang **sudah join grup**, sistem tetap memprioritaskannya.

### Di mana melihatnya
Masuk ke **Tab Assignments Grup**.
Di sana sekarang ada panel **Kriteria Auto Assign yang Dipakai** yang menampilkan:
- filter kandidat aktif
- formula ranking
- retry/reassign limit
- lokasi pengaturan di Settings

### Di mana mengaturnya
Masuk ke **Settings → Assignment Rules**:
- `assignment_min_health_score`
- `assignment_min_warming_level`
- `assignment_retry_count`
- `assignment_reassign_count`

---

## 3) Hapus akun permanen dari database
Pada **menu daftar akun**, sekarang ada tombol:
- `Logout`
- `Hapus`

### Perbedaan
- **Logout**
  - hanya memutus koneksi akun dari runtime
  - akun tetap ada di database

- **Hapus**
  - logout akun bila sedang online
  - hapus akun dari database
  - hapus file session lokal akun
  - relasi akun yang memakai foreign key ikut dibersihkan oleh database

### Endpoint yang ditambahkan
- Legacy UI: `POST /api/akun/hapus`
- API v2: `DELETE /api/v2/accounts/<phone>`

---

## 4) Ringkas alur setelah perubahan
1. Grup ditemukan / diimpor
2. Permission dinilai
3. Assignment memilih owner terbaik sesuai criteria aktif
4. Campaign prepare hanya memasukkan grup yang lolos send guard
5. Delivery mengecek send guard lagi sebelum benar-benar kirim
6. Jika grup sepi atau chat terakhir masih pesan kita, target ditahan / diskip, bukan dikirim

