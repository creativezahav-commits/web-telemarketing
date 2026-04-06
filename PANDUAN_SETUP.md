# Panduan Setup Lengkap — Telegram HR Dashboard

Ikuti langkah-langkah ini secara berurutan. Jangan lewati satupun.

---

## LANGKAH 0 — Dapatkan API Telegram

Ini wajib dilakukan sekali saja sebelum segalanya.

1. Buka browser → pergi ke https://my.telegram.org
2. Login dengan nomor HP Telegram kamu
3. Klik **"API development tools"**
4. Isi form (nama dan deskripsi bebas, contoh: "Dashboard HR")
5. Klik **Create application**
6. Catat dua nilai ini — akan dipakai di langkah berikutnya:
   - **App api_id** → angka, contoh: `12345678`
   - **App api_hash** → huruf dan angka panjang, contoh: `abcdef1234567890abcdef1234567890`

---

## LANGKAH 1 — Siapkan File .env

Di dalam folder project (sejajar dengan folder `backend/`), buat file bernama `.env`:

```
API_ID=12345678
API_HASH=abcdef1234567890abcdef1234567890
```

Ganti `12345678` dan `abcdef1234...` dengan nilai asli dari langkah 0.

> Jika deploy ke **Railway**: jangan buat file .env. Masukkan `API_ID` dan `API_HASH` langsung
> di Railway → project → Variables. Railway otomatis mengisi `PORT`.

---

## LANGKAH 2 — Install Python dan Dependensi

Pastikan Python 3.10 atau lebih baru sudah terinstall.

```bash
# Cek versi Python
python --version

# Masuk ke folder project
cd telegram-dashboard-v3-fixed

# Install semua library yang dibutuhkan
pip install -r requirements.txt
```

Jika perintah `pip` tidak dikenal, coba:
```bash
pip3 install -r requirements.txt
```

---

## LANGKAH 3 — Inisialisasi Database

Database akan dibuat otomatis saat pertama kali server dijalankan.
Tapi jalankan migrate dulu untuk memastikan semua tabel dan settings default sudah ada:

```bash
# Masuk ke folder backend
cd backend

# Jalankan migrasi
python migrate.py
```

Output yang benar:
```
Migrasi database...
  ✅ Tabel akun_grup siap
  ⏭️  grup.aktif_indikator sudah ada   ← normal jika database sudah pernah dibuat
  ✅ Settings baru ditambahkan
✅ Migrasi selesai!
```

---

## LANGKAH 4 — Jalankan Server

### Di komputer lokal (Windows/Mac/Linux):

```bash
# Pastikan masih di folder backend/
cd backend

# Jalankan server
python app.py
```

Server berjalan di: **http://127.0.0.1:5000**

Buka browser → ketik `http://127.0.0.1:5000` → dashboard akan muncul.

### Di Railway (cloud):

Tidak perlu langkah manual — Railway otomatis membaca `Procfile` dan menjalankan:
```
gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120
```

Pastikan sudah push ke GitHub dan terhubung ke Railway.

---

## LANGKAH 5 — Login Akun Telegram

Ini dilakukan dari dalam dashboard, bukan dari terminal.

1. Buka dashboard di browser
2. Klik menu **👤 Akun** di sidebar kiri
3. Di bagian bawah, isi **Nomor HP** dengan format internasional:
   ```
   +628123456789
   ```
4. Klik **🔗 Login**
5. Buka aplikasi Telegram di HP → cari pesan dari **"Telegram"** berisi kode 5 digit
6. Masukkan kode tersebut di kolom **Kode OTP**
7. Klik **✅ Verifikasi OTP**

Kalau akun punya **2FA (password)**, kolom password akan muncul otomatis — isi dan verifikasi.

Ulangi untuk setiap akun yang ingin ditambahkan.

---

## LANGKAH 6 — Ambil Daftar Grup

Setelah akun login:

1. Klik menu **🔍 Discovery** di sidebar
2. Pilih akun dari dropdown
3. Klik **🔍 Cari Grup Baru**
4. Sistem akan menampilkan semua grup yang diikuti akun tersebut
5. Grup akan otomatis masuk ke database

---

## LANGKAH 7 — Cara Kerja Fitur Utama

### Kirim Pesan Manual
1. Buat draft dulu di tab **📝 Draft** → klik "Jadikan Aktif"
2. Ke tab **✉️ Kirim** → pilih akun, pilih grup, pesan otomatis terisi dari draft aktif
3. Klik **📤 Kirim Sekarang**

### Broadcast ke Banyak Grup
1. Pastikan draft aktif sudah ada
2. Ke tab **📡 Broadcast**
3. Atur jeda pengiriman (default: **30 detik** — jangan terlalu cepat)
4. Centang grup yang ingin dikirim
5. Klik **📡 Mulai Broadcast**

### Scraper Grup Baru
1. Ke tab **🧲 Scraper**
2. Pilih akun, isi keyword (contoh: `loker medan`, `lowongan kerja`)
3. Setting default sudah aman, tidak perlu diubah
4. Klik **🚀 Mulai Job**
5. Tunggu job selesai (lihat Monitor Job di kanan)
6. Klik **⭐ Impor Rekomendasi** atau **📥 Impor Semua**

---

## PENGATURAN DEFAULT (Sudah Diisi Otomatis)

Semua nilai ini sudah terisi otomatis saat database pertama kali dibuat.
Kamu bisa lihat dan ubah di tab **⚙️ Settings**.

### Warming — Batas Pengiriman per Akun

| Level | Umur Akun | Maks Kirim/Hari | Maks Join/Hari | Jeda Kirim | Jeda Join |
|-------|-----------|-----------------|----------------|------------|-----------|
| 1 (Baru) | 0–7 hari | **5** pesan | **3** grup | 90 detik | 120 detik |
| 2 (Berkembang) | 8–30 hari | **15** pesan | **10** grup | 45 detik | 60 detik |
| 3 (Dewasa) | 31–90 hari | **25** pesan | **20** grup | 30 detik | 30 detik |
| 4 (Terpercaya) | 90+ hari | **30** pesan | **30** grup | 20 detik | 15 detik |

### Score Akun

| Kondisi | Nilai |
|---------|-------|
| Bobot umur akun | 40% |
| Bobot kesehatan | 30% |
| Bobot performa | 30% |
| Terpercaya (hijau) | score ≥ 80 |
| Baik (kuning) | score ≥ 60 |
| Perhatian (merah) | score ≥ 40 |

### Score Grup

| Kondisi | Nilai |
|---------|-------|
| Bobot jumlah member | 50% |
| Bobot riwayat kirim | 50% |
| Status Hot (diprioritaskan) | score ≥ 70 |
| Status Normal | score ≥ 30 |

### Broadcast

| Setting | Default |
|---------|---------|
| Jeda minimum antar kirim | 20 detik |
| Jeda maksimum antar kirim | 60 detik |
| Jadwal ulang setelah | 3 hari |

### Scraper

| Setting | Default |
|---------|---------|
| Hasil per keyword | 30 grup |
| Minimum member | 0 (semua) |
| Score rekomendasi | 40 |
| Maksimal keyword | 80 |
| Jeda antar keyword | 1–2 detik |

---

## TROUBLESHOOTING — Masalah Umum

### Dashboard tidak bisa dibuka
```
Pastikan server sudah berjalan (lihat Langkah 4).
Coba buka: http://127.0.0.1:5000
Kalau Railway: cek apakah deploy berhasil di tab Deployments.
```

### Login akun gagal / OTP tidak datang
```
Pastikan API_ID dan API_HASH sudah benar di file .env
Coba lagi setelah 1 menit — Telegram kadang delay kirim OTP
Pastikan tidak ada sesi lama yang masih aktif di folder backend/session/
```

### Scraper jalan tapi tidak ada hasil
```
Cek apakah akun scraper sudah login dan online (badge hijau di tab Akun)
Coba keyword yang lebih umum, contoh: "loker" bukan "loker pt abc"
Matikan filter "Hanya yang punya username" jika ingin hasil lebih banyak
```

### Import scraper tidak terjadi apa-apa
```
Pastikan job scraper sudah berstatus "done" (bukan masih "running")
Klik tab Hasil Scrape → pilih job dari dropdown
Kalau "Impor Rekomendasi" hasilnya 0, coba klik "Impor Semua"
Cek pesan yang muncul di bawah tombol — sekarang ada keterangan kenapa kosong
```

### Broadcast gagal semua
```
Pastikan minimal 1 akun online (badge hijau di tab Akun)
Pastikan draft aktif sudah dipilih di tab Draft
Jangan kirim ke grup yang sudah dikirim hari ini — cek tanda di tab Riwayat
Naikkan jeda broadcast ke 45–60 detik agar lebih aman
```

### Error "Akun tidak aktif" saat kirim
```
Akun terputus dari Telegram — perlu reconnect
Klik tab Akun → klik tombol Pulihkan di samping akun yang bermasalah
Atau restart server — server akan otomatis reconnect semua akun saat startup
```

---

## STRUKTUR FOLDER

```
telegram-dashboard-v3-fixed/
├── .env                    ← buat sendiri, isi API_ID dan API_HASH
├── .env.example            ← contoh format .env
├── requirements.txt        ← daftar library Python
├── Procfile                ← untuk Railway
├── PANDUAN_SETUP.md        ← file ini
├── backend/
│   ├── app.py              ← server utama, jalankan ini
│   ├── migrate.py          ← jalankan sekali setelah install
│   ├── config.py           ← konfigurasi path dan env
│   ├── data/
│   │   └── dashboard.db    ← database SQLite (otomatis dibuat)
│   ├── session/            ← file sesi Telegram (otomatis diisi saat login)
│   ├── core/               ← logika warming, scoring, broadcast
│   ├── services/           ← account manager, scraper, message
│   ├── routes/             ← endpoint API v2
│   └── utils/              ← database, storage, settings
└── frontend/
    ├── index.html          ← halaman utama dashboard
    ├── app.js              ← logika utama frontend
    ├── scraper.js          ← tab scraper
    ├── broadcast.js        ← tab broadcast
    ├── permissions.js      ← tab permissions (engine baru)
    ├── assignments.js      ← tab assignments (engine baru)
    ├── campaigns.js        ← tab campaigns (engine baru)
    ├── automation.js       ← tab automation rules (engine baru)
    ├── recovery.js         ← tab recovery (engine baru)
    └── style.css           ← tampilan dashboard
```
