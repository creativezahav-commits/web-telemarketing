# CLAUDE.md — TG Dashboard Project

> **Instruksi untuk Claude Code:** Baca file ini sebelum mengerjakan apapun di proyek ini.
> Setiap kali ada perubahan signifikan pada proyek, update file ini.

---

## Ringkasan Proyek

**TG Dashboard** adalah sistem broadcast automation untuk Telegram berbasis web.
Dibangun dengan Python Flask backend, Telethon (Telegram MTProto), dan frontend HTML/JS/CSS SPA.

**Lokasi proyek:**
```
C:\Users\user\Downloads\telegrammm\web-telemarketing\
```

**Cara menjalankan:**
```bash
cd backend
python app.py
```

Server berjalan di `http://127.0.0.1:5000`

---

## Tech Stack

| Komponen | Teknologi |
|---|---|
| Backend | Python Flask |
| Telegram API | Telethon 1.36.0 |
| Database | **PostgreSQL** (migrasi dari SQLite — April 2026) |
| DB Driver | psycopg2-binary |
| Frontend | Vanilla HTML/JS/CSS (SPA) |
| Deploy | Gunicorn via Procfile |
| Config | `.env` di folder `backend/` |

---

## Struktur Folder

```
web-telemarketing/
├── backend/
│   ├── app.py                  # Entry point Flask (besar, ~1700 baris)
│   ├── config.py               # Konfigurasi env, path, session
│   ├── .env                    # API_ID, API_HASH, PG_* (JANGAN commit)
│   ├── core/
│   │   ├── broadcast_session.py
│   │   ├── smart_sender.py
│   │   ├── message_queue.py
│   │   ├── account_status.py
│   │   ├── group_status.py
│   │   ├── grup_analisis.py    # DailyReset worker ada di sini
│   │   ├── scoring.py
│   │   ├── sync_manager.py
│   │   └── warming.py          # Kalkulasi level warming & kuota
│   ├── routes/                 # Blueprint API v2 modular
│   ├── services/
│   │   ├── account_manager.py
│   │   ├── orchestrator_service.py  # ENGINE UTAMA — throttle per akun
│   │   ├── automation_rule_engine.py
│   │   ├── group_send_guard.py
│   │   ├── message_service.py
│   │   ├── scraper_service.py
│   │   └── overview_service.py
│   └── utils/
│       ├── database.py         # Koneksi PostgreSQL + init_db()
│       ├── storage_db.py       # Semua CRUD fungsi
│       ├── settings_manager.py # Cache settings dari DB
│       ├── settings_defaults.py
│       └── api.py              # Response standar
├── frontend/                   # SPA HTML/JS/CSS
├── requirements.txt
└── Procfile
```

---

## Konfigurasi `.env`

File `.env` wajib ada di folder `backend/`:

```env
API_ID=isi_api_id_telegram
API_HASH=isi_api_hash_telegram

PG_HOST=localhost
PG_PORT=5432
PG_DB=tg_dashboard
PG_USER=postgres
PG_PASSWORD=isi_password_postgres
```

---

## Database — PostgreSQL

### Koneksi
Semua koneksi database melalui `utils/database.py` → fungsi `get_conn()`.
Wrapper `_ConnWrapper` membuat API kompatibel dengan kode lama (SQLite-style).

### Placeholder Query
PostgreSQL menggunakan `%s`, **bukan** `?`.

### Waktu Sekarang
PostgreSQL menggunakan `TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')`, **bukan** `datetime('now','localtime')`.

### Insert Unik
PostgreSQL menggunakan `ON CONFLICT DO NOTHING`, **bukan** `INSERT OR IGNORE`.

### Get Last ID
PostgreSQL menggunakan `RETURNING id` di akhir query INSERT, **bukan** `lastrowid`.

### Tabel Utama
| Tabel | Fungsi |
|---|---|
| `akun` | Data akun Telegram |
| `grup` | Data grup Telegram |
| `akun_grup` | Relasi akun ↔ grup |
| `campaign` | Sesi broadcast |
| `campaign_target` | Target per grup per campaign |
| `automation_rule` | Aturan otomasi per stage |
| `broadcast_throttle_akun` | Throttle per akun (independen) |
| `riwayat` | Log pengiriman |
| `settings` | Konfigurasi sistem |
| `recovery_item` | Item recovery orchestrator |
| `audit_log` | Log audit sistem |

---

## Pipeline Otomasi

```
Scraper → Import → Permission → Assignment →
Stabilization Wait → Broadcast Eligible →
Campaign Prepare → Campaign Queue →
Delivery → Cooldown → Recovery
```

### State Grup (`broadcast_status`)
`hold` → `ready_assign` → `assigned` → `managed` → `stabilization_wait` → `broadcast_eligible` → `queued` → `cooldown` / `blocked` / `failed`

---

## Throttle Per Akun

**Diimplementasikan April 2026** — setiap akun punya jeda kirim independen.

### Cara Kerja
```
Jeda = (24 jam × 60 menit) ÷ kuota_harian_akun
Variasi ±20% untuk menghindari pola
Clamp: min=jeda_min_menit, max=120 menit
```

### Setting Throttle
- `broadcast_jam_mulai = 0` dan `broadcast_jam_selesai = 0` → aktif 24 jam
- `w1_maks_kirim = 20` → kuota kirim level warming 1
- `broadcast_jeda_min_menit = 1` → jeda minimum
- `broadcast_jeda_max_menit = 10` → jeda maksimum

### Limit Per Siklus
Setiap siklus (~10 detik), sistem kirim sebanyak **akun yang jedanya sudah selesai** (bukan fixed 1).

### Tabel Throttle
```sql
broadcast_throttle_akun (phone, last_broadcast_at, next_allowed_at)
```

---

## Akun Aktif

| Phone | Status | Warming | Grup |
|---|---|---|---|
| +6283186603470 | active | 1 | 57 grup |
| +6285368414569 | active | 1 | 5 grup (baru) |
| +6283161394209 | banned | — | — |
| +6287788741275 | banned | — | — |
| +6287884147284 | banned | — | — |
| +6283871002868 | session expired | — | — |
| +6285735679328 | session expired | — | — |

---

## Konvensi Kode

- Variabel dan fungsi dalam **bahasa Indonesia** (`akun`, `grup`, `kirim`, `riwayat`, dll)
- Nama tabel: lowercase singkat (`akun`, `grup`, `riwayat`)
- Semua CRUD ada di `utils/storage_db.py`
- Semua query gunakan `%s` (PostgreSQL), bukan `?`
- Setiap fungsi buka dan tutup koneksi sendiri (`get_conn()` → `conn.close()`)
- Commit selalu sebelum `conn.close()`

---

## API Namespace

| Namespace | Keterangan |
|---|---|
| `/api/*` | Legacy API (masih aktif) |
| `/api/v2/overview/*` | Ringkasan dashboard |
| `/api/v2/accounts/*` | Manajemen akun |
| `/api/v2/campaigns/*` | Manajemen campaign |
| `/api/v2/assignments/*` | Manajemen assignment |
| `/api/v2/automation-rules/*` | Aturan otomasi |
| `/api/v2/broadcast-queue/*` | Antrian broadcast |
| `/api/v2/orchestrator/*` | Status orchestrator |
| `/api/v2/recovery/*` | Recovery item |
| `/api/v2/logs/*` | Audit log |

---

## Titik Kritis — Hati-hati Saat Edit

1. **`orchestrator_service.py`** — file terbesar dan paling kompleks (~2500 baris). Engine utama sistem. Perubahan di sini bisa break seluruh pipeline.

2. **`database.py`** — jangan ganti ke SQLite. Sistem sudah migrasi ke PostgreSQL.

3. **`storage_db.py`** — semua query pakai `%s`, `TO_CHAR(NOW(),...)`, dan `ON CONFLICT`. Jangan pakai SQLite syntax.

4. **`app.py`** — entry point Flask. Ada banyak legacy route. Hati-hati saat hapus route.

5. **`settings_manager.py`** — ada cache in-memory. Perubahan setting via dashboard langsung clear cache otomatis.

6. **Throttle** — jangan ganti throttle global ke per-akun atau sebaliknya tanpa update tabel `broadcast_throttle_akun`.

---

## File yang Bisa Diabaikan / Dihapus

File berikut adalah sisa debugging lama, tidak digunakan oleh sistem:
- `app_old.py`
- `cek.py`
- `debug_queue.py`
- `debug_queue2.py`
- `debug_riwayat.py`
- `fix_banned_akun.py`
- `fix_campaign.py`
- `frontend/patch_automation.ps1.txt`
- `utils/storage_db_pg.py` (hasil konversi sementara)
- `utils/database_pg.py` (hasil konversi sementara)
- `utils/database_full.py` (hasil konversi sementara)

---

## Riwayat Perubahan Penting

| Tanggal | Perubahan |
|---|---|
| April 2026 | Migrasi database SQLite → PostgreSQL |
| April 2026 | Implementasi throttle per akun (independen per akun) |
| April 2026 | Limit delivery dinamis (= jumlah akun siap per siklus) |
| April 2026 | Broadcast 24 jam (`jam_mulai=0`, `jam_selesai=0`) |
| April 2026 | Fix `fix_broadcast.py` — reset target stuck di `sending` |
| April 2026 | Fix 110 grup bottleneck (owner banned) di-skip permanen |

---

## TODO / Rencana

- [ ] Halaman **Riwayat** baru (yang lama dihapus)
- [ ] Halaman **Diagnosa Macet** baru (yang lama error `ct.assigned_account_id`)
- [ ] Fokus perbaikan halaman **Automation Rules**
- [ ] CLAUDE.md diupdate setiap ada perubahan signifikan
