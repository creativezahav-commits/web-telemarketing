# Konfigurasi Assign per Akun

Fitur ini dipakai ketika akun sebenarnya sehat, tetapi baru login ulang sehingga health/warming aktual belum mencerminkan kondisi lapangan.

## Yang bisa diatur per akun
- Auto assign aktif atau tidak
- Priority weight
- Daily new group cap
- Manual health override
- Manual warming override
- Fresh login grace
- Catatan assignment

## Fresh login grace
Fresh login grace membuat akun baru login dianggap memiliki nilai minimum tertentu selama beberapa menit setelah login.

Contoh aman:
- fresh_login_grace_enabled = aktif
- fresh_login_grace_minutes = 180
- fresh_login_health_floor = 80
- fresh_login_warming_floor = 2

## Manual override
Gunakan manual override bila operator sudah yakin akun sehat dan ingin memaksa nilai health/warming minimum tertentu untuk auto-assign.

## Dampak ke auto-assign
Engine assignment sekarang memakai nilai efektif:
- effective_health_score
- effective_warming_level

Nilai efektif ini bisa berasal dari:
1. nilai aktual akun
2. manual override
3. fresh login grace

Kapasitas harian dan cooldown tetap dihormati agar sistem tidak agresif.
