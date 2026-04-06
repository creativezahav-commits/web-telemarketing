# BACKEND V2 NOTES

Versi ini menambahkan baseline backend modular tanpa memutus route lama.

## Apa yang baru
- `backend/routes/` berisi blueprint API v2
- `backend/utils/api.py` untuk response standar
- `backend/services/overview_service.py` untuk ringkasan dashboard
- schema database diperluas dengan kolom baru agar siap menuju dashboard-driven control

## Kenapa pakai `/api/v2`
Agar frontend lama yang masih memakai route legacy tidak langsung rusak.

## Fokus baseline ini
- menjaga kompatibilitas
- menyiapkan migrasi bertahap ke sistem modular
- membuat fondasi yang lebih serius untuk uji operasional

## Hal yang masih butuh uji nyata
- login/session Telegram di lingkungan asli
- performa pada banyak akun
- perilaku recovery saat restart
- integrasi frontend v2 penuh
