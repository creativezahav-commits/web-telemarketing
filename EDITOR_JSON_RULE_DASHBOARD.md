# Editor JSON Rule di Dashboard

## Yang ditambahkan
- Tombol **Edit JSON** pada setiap rule di tab Automation.
- Editor visual untuk mengubah `condition_json`, `action_json`, dan `scope_json` langsung dari dashboard.
- Validasi backend agar JSON yang salah tipe tidak tersimpan dan tidak merusak orchestrator.
- Helper penjelasan per stage supaya operator tahu arti setiap key.

## Alur pakai
1. Buka tab **Automation Rules**.
2. Klik **Edit JSON** pada rule yang ingin diubah.
3. Ubah `condition_json`, `action_json`, dan `scope_json`.
4. Klik **Validasi JSON** untuk memastikan struktur aman.
5. Klik **Simpan Perubahan**.
6. Klik **Test Rule** bila ingin melihat apakah rule sedang match terhadap konteks runtime saat ini.

## Makna tiga JSON
- `condition_json`: syarat kapan rule boleh aktif.
- `action_json`: parameter aksi yang dijalankan saat rule match.
- `scope_json`: batas entity mana yang boleh diproses.

## Perlindungan crash
Route patch/create sekarang menolak JSON yang bukan objek, menolak tipe data yang salah pada key-key penting, dan menolak nilai angka negatif pada parameter yang semestinya non-negatif. Ini penting agar orchestrator tidak gagal akibat rule yang malformed.
