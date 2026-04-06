# Visual Rule Builder

Fitur ini menambahkan pembuat rule visual di dashboard Automation Rules.

## Tujuan
Operator awam dapat membuat atau mengubah `condition_json`, `action_json`, dan `scope_json` tanpa menulis JSON manual.

## Cara kerja
1. Stage rule dipilih pada form buat rule atau dibaca dari rule yang sedang diedit.
2. Dashboard memuat meta stage dari endpoint `/api/v2/automation-rules/meta`.
3. Berdasarkan `field_types`, dashboard merender komponen visual berupa:
   - input angka
   - checkbox boolean
   - dropdown string yang punya opsi tetap
   - checklist multi-select untuk daftar status/entitas
   - textarea satu nilai per baris untuk daftar string bebas
4. Nilai visual builder otomatis diserialisasi ulang ke JSON internal.
5. Saat simpan atau validasi, backend tetap memvalidasi payload agar aman untuk orchestrator.

## Mode lanjutan
Panel JSON tidak dihapus. Ia dipindahkan ke bagian lanjutan untuk:
- field custom yang belum punya komponen visual
- debug cepat
- copy/paste rule antar lingkungan

## Catatan penting
- Field yang belum punya editor visual tetap dipertahankan saat rule diubah dari visual builder.
- Dashboard memberi notifikasi bila rule mengandung field lanjutan yang hanya bisa diedit dari panel JSON.
