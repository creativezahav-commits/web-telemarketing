# Rule Wizard Operasional

Fitur ini menambahkan wizard berbasis bahasa operasional pada dashboard Automation Rules agar operator awam tidak perlu menulis `condition_json`, `action_json`, dan `scope_json` secara manual.

## Letak fitur

- Form **Buat Automation Rule**
- Form **Edit Rule**

Keduanya sekarang memiliki blok **Wizard Rule Operasional** di atas Visual Rule Builder.

## Cara kerja

1. Operator memilih **template wizard** atau menulis instruksi sendiri.
2. Wizard membaca kata kerja operasional seperti:
   - impor / scrape
   - permission / izin
   - assign / owner
   - queue / campaign
   - kirim / delivery / draft aktif
   - scan recovery / macet
   - pulihkan / requeue / retry
3. Wizard menentukan **stage** rule yang sesuai.
4. Wizard mengisi otomatis:
   - nama rule
   - stage rule
   - default condition / action / scope
   - batas proses per siklus
   - retry, cooldown, akun online minimum, threshold recovery, dan field umum lain jika terdeteksi dari kalimat
5. Hasil wizard langsung masuk ke **Visual Rule Builder** dan tetap tersimpan sebagai JSON internal yang valid.

## Contoh kalimat yang didukung

### Assignment
`Jika ada grup valid yang belum punya owner, assign otomatis maksimal 100 per siklus dan utamakan owner yang sudah join.`

Hasil utama:
- stage = `assignment`
- `action.limit = 100`
- `action.prefer_joined_owner = true`
- `scope.permission_status_in = ['valid', 'owned', 'admin', 'partner_approved', 'opt_in']`
- `scope.assignment_status_in = ['ready_assign', 'retry_wait', 'reassign_pending', 'failed']`

### Import
`Jika ada job scrape selesai yang hasilnya belum diimpor, impor otomatis maksimal 10 job per siklus.`

### Permission
`Jika ada grup aktif yang belum punya permission, beri permission valid berbasis opt_in maksimal 100 per siklus dan kecualikan channel.`

### Campaign Prepare
`Jika ada grup managed yang valid untuk broadcast, masukkan ke queue campaign maksimal 200 target per siklus dan hindari duplikasi.`

### Delivery
`Jika ada target queued dan minimal 2 akun online, kirim otomatis maksimal 20 target per siklus, wajib ada draft aktif, dan retry 15 menit.`

### Recovery Scan
`Pantau scraper, assignment, dan campaign; jika macet lebih dari 45 menit, buat recovery item otomatis maksimal 50 per siklus scan.`

### Recovery Execute
`Jika ada item recoverable, pulihkan otomatis maksimal 25 item per siklus, requeue maksimal 40 target failed, dan hentikan setelah 5 percobaan recovery.`

## Catatan desain

- Pada **create rule**, wizard boleh mengubah stage rule sesuai isi kalimat.
- Pada **edit rule**, wizard mengikuti stage rule yang sedang dibuka agar operator tidak tanpa sengaja memindahkan rule ke stage lain.
- Wizard ini **rule-based parser**, bukan NLP bebas penuh. Ia aman dan prediktif untuk operator, tetapi tidak dirancang untuk memahami seluruh variasi bahasa alami.
- Bila ada field lanjutan yang belum dicakup wizard, operator masih bisa memakai **Visual Rule Builder** atau panel **JSON Lanjutan**.

## File yang berubah

- `frontend/index.html`
- `frontend/automation.js`
- `frontend/style.css`
