# Scraper random keyword hardening

Perubahan inti pada mesin scraper:

1. **Bug crash `_is_valid_entity` diperbaiki**
   - fungsi filter entity sekarang ada dan dipakai sebelum hasil disimpan.

2. **Keyword acak tetap dipertahankan**
   - prefix/suffix huruf, angka, tahun, turunan, custom list tetap aktif sesuai opsi.
   - sistem tidak memaksa keyword menjadi "rapi".

3. **Prioritas eksekusi keyword**
   - high: `base`, `custom`, `derived`
   - medium: `smart`, `years`, `numbers`
   - low: `prefix_letters`, `suffix_letters`
   - tujuan: discovery luas tetap ada, tetapi keyword bernilai lebih tinggi dieksekusi lebih dulu.

4. **Retry terbatas per keyword**
   - error transien seperti timeout/flood/server akan dicoba ulang secara terbatas.
   - keyword low-tier otomatis memakai retry lebih pendek/lebih hemat.

5. **Fail-soft per keyword**
   - satu keyword gagal tidak menjatuhkan seluruh job.
   - status keyword akan menjadi `retrying`, `failed`, `skipped`, atau `done`.

6. **Scoring kualitas per keyword**
   - tiap keyword run kini punya `quality_score`.
   - UI detail keyword menampilkan kualitas, attempt count, source, tier, priority, dan last error code.

7. **Retry failed manual dari dashboard**
   - tombol `Retry Failed` ditambahkan pada panel monitor job.

## Field baru pada `scrape_keyword_run`

- `source`
- `priority`
- `tier`
- `attempt_count`
- `max_attempts`
- `quality_score`
- `last_error_code`

## Makna rule operasional scraper yang sekarang

- **Random keyword tetap legal**: engine tidak menganggap variasi liar sebagai bug.
- **Yang dibatasi adalah biaya dan risiko runtime**: urutan eksekusi, retry, dan cooldown.
- **Kualitas discovery disaring setelah pencarian**: lewat filter entity, dedup, relevance score, dan recommended score.
